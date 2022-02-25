package com.aliware.tianchi.semaphore;

import com.aliyun.tair.tairstring.TairString;
import com.aliyun.tair.tairstring.params.ExincrbyParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 这里都是非公平的, 若是想要公平, 则需要使用redis-list保存
 * @since 2022/2/22 15:28
 */
public class TairSemaphore  {

    private static final String KEY_PREFIX = "_hackathon:semaphore:";
    private static final String PUBSUB_PREFIX = "_pubsub:semaphore:";
    //通知每个节点 尝试去争抢
    private final Jedis jedis;
    private final TairString tairString;
    private final String key;
    //pub/sub的通道
    private final String pubsubChannel;
    //最大申请量
    private final int maxPermits;
    //使用一个本地的queue
    private LinkedBlockingQueue<WaitNode> waitQueue = new LinkedBlockingQueue<>();
    private JedisPubSub pubSub;
    //缓存存在时间, 0代表永久
    private int timeSecond;

    public TairSemaphore(Jedis jedis, String key, int permits) {
        this(jedis, key, permits, 0);
    }

    /**
     * @param jedis      jedis
     * @param key        保存的key后缀: KEY_PREFIX+key
     * @param permits    信号量
     * @param timeSecond 默认的存在时间 若为0时则永久存在
     */
    public TairSemaphore(Jedis jedis, String key, int permits, int timeSecond) {
        this.jedis = jedis;
        this.tairString = new TairString(jedis);
        this.key = KEY_PREFIX + key;
        this.pubsubChannel = PUBSUB_PREFIX + key;
        this.maxPermits = permits;
        this.timeSecond = timeSecond;
        //监听pub/sub消息, 获取到消息则将头部的消息取出,并进行处理, 变更状态有变化, 通过cas进行变更通知, 唤醒第一个线程
        this.pubSub = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                notifyNode();
            }
        };
        jedis.subscribe(pubSub, pubsubChannel);
        //todo 唯一的难点就是, 如何限制等待的时间. 重新构建一个索引段,用于代替刚才的限制
        // 那么permit该如何处理? 感觉这种就像是一种策略吧
        //todo 注册节点的信息, 并把它放入到列表中.
        //todo 注册看门狗,持续保持节点数据存在
        //pubSub.unsubscribe(pubsubChannel);
    }

    private void notifyNode() {
        WaitNode node;
        while ((node = waitQueue.peek()) != null) {
            Thread th = node.thread;
            if (th.isInterrupted()) {
                continue;
            }
            if (node.release.get()) {
                return;
            }
            if (node.release.compareAndSet(false, true)) {
                //防止并发情况下, 直接将当前给释放掉了
                LockSupport.unpark(th);
            }
        }
    }

    public void acquire(int permits) {
        if (!tryAcquire(permits)) {
            waitQueue.offer(new WaitNode(Thread.currentThread()));
            for (; ; ) {
                WaitNode head = waitQueue.peek();
                if (head != null && head.thread == Thread.currentThread()) {
                    if (!tryAcquire(permits)) {
                        LockSupport.park();
                    } else {
                        waitQueue.poll();
                        return;
                    }
                } else {
                    LockSupport.park();
                }
            }
        }
    }

    public boolean release(int permits) {
        if (tryRelease(permits)) {
            try {
                jedis.publish(pubsubChannel, "1");
            } catch (Exception e) {
                if (e.getMessage().contains("increment or decrement would overflow")) {
                    return true;
                }
                e.printStackTrace();
            }
            return true;
        }
        return false;
    }

    private boolean tryRelease(int permits) {
        try {
            ExincrbyParams params = new ExincrbyParams();
            params.min(0);
            if (timeSecond > 0) {
                params.ex(timeSecond);
            }
            tairString.exincrBy(key, -permits, params);
            return true;
        } catch (Exception e) {
            if (e.getMessage().contains("increment or decrement would overflow")) {
                return false;
            }
            throw e;
        }
    }

    public boolean tryAcquire(int permits) {
        try {
            ExincrbyParams params = new ExincrbyParams();
            params.max(maxPermits);
            if (timeSecond > 0) {
                params.ex(timeSecond);
            }
            tairString.exincrBy(key, permits, params);
            return true;
        } catch (Exception e) {
            if (e.getMessage().contains("increment or decrement would overflow")) {
                return false;
            }
            throw e;
        }
    }

    static class WaitNode {
        private Thread thread;
        private AtomicBoolean release = new AtomicBoolean(false);

        public WaitNode(Thread thread) {
            this.thread = thread;
        }
    }

}
