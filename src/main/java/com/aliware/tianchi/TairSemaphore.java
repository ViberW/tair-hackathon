package com.aliware.tianchi;

import com.aliyun.tair.tairstring.TairString;
import com.aliyun.tair.tairstring.params.ExincrbyParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/2/22 15:28
 */
public class TairSemaphore {

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
    //线程池的检测时长
    private long schedule_interval = 3;
    //使用一个本地的queue
    private LinkedBlockingQueue<WaitNode> waitQueue = new LinkedBlockingQueue<>();
    private AtomicBoolean running = new AtomicBoolean(false);
    //尝试通过反射获取到对应的Queue的Lock
    private final ReentrantLock putLock;

    public TairSemaphore(Jedis jedis, String key, int permits) {
        this.jedis = jedis;
        this.tairString = new TairString(jedis);
        this.key = KEY_PREFIX + key;
        this.pubsubChannel = PUBSUB_PREFIX + key;
        this.maxPermits = permits;
        //监听pub/sub消息, 获取到消息则将头部的消息取出,并进行处理, 变更状态有变化, 通过cas进行变更通知, 唤醒第一个线程
        jedis.subscribe(new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                notifyNode();
            }
        });
        //获取到queue内部的lock
        this.putLock = QueueUtil.lockByQueue(waitQueue);
        //todo 唯一有个难处理的点就是 若是系统非正常关闭, 导致没有释放掉, 那么permit该如何处理? 感觉这种就像是一种策略吧
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

    private void notifyOrAdd() {
        this.notifyNode();
        if (!waitQueue.isEmpty()) {
            GlobalExecutor.schedule().schedule(this::notifyOrAdd, schedule_interval, TimeUnit.MILLISECONDS);
        } else {
            putLock.lock();//先一步阻止线程进入queue
            try {
                if (waitQueue.isEmpty()) {//若是节点为空, 则尝试将running设置为false
                    running.compareAndSet(true, false);
                }
            } finally {
                putLock.unlock();
            }
        }
    }

    /**
     * 判断是否需要添加检测任务
     */
    private void runningStamp() {
        if (!running.get()) {
            if (running.compareAndSet(false, true)) {
                GlobalExecutor.schedule().schedule(this::notifyOrAdd, schedule_interval, TimeUnit.MILLISECONDS);
            }
        }
    }

    public void acquire(int permits) {
        if (!tryAcquire(permits)) {
            waitQueue.offer(new WaitNode(Thread.currentThread()));
            runningStamp();
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
