package com.aliware.tianchi.semaphore;

import com.aliware.tianchi.common.AbstractLifeCycle;
import com.aliware.tianchi.common.CommonUtil;
import com.aliware.tianchi.common.GlobalExecutor;
import com.aliware.tianchi.common.TairUtil;
import com.aliware.tianchi.leader.LeaderListener;
import com.aliware.tianchi.leader.LeaderSelector;
import com.aliyun.tair.tairstring.TairString;
import com.aliyun.tair.tairstring.TairStringPipeline;
import com.aliyun.tair.tairstring.params.ExincrbyParams;
import com.aliyun.tair.tairstring.params.ExsetParams;
import com.aliyun.tair.tairstring.results.ExgetResult;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 这里都是非公平的, 若是想要公平, 则需要使用redis-list保存,就不去实现了
 * -----
 * 有区别于单系统, 这里分布式系统中若是节点的下线 会造成permits不可用, 需要定时重新构建permits
 * @since 2022/2/22 15:28
 */
public class TairSemaphore extends AbstractLifeCycle {

    private static final String KEY_PREFIX = "_hackathon:semaphore:";
    private static final String BLOCK_KEY_PREFIX = "_hackathon:_block:semaphore:";
    private static final String PUBSUB_PREFIX = "_pubsub:semaphore:";

    private static final String TS_PKEY = "_ts:semaphore:pkey";
    private static final String TS_S_TIME_KEY_PREFIX = "s_time:";
    private static final String TS_S_COUNT_KEY_PREFIX = "s_count:";

    //通知每个节点 尝试去争抢
    private final JedisPool jedisPool;
    private final String key;
    //pub/sub的通道
    private final String pubsubChannel;
    //最大申请量
    private final int maxPermits;
    //使用一个本地的queue
    private LinkedBlockingQueue<WaitNode> waitQueue = new LinkedBlockingQueue<>();
    private JedisPubSub pubSub;
    /**
     * 时间处理监听器
     */
    private SemaphoreNodeListener listener;

    private LeaderSelector selector;

    /**
     * 重新构建的超时时间
     */
    private volatile long rebuildTimeout;

    /**
     * ts时间计算相关的
     */
    private boolean tsCalculate;
    private TairTsRecord tairTsRecord;

    public String getKey() {
        return key;
    }

    public int getMaxPermits() {
        return maxPermits;
    }

    public void pushRelease() {
        TairUtil.poolExecute(jedisPool, jedis -> jedis.publish(pubsubChannel, "1"));
    }

    /**
     * @param selector    分布式下的leader选择器
     * @param jedisPool   jedis
     * @param key         保存的key后缀: KEY_PREFIX+key
     * @param permits     信号量
     * @param timeOut     节点下线时默认等待的时间节点,重新构建新的semaphore的permits,毫秒
     * @param tsCalculate 是否开启计算合适的执行时间
     */
    public TairSemaphore(LeaderSelector selector, JedisPool jedisPool,
                         String key, int permits, long timeOut, boolean tsCalculate) {
        this.selector = selector;
        this.jedisPool = jedisPool;
        this.key = KEY_PREFIX + key;
        this.pubsubChannel = PUBSUB_PREFIX + key;
        this.maxPermits = permits;
        this.rebuildTimeout = timeOut;
        this.tsCalculate = tsCalculate;
        if (tsCalculate) {
            tairTsRecord = new TairTsRecord(jedisPool, key);
        }
        //监听pub/sub消息, 获取到消息则将头部的消息取出,并进行处理, 变更状态有变化, 通过cas进行变更通知, 唤醒第一个线程
        this.pubSub = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                notifyNode();
            }
        };
        this.listener = new SemaphoreNodeListener(jedisPool, this, key, tsCalculate);
    }

    @Override
    protected void doStart() {
        CommonUtil.pubsubThread(() -> {
            TairUtil.poolExecute(jedisPool, jedis -> {
                jedis.subscribe(pubSub, pubsubChannel);
                return null;
            });
        }).start();
        while (!pubSub.isSubscribed()) {
            CommonUtil.sleep(0);
        }
        listener.start();
        selector.registerListener(listener);
    }

    @Override
    protected void doStop() {
        pubSub.unsubscribe(pubsubChannel);
        listener.stop();
        selector.unRegisterListener(listener);
    }

    private void notifyNode() {
        WaitNode node;
        while ((node = waitQueue.peek()) != null) {
            if (node.release.get()) {
                return;
            }
            //这里是不是需要等待呢?
            if (node.release.compareAndSet(false, true)) {
                //防止并发情况下, 直接将当前给释放掉了
                LockSupport.unpark(node.thread);
                if (node.thread.isInterrupted()) {
                    continue;
                }
                return;
            }
        }
    }

    /**
     * ******************************************
     * 下面为调用方法
     * ******************************************
     */
    public void acquire() {
        acquire(1);
    }

    public void acquire(int permits) {
        if (!tryAcquire(permits)) {
            WaitNode waitNode = new WaitNode(Thread.currentThread());
            waitQueue.offer(waitNode);
            for (; ; ) {
                WaitNode head = waitQueue.peek();
                if (waitNode == head && tryAcquire(permits)) {
                    waitQueue.poll();
                    break;
                }
                waitNode.release.set(false);
                LockSupport.park();
            }
        }
        if (tsCalculate) {
            tairTsRecord.beginTs();
        }
    }

    //todo 添加超时的检测的任务
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) {
        long nanosTimeout = unit.toNanos(timeout);
        long deadline = System.nanoTime() + nanosTimeout;
        while (!isStart()) {
            nanosTimeout = deadline - System.nanoTime();
            if (nanosTimeout <= 0) {
                return false;
            } else if (nanosTimeout > 1000L) {//spinForTimeoutThreshold
                LockSupport.park(nanosTimeout);
            }
        }
        return true;
    }

    public void release() {
        release(1);
    }

    public void release(int permits) {
        if (tryRelease(permits)) {
            TairUtil.poolExecute(jedisPool, jedis -> jedis.publish(pubsubChannel, "1"));
            if (tsCalculate) {//记录执行耗时
                tairTsRecord.endTs();
            }
        }
    }

    public boolean tryAcquire(int permits) {
        try {
            ExincrbyParams params = new ExincrbyParams();
            params.max(maxPermits);
            TairUtil.poolExecute(jedisPool, jedis -> {
                TairString tairString = new TairString(jedis);
                return tairString.exincrBy(key, permits, params);
            });
            return true;
        } catch (Exception e) {
            if (e.getMessage().contains("increment or decrement would overflow")) {
                return false;
            }
            throw e;
        }
    }

    private boolean tryRelease(int permits) {
        try {
            ExincrbyParams params = new ExincrbyParams();
            params.min(0);
            TairUtil.poolExecute(jedisPool, jedis -> {
                TairString tairString = new TairString(jedis);
                return tairString.exincrBy(key, -permits, params);
            });
            return true;
        } catch (Exception e) {
            System.out.println("error!!!!!!!" + e.getMessage());
            if (e.getMessage().contains("increment or decrement would overflow")) {
                return false;
            }
            throw e;
        }
    }

    static class WaitNode {
        private volatile Thread thread;
        private AtomicBoolean release = new AtomicBoolean(false);

        public WaitNode(Thread thread) {
            this.thread = thread;
        }
    }

}
