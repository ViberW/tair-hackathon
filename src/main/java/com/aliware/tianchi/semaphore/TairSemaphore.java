package com.aliware.tianchi.semaphore;

import com.aliware.tianchi.common.AbstractLifeCycle;
import com.aliware.tianchi.common.GlobalExecutor;
import com.aliware.tianchi.leader.LeaderListener;
import com.aliware.tianchi.leader.LeaderSelector;
import com.aliyun.tair.tairstring.TairString;
import com.aliyun.tair.tairstring.TairStringPipeline;
import com.aliyun.tair.tairstring.params.ExincrbyParams;
import com.aliyun.tair.tairstring.params.ExsetParams;
import com.aliyun.tair.tairstring.results.ExgetResult;
import redis.clients.jedis.Jedis;
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
    //通知每个节点 尝试去争抢
    private final Jedis jedis;
    private final TairString tairString;
    private final TairStringPipeline tairStringPipeline;
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
    private LeaderListener listener;

    private LeaderSelector selector;

    /**
     * @param selector 分布式下的leader选择器
     * @param jedis    jedis
     * @param key      保存的key后缀: KEY_PREFIX+key
     * @param permits  信号量
     * @param timeOut  节点下线时默认等待的时间节点,重新构建新的semaphore的permits,毫秒
     */
    public TairSemaphore(LeaderSelector selector, Jedis jedis,
                         String key, int permits, long timeOut) {
        this.selector = selector;
        this.jedis = jedis;
        this.tairString = new TairString(jedis);
        this.tairStringPipeline = new TairStringPipeline();
        this.tairStringPipeline.setClient(jedis.getClient());
        this.key = KEY_PREFIX + key;
        this.pubsubChannel = PUBSUB_PREFIX + key;
        this.maxPermits = permits;
        //监听pub/sub消息, 获取到消息则将头部的消息取出,并进行处理, 变更状态有变化, 通过cas进行变更通知, 唤醒第一个线程
        this.pubSub = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                notifyNode();
            }
        };
        //这里需要监听节点是否下线,
        this.listener = new LeaderListener() {
            @Override
            public void onRemoveNode(boolean leader) {
                markRebuild(timeOut);
                if (leader) {
                    startRebuild();
                    clearRebuild(timeOut);
                }
            }

            @Override
            public void onAddNode(boolean leader) {
                //....不做处理
            }

            @Override
            public void onBecomeLeader() {
                //这里变为Leader
                Long ttl = jedis.ttl(BLOCK_KEY_PREFIX);
                if (null == ttl || ttl == -1) {
                    markRebuild(timeOut);
                    startRebuild();
                    ttl = timeOut;
                } else {
                    ttl = TimeUnit.SECONDS.toMillis(ttl == 0 ? 1 : ttl);
                }
                clearRebuild(ttl);
            }
        };

    }

    @Override
    protected void doStart() {
        jedis.subscribe(pubSub, pubsubChannel);
        selector.registerListener(listener);
    }

    @Override
    protected void doStop() {
        pubSub.unsubscribe(pubsubChannel);
        selector.unRegisterListener(listener);
    }

    /**
     * 开启重建任务
     *
     * @param timeout 检测时间
     */
    private void markRebuild(long timeout) {
        ExsetParams params = new ExsetParams();
        params.nx().px(timeout + TimeUnit.SECONDS.toMillis(3));
        tairString.exset(BLOCK_KEY_PREFIX, "1", params);
    }

    private void startRebuild() {
        ExsetParams params = new ExsetParams();
        params.xx();
        tairStringPipeline.exset(key, String.valueOf(2 * maxPermits + 1), params);
        tairStringPipeline.sync();
    }

    /**
     * 重新启动任务处理
     *
     * @param timeout 检测时间
     */
    private void clearRebuild(long timeout) {
        GlobalExecutor.schedule().schedule(() -> {
            ExgetResult<String> exget = tairString.exget(key);
            if (exget == null || Integer.parseInt(exget.getValue()) > maxPermits) {
                //说明已经有重置过了
                return;
            }
            ExsetParams params = new ExsetParams();
            params.xx();
            tairStringPipeline.exset(key, "0", params);
            tairStringPipeline.del(BLOCK_KEY_PREFIX);
            tairStringPipeline.sync();
        }, timeout, TimeUnit.MILLISECONDS);
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

    /**
     * ******************************************
     * 下面为调用方法
     * ******************************************
     */

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

    static class WaitNode {
        private Thread thread;
        private AtomicBoolean release = new AtomicBoolean(false);

        public WaitNode(Thread thread) {
            this.thread = thread;
        }
    }

}
