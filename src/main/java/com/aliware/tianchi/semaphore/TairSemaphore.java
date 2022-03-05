package com.aliware.tianchi.semaphore;

import com.aliware.tianchi.common.AbstractLifeCycle;
import com.aliware.tianchi.common.CommonUtil;
import com.aliware.tianchi.common.TairUtil;
import com.aliware.tianchi.common.recorder.ConstTimeRecorder;
import com.aliware.tianchi.common.recorder.TimeRecorder;
import com.aliware.tianchi.leader.LeaderSelector;
import com.aliyun.tair.tairstring.TairString;
import com.aliyun.tair.tairstring.TairStringPipeline;
import com.aliyun.tair.tairstring.params.ExincrbyParams;
import com.aliyun.tair.tairstring.params.ExsetParams;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 这里都是非公平的, 若是想要公平, 则需要使用redis-list保存,就不去实现了
 * -----
 * 有区别于单系统, 这里分布式系统中若是节点的下线 会造成permits不可用, 需要定时重新构建permits
 * <p>
 * >>> {@link com.aliware.tianchi.semaphore.bak.TairSemaphoreBak} 这个是最初版本的简单实现AQS, 目前尽量换成AQS
 * @since 2022/2/22 15:28
 */
public class TairSemaphore extends AbstractLifeCycle {


    private static final String KEY_PREFIX = "_hackathon:semaphore:";
    private static final String PUBSUB_PREFIX = "_pubsub:semaphore:";

    //通知每个节点 尝试去争抢
    private final JedisPool jedisPool;
    private final String key;
    //pub/sub的通道
    private final String pubsubChannel;
    //使用一个本地的queue
    private JedisPubSub pubSub;
    /**
     * 时间处理监听器
     */
    private SemaphoreNodeListener listener;

    private LeaderSelector selector;

    private TimeRecorder timeRecorder;
    private Sync sync;

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
        this.timeRecorder = tsCalculate ? new TairTsRecorder(jedisPool, key, timeOut) : new ConstTimeRecorder(timeOut);
        this.sync = new NonfairSync(jedisPool, timeRecorder, this.key, permits);

        //监听pub/sub消息, 获取到消息则将头部的消息取出,并进行处理, 变更状态有变化, 通过cas进行变更通知, 唤醒第一个线程
        this.pubSub = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                try {
                    shardMethod.invoke(sync);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        };
        this.listener = new SemaphoreNodeListener(jedisPool, this, selector, timeRecorder, key);
    }

    @Override
    protected void doStart() {
        CommonUtil.pubsubThread(() -> {
            TairUtil.poolExecute(jedisPool, jedis -> {
                jedis.subscribe(pubSub, pubsubChannel);
                return null;
            });
        }).start();
        timeRecorder.start();
        selector.registerListener(listener);
    }

    @Override
    protected void doStop() {
        selector.unRegisterListener(listener);
        timeRecorder.stop();
        pubSub.unsubscribe(pubsubChannel);
    }

    //////////////////Sync类//////////////////

    static abstract class Sync extends AbstractQueuedSynchronizer {
        private static final long serialVersionUID = 1192457210091910900L;

        final int maxPermits;
        final JedisPool jedisPool;
        final String key;
        final TimeRecorder timeRecorder;

        Sync(JedisPool jedisPool, TimeRecorder timeRecorder, String key, int permits) {
            this.jedisPool = jedisPool;
            this.timeRecorder = timeRecorder;
            this.key = key;
            this.maxPermits = permits;
        }

        final int getPermits() {
            return TairUtil.poolExecute(jedisPool, jedis -> {
                String value = jedis.get(key);
                if (value == null) {
                    return 0;
                }
                return Integer.valueOf(value);
            });
        }

        final int nonfairTryAcquireShared(int acquires) {
            try {
                ExincrbyParams params = new ExincrbyParams();
                params.max(maxPermits);
                int ret = (int) (maxPermits - TairUtil.poolExecute(jedisPool, jedis -> {
                    TairString tairString = new TairString(jedis);
                    return tairString.exincrBy(key, acquires, params);
                }));
                timeRecorder.beginRecord();
                return ret;
            } catch (Exception e) {
                if (e.getMessage().contains("increment or decrement would overflow")) {
                    return -1;
                }
                throw e;
            }
        }

        protected final boolean tryReleaseShared(int releases) {
            try {
                ExincrbyParams params = new ExincrbyParams();
                params.min(0);
                TairUtil.poolExecute(jedisPool, jedis -> {
                    TairString tairString = new TairString(jedis);
                    return tairString.exincrBy(key, -releases, params);
                });
                timeRecorder.beginRecord();
                return true;
            } catch (Exception e) {
                if (e.getMessage().contains("increment or decrement would overflow")) {
                    return false;
                }
                throw new Error("Maximum permit count exceeded", e);
            }
        }

        final void reducePermits(int reductions) {
            try {
                ExincrbyParams params = new ExincrbyParams();
                params.min(0);
                TairUtil.poolExecute(jedisPool, jedis -> {
                    TairString tairString = new TairString(jedis);
                    return tairString.exincrBy(key, -reductions, params);
                });
            } catch (Exception e) {
                if (e.getMessage().contains("increment or decrement would overflow")) {
                    throw new Error("Permit count underflow");
                }
                throw new Error(e);
            }
        }

        final int drainPermits() {
            return TairUtil.poolExecute(jedisPool, jedis -> {
                TairStringPipeline tairStringPipeline = new TairStringPipeline();
                tairStringPipeline.setClient(jedis.getClient());
                ExsetParams params = new ExsetParams();
                params.xx();
                tairStringPipeline.get(key);
                tairStringPipeline.exset(key, "0", params);
                List<Object> objects = tairStringPipeline.syncAndReturnAll();
                return Integer.valueOf(String.valueOf(objects.get(0)));
            });
        }
    }

    /**
     * 非公平版本
     */
    static final class NonfairSync extends Sync {
        private static final long serialVersionUID = -2694183684443567898L;

        NonfairSync(JedisPool jedisPool, TimeRecorder timeRecorder, String key, int permits) {
            super(jedisPool, timeRecorder, key, permits);
        }

        protected int tryAcquireShared(int acquires) {
            return nonfairTryAcquireShared(acquires);
        }
    }
    //////////////////////////////////////////////////////

    /**
     * ******************************************
     * 下面为调用方法
     * ******************************************
     */
    public String getKey() {
        return key;
    }

    public int getMaxPermits() {
        return sync.maxPermits;
    }

    public void pushRelease() {
        TairUtil.poolExecute(jedisPool, jedis -> jedis.publish(pubsubChannel, "1"));
    }


    public void acquire() throws InterruptedException {
        sync.acquireSharedInterruptibly(1);
    }

    public void acquireUninterruptibly() {
        sync.acquireShared(1);
    }

    public boolean tryAcquire() {
        return sync.nonfairTryAcquireShared(1) >= 0;
    }

    public boolean tryAcquire(long timeout, TimeUnit unit)
            throws InterruptedException {
        return sync.tryAcquireSharedNanos(1, unit.toNanos(timeout));
    }


    public void acquire(int permits) throws InterruptedException {
        if (permits < 0) throw new IllegalArgumentException();
        sync.acquireSharedInterruptibly(permits);
    }

    public void acquireUninterruptibly(int permits) {
        if (permits < 0) throw new IllegalArgumentException();
        sync.acquireShared(permits);
    }

    public boolean tryAcquire(int permits) {
        if (permits < 0) throw new IllegalArgumentException();
        return sync.nonfairTryAcquireShared(permits) >= 0;
    }

    public boolean tryAcquire(int permits, long timeout, TimeUnit unit)
            throws InterruptedException {
        if (permits < 0) throw new IllegalArgumentException();
        boolean ret = sync.tryAcquireSharedNanos(permits, unit.toNanos(timeout));
        if (ret) {
            timeRecorder.beginRecord();
        }
        return ret;
    }

    public void release() {
        release(1);
    }

    public void release(int permits) {
        if (permits < 0) throw new IllegalArgumentException();
        try {
            Long time = timeRecorder.beginTime();
            //防止因节点断开, 重置信号量,但部分请求并未及时完成处理, 导致超出信号量控制, 甚至也可以考虑epoch,
            // 但因为有执行耗时的统计, 就仍然使用获取时间, 因为小于当前时间的, 后续都会重置信号量
            if (time < listener.preParkTime()) {
                return;
            }
            sync.releaseShared(permits);
            pushRelease();//pubsub消息通知
        } finally {
            timeRecorder.endRecord();
        }
    }

    public int availablePermits() {
        return sync.getPermits();
    }

    public int drainPermits() {
        return sync.drainPermits();
    }

    protected void reducePermits(int reduction) {
        if (reduction < 0) throw new IllegalArgumentException();
        sync.reducePermits(reduction);
    }

    public boolean isFair() {
        return false;
    }

    public final boolean hasQueuedThreads() {
        return sync.hasQueuedThreads();
    }

    public final int getQueueLength() {
        return sync.getQueueLength();
    }

    protected Collection<Thread> getQueuedThreads() {
        return sync.getQueuedThreads();
    }

    public String toString() {
        return super.toString() + "[Permits = " + sync.getPermits() + "]";
    }


    private static final Method shardMethod;

    static {
        Class<AbstractQueuedSynchronizer> clazz = AbstractQueuedSynchronizer.class;
        try {
            Method method = clazz.getDeclaredMethod("doReleaseShared");
            method.setAccessible(true);
            shardMethod = method;
        } catch (NoSuchMethodException e) {
            throw new Error(e);
        }
    }
}
