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
import com.aliyun.tair.tairts.TairTs;
import com.aliyun.tair.tairts.params.ExtsAggregationParams;
import com.aliyun.tair.tairts.params.ExtsAttributesParams;
import com.aliyun.tair.tairts.params.ExtsDataPoint;
import com.aliyun.tair.tairts.results.ExtsDataPointResult;
import com.aliyun.tair.tairts.results.ExtsSkeyResult;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.ArrayList;
import java.util.List;
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

    private static final ThreadLocal<Long> BEGIN_TIME_LOCAL = new ThreadLocal<>();
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
    private LeaderListener listener;

    private LeaderSelector selector;

    /**
     * 重新构建的超时时间
     */
    private volatile long rebuildTimeout;

    private boolean tsCalculate;
    private String tsTimeKey;
    private String tsCountKey;
    private Integer chunkSize = 256;
    //记录缓存的时间
    private long tsDateEt = TimeUnit.SECONDS.toMillis(30);
    //记录的存储间隔
    private long tsInterval = 200;

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
            tsTimeKey = TS_S_TIME_KEY_PREFIX + key;
            tsCountKey = TS_S_COUNT_KEY_PREFIX + key;
            chunkSize = CommonUtil.calculateChunkSize(tsDateEt, tsInterval);
            //开启定时任务的计算
            startTsCalculate();
        }
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
                markRebuild(rebuildTimeout);
                if (leader) {
                    startRebuild();
                    clearRebuild(rebuildTimeout);
                }
            }

            @Override
            public void onAddNode(boolean leader) {
                //....不做处理
            }

            @Override
            public void onBecomeLeader() {
                //这里变为Leader
                Long ttl = TairUtil.poolExecute(jedisPool, jedis -> jedis.ttl(BLOCK_KEY_PREFIX));
                if (null == ttl || ttl == -1) {
                    markRebuild(rebuildTimeout);
                    startRebuild();
                    ttl = rebuildTimeout;
                } else {
                    ttl = TimeUnit.SECONDS.toMillis(ttl == 0 ? 1 : ttl);
                }
                clearRebuild(ttl);
            }
        };
    }

    @Override
    protected void doStart() {
        new Thread(() -> {
            TairUtil.poolExecute(jedisPool, jedis -> {
                jedis.subscribe(pubSub, pubsubChannel);
                return null;
            });
        }).start();
        selector.registerListener(listener);
    }

    @Override
    protected void doStop() {
        pubSub.unsubscribe(pubsubChannel);
        selector.unRegisterListener(listener);
    }

    private void startTsCalculate() {
        GlobalExecutor.schedule().schedule(() -> {
            if (isStart()) {
                this.tsCalculate();
                startTsCalculate();
            }
        }, 10, TimeUnit.SECONDS);
    }

    /**
     * 开启定时任务的计算rebuildTimeout
     */
    private void tsCalculate() {
        long current = System.currentTimeMillis();
        long toTs = current - (current % tsInterval);
        //近10s的变化
        long fromTs = toTs - (TimeUnit.SECONDS.toMillis(9) / tsInterval);
        ExtsAggregationParams paramsAgg = new ExtsAggregationParams();
        paramsAgg.maxCountSize(3);
        paramsAgg.aggSum(3000); //每3秒

        List<ExtsSkeyResult> extsmrange = TairUtil.poolExecute(jedisPool, jedis -> {
            TairTs tairTs = new TairTs(jedis);
            return tairTs.extsmrange(TS_PKEY, new ArrayList<String>() {{
                add(tsTimeKey);
                add(tsCountKey);
            }}, String.valueOf(fromTs), String.valueOf(toTs), paramsAgg);
        });
        //todo 计算结点的变化

        double time = 0;
        double count = 0;
        for (ExtsSkeyResult extsSkeyResult : extsmrange) {
            List<ExtsDataPointResult> dataPoints = extsSkeyResult.getDataPoints();
            boolean isTime = tsTimeKey.equals(extsSkeyResult.getSkey());
            if (isTime) {
                time = dataPoints.get(dataPoints.size() - 1).getDoubleValue();
            } else {
                count = dataPoints.get(dataPoints.size() - 1).getDoubleValue();
            }
        }
        if (count == 0 || time == 0) {
            return;
        }
        //500毫秒的补偿
        long timeout = (long) ((rebuildTimeout + time / count) / 2) + TimeUnit.MILLISECONDS.toMillis(500);
        this.rebuildTimeout = timeout;
    }

    /**
     * 开启重建任务
     *
     * @param timeout 检测时间
     */
    private void markRebuild(long timeout) {
        ExsetParams params = new ExsetParams();
        params.nx().px(timeout + TimeUnit.SECONDS.toMillis(3));

        TairUtil.poolExecute(jedisPool, jedis -> {
            TairString tairString = new TairString(jedis);
            return tairString.exset(BLOCK_KEY_PREFIX, "1", params);
        });
    }

    private void startRebuild() {
        ExsetParams params = new ExsetParams();
        params.xx();
        TairUtil.poolExecute(jedisPool, jedis -> {
            TairStringPipeline tairStringPipeline = new TairStringPipeline();
            tairStringPipeline.setClient(jedis.getClient());
            tairStringPipeline.exset(key, String.valueOf(2 * maxPermits + 1), params);
            tairStringPipeline.sync();
            return null;
        });
    }

    /**
     * 重新启动任务处理
     *
     * @param timeout 检测时间
     */
    private void clearRebuild(long timeout) {
        GlobalExecutor.schedule().schedule(() -> {
            ExgetResult<String> exget = TairUtil.poolExecute(jedisPool, jedis -> {
                TairString tairString = new TairString(jedis);
                return tairString.exget(key);
            });
            if (exget == null || Integer.parseInt(exget.getValue()) > maxPermits) {
                //说明已经有重置过了
                return;
            }
            ExsetParams params = new ExsetParams();
            params.xx();
            TairUtil.poolExecute(jedisPool, jedis -> {
                TairStringPipeline tairStringPipeline = new TairStringPipeline();
                tairStringPipeline.setClient(jedis.getClient());
                tairStringPipeline.exset(key, "0", params);
                tairStringPipeline.del(BLOCK_KEY_PREFIX);
                tairStringPipeline.sync();
                return null;
            });
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
    public void acquire() {
        acquire(1);
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
                        if (tsCalculate) {
                            BEGIN_TIME_LOCAL.set(System.currentTimeMillis());
                        }
                        return;
                    }
                } else {
                    LockSupport.park();
                }
            }
        }
        //记录当前的执行耗时
        if (tsCalculate) {
            BEGIN_TIME_LOCAL.set(System.currentTimeMillis());
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
            //记录到TairTs中, 用于计算处理的真实耗时
            if (tsCalculate) {
                long current = System.currentTimeMillis();
                long duration = current - BEGIN_TIME_LOCAL.get();
                ExtsAttributesParams params = new ExtsAttributesParams();
                //统计近30秒内的数据信息
                params.dataEt(tsDateEt).uncompressed().chunkSize(chunkSize);
                current = current - (current % tsInterval);
                ArrayList<ExtsDataPoint<String>> skeys = new ArrayList<>();
                skeys.add(new ExtsDataPoint<>(tsTimeKey, String.valueOf(current), duration));
                skeys.add(new ExtsDataPoint<>(tsCountKey, String.valueOf(current), 1));

                TairUtil.poolExecute(jedisPool, jedis -> {
                    TairTs tairTs = new TairTs(jedis);
                    return tairTs.extsmrawincr(TS_PKEY, skeys, params);
                });
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
            if (e.getMessage().contains("increment or decrement would overflow")) {
                return true; //这里认为是true吧
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
