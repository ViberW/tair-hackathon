package com.aliware.tianchi.semaphore;

import com.aliware.tianchi.common.AbstractLifeCycle;
import com.aliware.tianchi.common.GlobalExecutor;
import com.aliware.tianchi.common.TairUtil;
import com.aliware.tianchi.leader.LeaderListener;
import com.aliyun.tair.tairstring.TairString;
import com.aliyun.tair.tairstring.TairStringPipeline;
import com.aliyun.tair.tairstring.params.ExsetParams;
import com.aliyun.tair.tairstring.results.ExgetResult;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.TimeUnit;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/3/3 13:47
 */
public class SemaphoreNodeListener extends AbstractLifeCycle implements LeaderListener {

    private static final String BLOCK_KEY_PREFIX = "_hackathon:_block:semaphore:";

    private final JedisPool jedisPool;
    private final TairSemaphore semaphore;

    /**
     * ts时间计算相关的
     */
    private boolean tsCalculate;
    private TairTsRecord tairTsRecord;
    /**
     * 重新构建的超时时间
     */
    private volatile long stopTimeout;

    /**
     * 最多保留时长
     */
    private long maxStopAddition = TimeUnit.SECONDS.toMillis(3);

    public SemaphoreNodeListener(JedisPool jedisPool, TairSemaphore semaphore,
                                 String key, boolean tsCalculate) {
        this.jedisPool = jedisPool;
        this.semaphore = semaphore;
        this.tsCalculate = tsCalculate;
        if (tsCalculate) {
            tairTsRecord = new TairTsRecord(jedisPool, key);
        }
    }

    @Override
    protected void doStart() {
        if (tsCalculate) {
            startTsCalculate();
        }
    }

    @Override
    public void onRemoveNode(boolean leader) {
        markClusterStop(stopTimeout);
        if (leader) {
            startClusterStop();
            clearRebuild(stopTimeout);
        }
    }

    @Override
    public void onAddNode(boolean leader) {
        //....不做处理
    }

    @Override
    public void onBecomeLeader() {
        Long ttl = TairUtil.poolExecute(jedisPool, jedis -> jedis.ttl(BLOCK_KEY_PREFIX));
        if (null == ttl || ttl <= 0) {
            markClusterStop(stopTimeout);
            startClusterStop();
            ttl = stopTimeout;
        } else {
            ttl = TimeUnit.SECONDS.toMillis(ttl);
        }
        clearRebuild(ttl);
    }

    private void startTsCalculate() {
        GlobalExecutor.schedule().schedule(() -> {
            if (isStart()) {
                // 开启定时任务的计算stopTimeout
                long l = tairTsRecord.tsCalculate();
                if (l > 0) {
                    this.stopTimeout = (stopTimeout + l) / 2 + 500;//追加500ms
                }
                startTsCalculate();
            }
        }, 10, TimeUnit.SECONDS);
    }

    /**
     * 标记集群暂停
     *
     * @param timeout 超时时间
     */
    private void markClusterStop(long timeout) {
        ExsetParams params = new ExsetParams();
        params.nx().px(timeout + maxStopAddition);

        TairUtil.poolExecute(jedisPool, jedis -> {
            TairString tairString = new TairString(jedis);
            return tairString.exset(BLOCK_KEY_PREFIX, "1", params);
        });
    }

    /**
     * 开始设置集群暂停
     */
    private void startClusterStop() {
        ExsetParams params = new ExsetParams();
        params.xx();
        TairUtil.poolExecute(jedisPool, jedis -> {
            TairString tairString = new TairString(jedis);
            tairString.exset(semaphore.getKey(), String.valueOf(2 * semaphore.getMaxPermits() + 1), params);
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
                return tairString.exget(semaphore.getKey());
            });
            if (exget == null || Integer.parseInt(exget.getValue()) <= semaphore.getMaxPermits()) {
                //说明已经被有重置过了
                return;
            }
            ExsetParams params = new ExsetParams();
            params.xx();
            TairUtil.poolExecute(jedisPool, jedis -> {
                TairStringPipeline tairStringPipeline = new TairStringPipeline();
                tairStringPipeline.setClient(jedis.getClient());
                tairStringPipeline.exset(semaphore.getKey(), "0", params);
                tairStringPipeline.del(BLOCK_KEY_PREFIX);
                tairStringPipeline.sync();
                return null;
            });
            //手动触发一次通知
            semaphore.pushRelease();
        }, timeout, TimeUnit.MILLISECONDS);
    }

}
