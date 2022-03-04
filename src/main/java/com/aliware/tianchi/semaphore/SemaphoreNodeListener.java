package com.aliware.tianchi.semaphore;

import com.aliware.tianchi.common.GlobalExecutor;
import com.aliware.tianchi.common.TairUtil;
import com.aliware.tianchi.common.TimeRecord;
import com.aliware.tianchi.leader.LeaderListener;
import com.aliware.tianchi.leader.LeaderSelector;
import com.aliyun.tair.tairstring.TairString;
import com.aliyun.tair.tairstring.TairStringPipeline;
import com.aliyun.tair.tairstring.params.ExsetParams;
import com.aliyun.tair.tairstring.results.ExgetResult;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/3/3 13:47
 */
public class SemaphoreNodeListener implements LeaderListener {

    private static final String BLOCK_KEY_PREFIX = "_hackathon:_block:semaphore:";

    private final JedisPool jedisPool;
    private final TairSemaphore semaphore;
    private final LeaderSelector selector;
    private final String blockKey;
    private final TimeRecord timeRecord;

    /**
     * 保留延长的时长
     */
    private long maxStopAddition = TimeUnit.SECONDS.toMillis(3);

    //0: 正常 1-正在处理
    private final AtomicBoolean state = new AtomicBoolean(true);

    public SemaphoreNodeListener(JedisPool jedisPool, TairSemaphore semaphore, LeaderSelector selector,
                                 TimeRecord timeRecord, String key) {
        this.jedisPool = jedisPool;
        this.semaphore = semaphore;
        this.selector = selector;
        this.blockKey = BLOCK_KEY_PREFIX + key;
        this.timeRecord = timeRecord;
    }


    @Override
    public void onRemoveNode() {
        markClusterStop(); //这里的时间最好能够延迟一定的时间才进行处理并封闭
        if (selector.isMaster()) {
            scheduleStop(true);
        } else if (!state.get()) {
            state.compareAndSet(false, true); //防止意外的断开master注册
        }
    }

    @Override
    public void onAddNode() {
        //....不做处理
    }

    @Override
    public void onBecomeLeader() {
        Boolean exist = TairUtil.poolExecute(jedisPool, jedis -> jedis.exists(blockKey));
        scheduleStop(exist);
    }

    private void scheduleStop(Boolean exist) {
        //先标记好. master已经在处理了
        if (null == exist || !exist) {
            markClusterStop();
        }
        if (state.compareAndSet(true, false)) {
            startClusterStop();
            clearRebuild(timeRecord.getTime());
        }
    }

    /**
     * 标记集群暂停
     */
    private void markClusterStop() {
        long timeout = timeRecord.getTime() + maxStopAddition;
        ExsetParams params = new ExsetParams();
        params.nx().px(timeout);
        TairUtil.poolExecute(jedisPool, jedis -> {
            TairString tairString = new TairString(jedis);
            return tairString.exset(blockKey, "1", params);
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
            if (state.compareAndSet(false, true)) {
                ExsetParams params = new ExsetParams();
                params.xx();
                TairUtil.poolExecute(jedisPool, jedis -> {
                    TairStringPipeline tairStringPipeline = new TairStringPipeline();
                    tairStringPipeline.setClient(jedis.getClient());
                    tairStringPipeline.exset(semaphore.getKey(), "0", params);
                    tairStringPipeline.del(blockKey);
                    tairStringPipeline.sync();
                    return null;
                });
                //手动触发一次通知
                semaphore.pushRelease();
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }

}
