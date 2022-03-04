package com.aliware.tianchi.semaphore;

import com.aliware.tianchi.common.AbstractLifeCycle;
import com.aliware.tianchi.common.GlobalExecutor;
import com.aliware.tianchi.common.TairUtil;
import com.aliware.tianchi.common.TimeRecord;
import com.aliyun.tair.tairts.TairTs;
import com.aliyun.tair.tairts.params.ExtsAggregationParams;
import com.aliyun.tair.tairts.params.ExtsAttributesParams;
import com.aliyun.tair.tairts.results.ExtsDataPointResult;
import com.aliyun.tair.tairts.results.ExtsSkeyResult;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/3/3 13:36
 */
public class TairTsRecord extends AbstractLifeCycle implements TimeRecord {

    private static final String TS_PKEY = "_ts:ts:pkey";
    private static final String TS_S_TIME_KEY_PREFIX = "s_time:";

    //用于ts计算时间的
    private static ThreadLocal<Long> BEGIN_TIME_LOCAL = new ThreadLocal<>();

    private final JedisPool jedisPool;
    private final String tsTimeKey;

    //记录缓存的时间
    private long tsDateEt = TimeUnit.SECONDS.toMillis(10);
    private long tsInterval = 15; //每16秒的一次数据
    /**
     * 重新构建的超时时间
     */
    private volatile long stopTimeout;


    public TairTsRecord(JedisPool jedisPool, String key, long stopTimeout) {
        this.jedisPool = jedisPool;
        tsTimeKey = TS_S_TIME_KEY_PREFIX + key;
        this.stopTimeout = stopTimeout;
    }

    @Override
    public long getTime() {
        return stopTimeout;
    }

    @Override
    protected void doStart() {
        startTsCalculate();
    }

    private void startTsCalculate() {
        GlobalExecutor.schedule().schedule(() -> {
            if (isStart()) {
                //就简单计算了
                long l = this.tsCalculate();
                if (l > 0) {
                    this.stopTimeout = (stopTimeout + l) / 2 + 500;//追加500ms
                }
                System.out.println("current_calculate_time: " + stopTimeout);
                startTsCalculate();
            }
        }, 10, TimeUnit.SECONDS);//这个时间可以弄成自定义
    }

    public void beginTs() {
        BEGIN_TIME_LOCAL.set(System.currentTimeMillis());
    }

    public void endTs() {
        Long time = BEGIN_TIME_LOCAL.get();
        if (null != time) {
            long current = System.currentTimeMillis();
            current = current - (current & tsInterval);
            long duration = current - time;
            ExtsAttributesParams params = new ExtsAttributesParams();
            params.dataEt(tsDateEt).uncompressed();
            //这里若相同时 则直接覆盖吧
            String skey = String.valueOf(current);
            TairUtil.poolExecute(jedisPool, jedis -> {
                TairTs tairTs = new TairTs(jedis);
                return tairTs.extsadd(TS_PKEY, tsTimeKey, skey, duration, params);
            });
        }
    }

    /**
     * 开启定时任务的计算rebuildTimeout, 这里就简单点了
     */
    public long tsCalculate() {
        long toTs = System.currentTimeMillis();
        long fromTs = toTs - TimeUnit.SECONDS.toMillis(5);
        ExtsAggregationParams paramsAgg = new ExtsAggregationParams();
        paramsAgg.maxCountSize(1);
        paramsAgg.aggMax(5000); //5秒

        ExtsSkeyResult extsSkeyResult = TairUtil.poolExecute(jedisPool, jedis -> {
            TairTs tairTs = new TairTs(jedis);
            return tairTs.extsrange(TS_PKEY, tsTimeKey, String.valueOf(fromTs),
                    String.valueOf(toTs), paramsAgg);
        });
        List<ExtsDataPointResult> dataPoints = extsSkeyResult.getDataPoints();
        if (null == dataPoints || dataPoints.isEmpty()) {
            return 0;
        }
        double avg = dataPoints.get(0).getDoubleValue();
        if (avg == 0) {
            return 0;
        }
        return (long) avg;
    }

}