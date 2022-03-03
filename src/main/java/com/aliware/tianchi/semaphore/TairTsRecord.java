package com.aliware.tianchi.semaphore;

import com.aliware.tianchi.common.*;
import com.aliyun.tair.tairts.TairTs;
import com.aliyun.tair.tairts.params.ExtsAggregationParams;
import com.aliyun.tair.tairts.params.ExtsAttributesParams;
import com.aliyun.tair.tairts.params.ExtsDataPoint;
import com.aliyun.tair.tairts.results.ExtsDataPointResult;
import com.aliyun.tair.tairts.results.ExtsSkeyResult;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
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
    private static final String TS_S_COUNT_KEY_PREFIX = "s_count:";

    //用于ts计算时间的
    private static ThreadLocal<Long> BEGIN_TIME_LOCAL = new ThreadLocal<>();

    private final JedisPool jedisPool;
    private final String tsTimeKey;
    private final String tsCountKey;

    private Integer chunkSize;
    //记录缓存的时间
    private long tsDateEt = TimeUnit.SECONDS.toMillis(30);
    //记录的存储间隔
    private long tsInterval = 200;
    /**
     * 重新构建的超时时间
     */
    private volatile long stopTimeout;


    public TairTsRecord(JedisPool jedisPool, String key, long stopTimeout) {
        this.jedisPool = jedisPool;
        tsTimeKey = TS_S_TIME_KEY_PREFIX + key;
        tsCountKey = TS_S_COUNT_KEY_PREFIX + key;
        chunkSize = CommonUtil.calculateChunkSize(tsDateEt, tsInterval);
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
                //就简单计算了,  其实可以根据多个点的斜率变化, 判断处理时长的变化趋势, 从而选择增大或减少停顿的合适时间
                long l = this.tsCalculate();
                if (l > 0) {
                    this.stopTimeout = (stopTimeout + l) / 2 + 500;//追加500ms
                }
                System.out.println("current_calculate_time: " + stopTimeout);
                startTsCalculate();
            }
        }, 10, TimeUnit.SECONDS);
    }

    public void beginTs() {
        BEGIN_TIME_LOCAL.set(System.currentTimeMillis());
    }

    public void endTs() {
        Long time = BEGIN_TIME_LOCAL.get();
        if (null == time) {
            return;
        }
        long current = System.currentTimeMillis();
        long duration = current - time;
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

    /**
     * 开启定时任务的计算rebuildTimeout, 这里就简单点了
     */
    public long tsCalculate() {
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
            return 0;
        }
        return (long) (time / count);
    }

}
