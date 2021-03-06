package com.aliware.tianchi.countdownlatch;

import com.aliware.tianchi.common.GlobalExecutor;
import com.aliyun.tair.tairstring.TairString;
import com.aliyun.tair.tairstring.params.ExincrbyParams;
import com.aliyun.tair.tairstring.params.ExsetParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 这个对象需要进行释放, 否则定时任务的线程池不断持有
 * ----- 这里考虑到的节点下线情况就是强制的事项
 * @since 2022/2/22 16:01
 */
public class TairCountDownLatch {

    private static final String KEY_PREFIX = "_hackathon:countdown:";
    private static final String PUBSUB_PREFIX = "_pubsub:countdown:";
    //通知每个节点 尝试去争抢
    private final Jedis jedis;
    private final TairString tairString;
    private final String key;
    private final String pubsubChannel;
    private final int maxCount;
    //使用一个本地的queue
    private AtomicBoolean release = new AtomicBoolean(false);
    private volatile Thread thread;
    private JedisPubSub pubSub;

    public TairCountDownLatch(Jedis jedis, String key, int count) {
        this.jedis = jedis;
        this.tairString = new TairString(jedis);
        this.key = KEY_PREFIX + key;
        this.pubsubChannel = PUBSUB_PREFIX + key;
        this.maxCount = count;
        this.pubSub = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                releaseThread();
            }
        };
        //订阅
        jedis.subscribe(pubSub, pubsubChannel);
        //直接写入到redis中去
        ExsetParams params = new ExsetParams();
        params.nx(); //不存在时才插入
        tairString.exset(key, String.valueOf(maxCount), params);
        releaseUntil();
    }

    /**
     * 开启定时任务, 防止消息通知没有接收到
     */
    private void releaseUntil() {
        String s = jedis.get(key);
        if (null == s || "0".equals(s)) {
            jedis.publish(pubsubChannel, "1");
            releaseThread();
        } else {
            GlobalExecutor.schedule().schedule(this::releaseUntil, 500, TimeUnit.MILLISECONDS);
        }
    }

    private void releaseThread() {
        if (release.compareAndSet(false, true)) {
            if (null != thread) {
                LockSupport.unpark(thread);
            }
            pubSub.unsubscribe(pubsubChannel);
            jedis.del(key);
        }
    }

    public void countDown() {
        if (tryRelease(1) == 0) {
            try {
                jedis.publish(pubsubChannel, "1");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 因为分布式的场景下, 使用await() 若是在节点下线时, 出现死锁情况, 所以使用await()
     */
    @Deprecated
    public void await() {
        while (!release.get()) {
            this.thread = Thread.currentThread();
            LockSupport.park();
        }
    }

    /**
     * !!!这里的语义就变成了, 等待多长时间, 若是到达时间点,则强制解锁
     *
     * @param timeout 最大容忍时长
     * @param unit
     * @return true 代表正常  false 代表强制解锁
     */
    public boolean await(long timeout, TimeUnit unit) {
        //强制设置
        long nanosTimeout = unit.toNanos(timeout);
        long deadline = System.nanoTime() + nanosTimeout;
        boolean ok = true;
        while (!release.get()) {
            this.thread = Thread.currentThread();
            nanosTimeout = deadline - System.nanoTime();
            if (nanosTimeout <= 0) {
                ok = false;
                break;
            } else if (nanosTimeout > 1000L) {
                LockSupport.park(nanosTimeout);
            }
        }
        if (!ok) {
            //强制解锁
            if (release.compareAndSet(false, true)) {
                pubSub.unsubscribe(pubsubChannel);
                jedis.del(key);
            }
            jedis.publish(pubsubChannel, "1");
        }
        return ok;
    }

    private Long tryRelease(int permits) {
        try {
            ExincrbyParams params = new ExincrbyParams();
            params.xx().min(0);
            return tairString.exincrBy(key, -permits, params);
        } catch (Exception e) {
            if (e.getMessage().contains("increment or decrement would overflow")) {
                return 0L;
            }
            throw e;
        }
    }

}
