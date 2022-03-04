package com.aliware.tianchi.semaphore;


import com.aliware.tianchi.common.CommonUtil;
import com.aliware.tianchi.common.GlobalExecutor;
import com.aliware.tianchi.common.NamedThreadFactory;
import com.aliware.tianchi.common.TairUtil;
import com.aliware.tianchi.leader.LeaderSelector;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.annotation.PostConstruct;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/3/4 13:19
 */
public class TairSemaphoreTest {


    protected static final String HOST = "r-bp1md046fkp6bftstnpd.redis.rds.aliyuncs.com";
    protected static final int PORT = 6379;
    protected static final String PWD = "Wwb843312160";
    private static String countKey = "test_000001";
    protected static JedisPool jedisPool;
    public static AtomicInteger atomic = new AtomicInteger(0);
    static NamedThreadFactory factory = new NamedThreadFactory("test_");
    static long endTime;

    @BeforeClass
    public static void setUp() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(20);
        jedisPool = new JedisPool(jedisPoolConfig, HOST, PORT, 60 * 1000, PWD);
    }

    @Test
    public void testGetCount() {
        Jedis jedis = new Jedis(HOST, PORT, 60 * 1000);
        jedis.auth(PWD);
        while (true) {
            CommonUtil.sleep(500);
            System.out.println("当前count:" + jedis.get(countKey));
        }
    }

    @Test
    public void testSemaphore() {
        endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(
                30 + ThreadLocalRandom.current().nextInt(30));
        System.out.println("准备启动测试..." + now());
        simulateServer(1, 3);
        System.out.println("所有服务测试完成..." + now());
        GlobalExecutor.instance().stop();
    }

    //模拟服务器
    private static void simulateServer(int serverId, int threadCount) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(20);
        JedisPool jedisPool = new JedisPool(jedisPoolConfig, HOST, PORT, 60 * 1000, PWD);
        System.out.println(serverId + "服务构建selector...");
        LeaderSelector selector = new LeaderSelector(jedisPool, "default");
        selector.start();
        System.out.println(serverId + "服务构建semaphore...");
        TairSemaphore tairSemaphore = new TairSemaphore(selector, jedisPool,
                "test", 5, TimeUnit.SECONDS.toMillis(5), true);
        tairSemaphore.start();
        //模拟多线程
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        long randomTime = endTime + ThreadLocalRandom.current().nextInt(3) * TimeUnit.SECONDS.toMillis(5);
        System.out.println(serverId + "服务构建==开始任务处理..............");
        for (int i = 0; i < threadCount; i++) {
            int threadId = i;
            factory.newThread(() -> {
                try {
                    //执行处理
                    while (System.currentTimeMillis() < randomTime) {
                        tairSemaphore.acquire();
                        try {
                            atomic.incrementAndGet();
                            TairUtil.poolExecute(jedisPool, jedis -> {
                                return jedis.incr(countKey);
                            });
                            Thread.sleep(100 + ThreadLocalRandom.current().nextInt(400));
                            TairUtil.poolExecute(jedisPool, jedis -> {
                                return jedis.decr(countKey);
                            });
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } finally {
                            atomic.getAndDecrement();
                            tairSemaphore.release();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    System.out.println("serverId: " + serverId + ", threadId: " + threadId + "===========ending");
                    countDownLatch.countDown();
                }
            }).start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(serverId + "服务测试完成...");
        tairSemaphore.stop();
        System.out.println(serverId + "集群选择器暂停...");
        selector.stop();
    }

    public static String now() {
        String result = "";
        try {
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            result = dateFormat.format(new Date());
        } catch (Exception e) {
        }
        return result;
    }
}