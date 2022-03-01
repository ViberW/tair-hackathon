package com.aliware.tianchi;

import com.aliware.tianchi.leader.LeaderSelector;
import com.aliware.tianchi.semaphore.TairSemaphore;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/2/16 16:34
 */
public class TairExample {
    protected static final String HOST = "r-bp1md046fkp6bftstnpd.redis.rds.aliyuncs.com";
    protected static final int PORT = 6379;


    public static void main(String[] args) throws Exception {
        JedisPool jedisPool = new JedisPool(new JedisPoolConfig(),
                HOST, PORT, 60 * 1000, "Wwb843312160");
        testTairSemaphore(jedisPool);
    }


    static long endTime;

    private static void testTairSemaphore(JedisPool jedisPool) {
        System.out.println("准备启动测试...");
        LeaderSelector selector = new LeaderSelector(jedisPool, "default");
        selector.start();
        endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(60);//执行60秒

        int serverCount = 1;
        CountDownLatch countDownLatch = new CountDownLatch(serverCount);
        System.out.println("启动各个服务...");
        for (int i = 0; i < serverCount; i++) {
            //模拟多个服务器
            int serverId = i;
            new Thread(() -> {
                simulateServer(selector, jedisPool, serverId);
                countDownLatch.countDown();
            }).start();
        }
        System.out.println("所有服务测试完成...");
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        selector.stop();
    }

    //模拟服务器
    private static void simulateServer(LeaderSelector selector, JedisPool jedisPool, int serverId) {
        System.out.println(serverId + "服务构建semaphore...");
        TairSemaphore tairSemaphore = new TairSemaphore(selector, jedisPool,
                "test", 100, TimeUnit.SECONDS.toMillis(5), true);
        tairSemaphore.start();
        //模拟多线程
        int threadCount = 3;
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            int threadId = i;
            new Thread(() -> {
                //执行处理
                while (System.currentTimeMillis() < endTime
                        || (ThreadLocalRandom.current().nextInt(5) == 0)) {
                    tairSemaphore.acquire();
                    try {
                        System.out.println("serverId: " + serverId + ", threadId: " + threadId);
                        Thread.sleep(2000 + ThreadLocalRandom.current().nextInt(1000));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        tairSemaphore.release();
                    }
                }
                countDownLatch.countDown();
            }).start();
        }
        System.out.println(serverId + "服务测试完成...");
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        tairSemaphore.stop();
    }
}
