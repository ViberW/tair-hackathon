package com.aliware.tianchi;

import com.aliware.tianchi.common.GlobalExecutor;
import com.aliware.tianchi.common.NamedThreadFactory;
import com.aliware.tianchi.leader.LeaderSelector;
import com.aliware.tianchi.semaphore.TairSemaphore;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/2/16 16:34
 */
public class TairExample {
    protected static final String HOST = "r-bp1md046fkp6bftstnpd.redis.rds.aliyuncs.com";
    protected static final int PORT = 6379;
    protected static final String PWD = "Wwb843312160";


    public static void main(String[] args) throws Exception {
        testTairSemaphore();
    }


    static long endTime;

    private static void testTairSemaphore() {
        GlobalExecutor.schedule().scheduleWithFixedDelay(() -> {
            System.out.println("当前的信号量=====" + atomic.get());
        }, 500, 500, TimeUnit.MILLISECONDS);
        System.out.println("准备启动测试...");
        endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);

        int serverCount = 5;
        CountDownLatch countDownLatch = new CountDownLatch(serverCount);
        System.out.println("启动各个服务...");
        for (int i = 0; i < serverCount; i++) {
            //模拟多个服务器
            int serverId = i;
            factory.newThread(() -> {
                simulateServer(serverId);
                countDownLatch.countDown();
            }).start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("所有服务测试完成...");
        GlobalExecutor.instance().stop();
    }

    public static AtomicInteger atomic = new AtomicInteger(0);
    static NamedThreadFactory factory = new NamedThreadFactory("test_");

    //模拟服务器
    private static void simulateServer(int serverId) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(20);
        JedisPool jedisPool = new JedisPool(jedisPoolConfig, HOST, PORT, 60 * 1000, PWD);
        System.out.println(serverId + "服务构建selector...");
        LeaderSelector selector = new LeaderSelector(jedisPool, "default");
        selector.start();
        System.out.println(serverId + "服务构建semaphore...");
        TairSemaphore tairSemaphore = new TairSemaphore(selector, jedisPool,
                "test", 5, TimeUnit.SECONDS.toMillis(5), false);
        tairSemaphore.start();
        //模拟多线程
        int threadCount = 3;
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
                            Thread.sleep(500 + ThreadLocalRandom.current().nextInt(100));
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
}
