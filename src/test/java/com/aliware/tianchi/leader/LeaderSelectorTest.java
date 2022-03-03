package com.aliware.tianchi.leader;

import com.aliware.tianchi.common.CommonUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/3/3 14:23
 */
public class LeaderSelectorTest {

    protected static final String HOST = "r-bp1md046fkp6bftstnpd.redis.rds.aliyuncs.com";
    protected static final int PORT = 6379;
    protected static final String PWD = "Wwb843312160";
    protected static JedisPool jedisPool;

    @BeforeClass
    public static void setUp() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(20);
        jedisPool = new JedisPool(jedisPoolConfig, HOST, PORT, 60 * 1000, PWD);
    }

    @Test
    public void testLeader() {
        LeaderSelector selector = new LeaderSelector(jedisPool, "default");
        selector.registerListener(new LeaderListener() {
            @Override
            public void onRemoveNode() {
                System.out.println("onRemoveNode==========");
            }

            @Override
            public void onAddNode() {
                System.out.println("onAddNode==========");
            }

            @Override
            public void onBecomeLeader() {
                System.out.println("onBecomeLeader==========");
            }
        });
        selector.start();
        CommonUtil.sleep(TimeUnit.SECONDS.toMillis(ThreadLocalRandom.current().nextInt(30) + 30));
        selector.stop();
        System.out.println(System.currentTimeMillis());
    }
}