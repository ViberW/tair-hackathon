package com.aliware.tianchi;

import redis.clients.jedis.Jedis;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/2/16 16:34
 */
public class TairExample {
    protected static final String HOST = "127.0.0.1";
    protected static final int PORT = 6379;

    public static void main(String[] args) throws Exception {
        Jedis jedis = new Jedis(HOST, PORT, 60 * 1000);
        testTairCountDownLatch();
        testTairSemaphore();
    }

    private static void testTairCountDownLatch() {

    }

    private static void testTairSemaphore() {

    }
}
