package com.aliware.tianchi;

import com.aliyun.tair.tairhash.TairHash;
import com.aliyun.tair.tairhash.params.ExhsetParams;
import com.aliyun.tair.tairstring.TairString;
import com.aliyun.tair.tairts.TairTs;
import com.aliyun.tair.tairts.params.ExtsAttributesParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static redis.clients.jedis.Protocol.toByteArray;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/2/16 16:34
 */
public class TairExample {
    protected static final String HOST = "127.0.0.1";
    protected static final int PORT = 6379;
//    protected static final int CLUSTER_PORT = 30001;

    //  tairString = new TairString(jedis);
    //        tairStringPipeline = new TairStringPipeline();
    //        tairStringPipeline.setClient(jedis.getClient());
    //        tairStringCluster = new TairStringCluster(jedisCluster);

    //jedis = new Jedis(HOST, PORT, 60 * 1000); // timeout 60s
    //            if (!"PONG".equals(jedis.ping())) {
    //                System.exit(-1);
    //            }
    //            jedisPool = new JedisPool(HOST, PORT);
    //            Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
    //            jedisClusterNodes.add(new HostAndPort(HOST, CLUSTER_PORT));
    //            jedisCluster = new JedisCluster(jedisClusterNodes);
    public static void main(String[] args) {
        Jedis jedis = new Jedis(HOST, PORT, 60 * 1000);
    }
}
