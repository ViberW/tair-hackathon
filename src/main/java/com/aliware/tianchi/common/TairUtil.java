package com.aliware.tianchi.common;

import com.google.common.base.Function;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;


/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/3/1 15:44
 */
public class TairUtil {

    public static <V> V poolExecute(JedisPool jedisPool,
                                    Function<Jedis, V> function) {
        Jedis resource = jedisPool.getResource();
        try {
            return function.apply(resource);
        } finally {
            resource.close();
        }
    }
}
