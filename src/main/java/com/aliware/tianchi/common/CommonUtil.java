package com.aliware.tianchi.common;

import java.util.Set;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/2/28 17:00
 */
public class CommonUtil {

    static NamedThreadFactory PUBSUB_FACTORY = new NamedThreadFactory("pubsub_");

    public static Thread pubsubThread(Runnable runnable) {
        return PUBSUB_FACTORY.newThread(runnable);
    }

    public static void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
