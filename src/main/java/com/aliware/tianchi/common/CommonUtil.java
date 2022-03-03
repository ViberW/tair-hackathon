package com.aliware.tianchi.common;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/2/28 17:00
 */
public class CommonUtil {

    static NamedThreadFactory PUBSUB_FACTORY = new NamedThreadFactory("pubsub_");

    public static Integer calculateChunkSize(long timeRange, long timeInterval) {
        long size = timeRange / timeInterval;
        if (size > 5000) {
            return 256;
        } else {
            return (int) (size / 20);
        }
    }

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
