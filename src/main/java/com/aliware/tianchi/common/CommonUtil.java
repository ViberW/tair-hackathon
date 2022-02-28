package com.aliware.tianchi.common;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/2/28 17:00
 */
public class CommonUtil {

    public static Integer calculateChunkSize(long timeRange, long timeInterval) {
        long size = timeRange / timeInterval;
        if (size > 5000) {
            return 256;
        } else {
            return (int) (size / 20);
        }
    }
}
