package com.aliware.tianchi.common;

import java.lang.reflect.Field;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/2/23 10:48
 */
public class QueueUtil {

    public static <T> ReentrantLock lockByQueue(LinkedBlockingQueue<T> queue) {
        Class<? extends BlockingQueue> dwqClass = queue.getClass();
        try {
            Field lock = dwqClass.getDeclaredField("putLock");
            lock.setAccessible(true);
            return (ReentrantLock) lock.get(queue);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
