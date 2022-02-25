package com.aliware.tianchi.common;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 全局线程池
 * @since 2022/2/18 15:59
 */
public class GlobalExecutor {

    private static class SingletonHolder {
        private static final GlobalExecutor SINGLETON = new GlobalExecutor();
    }

    public static ScheduledThreadPoolExecutor schedule() {
        return SingletonHolder.SINGLETON.scheduledExecutor;
    }

    public static ThreadPoolExecutor executor() {
        return SingletonHolder.SINGLETON.executor;
    }

    private final ThreadPoolExecutor executor;
    private final ScheduledThreadPoolExecutor scheduledExecutor;

    private GlobalExecutor() {
        int thread = Runtime.getRuntime().availableProcessors();
        this.executor = new ThreadPoolExecutor(thread, thread, 0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), new NamedThreadFactory("global-executor-"));
        this.scheduledExecutor = new ScheduledThreadPoolExecutor(thread,
                new NamedThreadFactory("global-schedule-"));
    }

}
