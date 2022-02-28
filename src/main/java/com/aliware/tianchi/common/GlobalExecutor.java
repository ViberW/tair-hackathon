package com.aliware.tianchi.common;

import java.util.concurrent.*;

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

    public static ExecutorService singleExecutor() {
        return SingletonHolder.SINGLETON.singleExecutor;
    }

    private final ExecutorService singleExecutor;
    private final ScheduledThreadPoolExecutor scheduledExecutor;

    private GlobalExecutor() {
        int thread = Runtime.getRuntime().availableProcessors();
        this.singleExecutor = Executors.newSingleThreadExecutor(
                new NamedThreadFactory("global-single-executor-"));
        this.scheduledExecutor = new ScheduledThreadPoolExecutor(thread,
                new NamedThreadFactory("global-schedule-"));
    }

}