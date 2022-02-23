package com.aliware.tianchi;

import java.util.concurrent.ScheduledThreadPoolExecutor;

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
        return SingletonHolder.SINGLETON.scheduledThreadPoolExecutor;
    }

    private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    private GlobalExecutor() {
        int thread = Runtime.getRuntime().availableProcessors();
        this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(thread,
                new NamedThreadFactory("global-executor-"));
    }

}
