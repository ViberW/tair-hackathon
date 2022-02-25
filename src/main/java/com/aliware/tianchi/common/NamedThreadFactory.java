package com.aliware.tianchi.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/2/18 16:03
 */
public class NamedThreadFactory implements ThreadFactory {

    private final AtomicInteger atomic = new AtomicInteger(1);

    private final String prefix;

    private final boolean daemon;

    private final ThreadGroup group;

    public NamedThreadFactory(String prefix) {
        this(prefix, false);
    }

    public NamedThreadFactory(String prefix, boolean daemon) {
        this.prefix = prefix;
        this.daemon = daemon;
        this.group = Thread.currentThread().getThreadGroup();
    }

    @Override
    public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(group, runnable,
                prefix + atomic.getAndIncrement(), 0);
        thread.setDaemon(daemon);
        return thread;
    }
}
