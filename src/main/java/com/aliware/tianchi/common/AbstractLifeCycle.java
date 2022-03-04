package com.aliware.tianchi.common;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/2/23 9:50
 */
public abstract class AbstractLifeCycle implements LifeCycle {
    private final AtomicBoolean state = new AtomicBoolean(false);

    @Override
    public void start() {
        if (this.state.compareAndSet(false, true)) {
            doStart();
        }
    }

    protected void doStart() {
    }

    @Override
    public void stop() {
        if (this.state.compareAndSet(true, false)) {
            doStop();
        }
    }

    protected void doStop() {
    }

    @Override
    public boolean isStart() {
        return this.state.get();
    }

}
