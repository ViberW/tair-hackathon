package com.aliware.tianchi.common;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/2/23 9:50
 */
public abstract class AbstractLifeCycle implements LifeCycle {
    private volatile boolean start = false;

    @Override
    public void start() {
        this.start = true;
        doStart();
    }

    protected void doStart() {
    }

    @Override
    public void stop() {
        this.start = false;
        doStop();
    }

    protected void doStop() {
    }

    @Override
    public boolean isStart() {
        return this.start;
    }

    protected void checkState() {
        if (!isStart()) {
            throw new IllegalStateException(this.getClass().getSimpleName() + " is stopped");
        }
    }
}
