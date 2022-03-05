package com.aliware.tianchi.common.recorder;

import com.aliware.tianchi.common.AbstractLifeCycle;

public abstract class AbstractTimeRecorder extends AbstractLifeCycle implements TimeRecorder {

    protected ThreadLocal<Long> TIME_RECORD_LOCAL = ThreadLocal.withInitial(() -> 0L);

    @Override
    public void beginRecord() {
        TIME_RECORD_LOCAL.set(System.currentTimeMillis());
    }

    @Override
    public void endRecord() {
        try {
            doEndRecord();
        } finally {
            TIME_RECORD_LOCAL.remove();
        }
    }

    protected abstract void doEndRecord();

    @Override
    public Long beginTime() {
        return TIME_RECORD_LOCAL.get();
    }
}
