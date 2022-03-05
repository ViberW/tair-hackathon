package com.aliware.tianchi.common.recorder;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 常量级的超时停顿时间
 * @since 2022/3/3 15:15
 */
public class ConstTimeRecorder extends AbstractTimeRecorder {

    private final long time;

    public ConstTimeRecorder(long time) {
        this.time = time;
    }

    @Override
    public long calculate() {
        return time;
    }

    @Override
    protected void doEndRecord() {
    }
}
