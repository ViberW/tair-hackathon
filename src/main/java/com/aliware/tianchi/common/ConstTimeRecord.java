package com.aliware.tianchi.common;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 常量级的超时停顿时间
 * @since 2022/3/3 15:15
 */
public class ConstTimeRecord implements TimeRecord {
    private final long time;

    public ConstTimeRecord(long time) {
        this.time = time;
    }

    @Override
    public long getTime() {
        return time;
    }
}
