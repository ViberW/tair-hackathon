package com.aliware.tianchi.common.recorder;

import com.aliware.tianchi.common.LifeCycle;

/**
 * @author Viber
 * @version 1.0
 * @apiNote
 * @since 2022/3/3 15:14
 */
public interface TimeRecorder extends LifeCycle {

    /**
     * 计算结果
     *
     * @return
     */
    long calculate();

    /**
     * 开始记录
     */
    void beginRecord();

    /**
     * 结束记录
     */
    void endRecord();

    /**
     * 开始进入的时间
     */
    Long beginTime();
}
