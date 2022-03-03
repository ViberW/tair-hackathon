package com.aliware.tianchi.leader;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 这里的监听就简单写了
 * @since 2022/2/25 17:13
 */
public interface LeaderListener {

    /**
     * master在检测节点移除时
     */
    void onRemoveNode();

    /**
     * master在检测节点添加时
     */
    void onAddNode();

    /**
     * node变为master时
     */
    void onBecomeLeader();
}
