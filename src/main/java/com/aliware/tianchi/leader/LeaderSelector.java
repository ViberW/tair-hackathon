package com.aliware.tianchi.leader;

import com.aliware.tianchi.common.AbstractLifeCycle;
import com.aliware.tianchi.common.Constants;
import com.aliware.tianchi.common.GlobalExecutor;
import com.aliware.tianchi.common.TairUtil;
import com.aliyun.tair.ModuleCommand;
import com.aliyun.tair.tairhash.TairHash;
import com.aliyun.tair.tairhash.params.ExhgetwithverResult;
import com.aliyun.tair.tairhash.params.ExhsetParams;
import redis.clients.jedis.*;
import redis.clients.jedis.util.SafeEncoder;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 主节点选择器  适用于集群节点较少, 也不想引入其他额外的中间件
 * @since 2022/2/25 16:43
 */
public class LeaderSelector extends AbstractLifeCycle {

    private static final String KEY_PREFIX = "_hackathon:leader:";
    private static final String PUBSUB_PREFIX = "_pubsub:leader:";
    //通知每个节点 尝试去争抢
    private final JedisPool jedisPool;
    private JedisPubSub pubSub;
    private final String key;
    //pub/sub的通道
    private final String pubsubChannel;

    //节点存在redis中的超时时间,单位秒, 需要大于ticketTimeout
    private int sessionTimeout = 10;
    //校验节点的是否存在,单位秒
    private int ticketTimeout = 5;

    private Integer nodeId;
    private volatile boolean master = false;
    //随便些的监听
    private List<LeaderListener> listeners = new CopyOnWriteArrayList<>();

    public LeaderSelector(JedisPool jedisPool, String name) {
        this.jedisPool = jedisPool;
        this.key = KEY_PREFIX + name;
        this.pubsubChannel = PUBSUB_PREFIX + name;
    }

    @Override
    protected void doStart() {
        //监听pub/sub消息
        this.pubSub = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                //节点变化的通知
                String[] split = message.split("@");
                Integer msgNodeId = Integer.valueOf(split[0]);
                if ("REMOVE".equals(split[0])) {
                    for (LeaderListener listener : listeners) {
                        GlobalExecutor.singleExecutor().execute(() -> {
                            listener.onRemoveNode(master);
                        });
                    }
                } else {
                    for (LeaderListener listener : listeners) {
                        GlobalExecutor.singleExecutor().execute(() -> {
                            listener.onAddNode(master);
                        });
                    }
                }
            }
        };
        new Thread(Thread.currentThread().getThreadGroup(), () -> {
            TairUtil.poolExecute(jedisPool, jedis -> {
                jedis.subscribe(pubSub, pubsubChannel);
                return null;
            });
        }, "leader_subscribe", 0).start();
        while (!pubSub.isSubscribed()) {
            try {
                Thread.sleep(0);
            } catch (InterruptedException e) {
            }
        }
        //注册节点的信息
        registerNode();
        //这里一开始判断是否有主节点在, 并尝试注册主节点
        registerLeader();
    }

    @Override
    protected void doStop() {
        TairUtil.poolExecute(jedisPool, jedis -> jedis.publish(pubsubChannel, nodeId + "@" + "REMOVE"));
        pubSub.unsubscribe(pubsubChannel);
    }

    /**
     * 尝试注册master的信息
     */
    private void registerLeader() {
        GlobalExecutor.schedule().schedule(() -> {
            if (isStart()) {
                try {
                    startRegisterLeader();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                registerLeader();
            }
        }, ticketTimeout, TimeUnit.SECONDS);
    }

    private void startRegisterLeader() {
        if (!master) {
            ExhgetwithverResult<String> exhgetwithver = TairUtil.poolExecute(jedisPool, jedis -> {
                TairHash tairHash = new TairHash(jedis);
                return tairHash.exhgetwithver(key, Constants.NODE_LEADER);
            });
            if (null == exhgetwithver) {
                ExhsetParams params = new ExhsetParams();
                params.nx().ex(sessionTimeout);

                Long exhset = TairUtil.poolExecute(jedisPool, jedis -> {
                    TairHash tairHash = new TairHash(jedis);
                    return tairHash.exhset(key, Constants.NODE_LEADER, String.valueOf(nodeId), params);
                });
                if (null != exhset && exhset == 1) {
                    master = true;
                    for (LeaderListener listener : listeners) {
                        GlobalExecutor.singleExecutor().execute(listener::onBecomeLeader);
                    }
                }
            } else {
                if (exhgetwithver.getValue().equals(String.valueOf(nodeId))) {
                    Boolean exhexpire = TairUtil.poolExecute(jedisPool, jedis -> {
                        TairHash tairHash = new TairHash(jedisPool.getResource());
                        return tairHash.exhexpire(key, String.valueOf(nodeId), sessionTimeout);
                    });
                    if (Boolean.TRUE.equals(exhexpire)) {
                        master = true;
                        return;
                    }
                }
                //校验节点是否存在
                Boolean hexists = TairUtil.poolExecute(jedisPool, jedis -> {
                    TairHash tairHash = new TairHash(jedis);
                    return tairHash.exhexists(key, exhgetwithver.getValue());
                });
                if (null == hexists || !hexists) {
                    ExhsetParams params = new ExhsetParams();
                    params.xx().ex(sessionTimeout).ver(exhgetwithver.getVer());//尝试版本的更新
                    Long exhset = null;
                    try {
                        exhset = TairUtil.poolExecute(jedisPool, jedis -> {
                            TairHash tairHash = new TairHash(jedis);
                            return tairHash.exhset(key, Constants.NODE_LEADER,
                                    String.valueOf(nodeId), params);
                        });
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (null != exhset && exhset == 1) {
                        master = true;
                    }
                }
            }
        } else {
            //延续时间
            TairUtil.poolExecute(jedisPool, jedis -> {
                TairHash tairHash = new TairHash(jedis);
                return tairHash.exhexpire(key, Constants.NODE_LEADER, sessionTimeout);
            });
        }
    }

    /**
     * 注册节点信息, 这里选取 1024 因为正好能够应对 {雪花算法的工作ID}
     */
    private void registerNode() {
        int newNodeId = null == nodeId ? ThreadLocalRandom.current().nextInt(1024) : nodeId;
        do {
            newNodeId++;
            String field = String.valueOf(newNodeId);
            ExhsetParams params = new ExhsetParams();
            params.nx().ex(sessionTimeout);

            Long exhset = TairUtil.poolExecute(jedisPool, jedis -> {
                TairHash tairHash = new TairHash(jedis);
                return tairHash.exhset(key, field,
                        String.valueOf(System.currentTimeMillis()), params);
            });
            if (null != exhset && exhset >= 0) {
                System.out.println("success register node:" + newNodeId);
                nodeId = newNodeId;
                ticketKeepalive();
                TairUtil.poolExecute(jedisPool, jedis -> jedis.publish(pubsubChannel, nodeId + "@" + "ADD"));
                break;
            }
        } while (true);
    }

    /**
     * 开启定时任务, 不断的维持ttl
     */
    private void ticketKeepalive() {
        GlobalExecutor.schedule().schedule(() -> {
            if (isStart()) {
                TairUtil.poolExecute(jedisPool, jedis -> {
                    TairHash tairHash = new TairHash(jedisPool.getResource());
                    return tairHash.exhexpire(key, String.valueOf(nodeId), sessionTimeout);
                });
                ticketKeepalive();
            }
        }, ticketTimeout, TimeUnit.SECONDS);
    }

    /**
     * 注册监听
     */
    public void registerListener(LeaderListener listener) {
        listeners.add(listener);
    }

    /**
     * 取消监听
     */
    public void unRegisterListener(LeaderListener listener) {
        listeners.remove(listener);
    }

}
