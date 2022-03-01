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
    private int sessionTimeout = 30;
    //校验节点的是否存在,单位秒
    private int ticketTimeout = 15;

    private Integer nodeId;
    private Long nodeVersion = 1L;
    private volatile boolean master = false;
    private ConcurrentHashMap<Integer, Boolean> map = new ConcurrentHashMap<>();
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
                    map.remove(msgNodeId);
                    for (LeaderListener listener : listeners) {
                        GlobalExecutor.singleExecutor().execute(() -> {
                            listener.onRemoveNode(master);
                        });
                    }
                } else {
                    map.put(msgNodeId, Boolean.TRUE);
                    for (LeaderListener listener : listeners) {
                        GlobalExecutor.singleExecutor().execute(() -> {
                            listener.onAddNode(master);
                        });
                    }
                }
            }
        };
        new Thread(() -> {
            Jedis resource = jedisPool.getResource();
            try {
                resource.subscribe(pubSub, pubsubChannel);
            } finally {
                resource.close();
            }
        }).start();
        //注册节点的信息
        registerNode();
        //这里一开始判断是否有主节点在, 并尝试注册主节点
        registerLeader();
    }

    @Override
    protected void doStop() {
        Jedis jedis = jedisPool.getResource();
        try {
            jedis.publish(pubsubChannel, nodeId + "@" + "REMOVE");
        } finally {
            jedis.close();
        }
        pubSub.unsubscribe(pubsubChannel);
    }

    /**
     * 尝试注册master的信息
     */
    private void registerLeader() {
        GlobalExecutor.schedule().scheduleWithFixedDelay(() -> {
            if (isStart()) {
                if (!master) {
                    ExhgetwithverResult<String> exhgetwithver = TairUtil.poolExecute(jedisPool, jedis -> {
                        TairHash tairHash = new TairHash(jedisPool.getResource());
                        return tairHash.exhgetwithver(key, Constants.NODE_LEADER);
                    });
                    if (null == exhgetwithver) {
                        ExhsetParams params = new ExhsetParams();
                        params.nx().ex(sessionTimeout);

                        Long exhset = TairUtil.poolExecute(jedisPool, jedis -> {
                            TairHash tairHash = new TairHash(jedisPool.getResource());
                            return tairHash.exhset(key, Constants.NODE_LEADER, String.valueOf(nodeId), params);
                        });
                        if (null != exhset && exhset == 1) {
                            master = true;
                            for (LeaderListener listener : listeners) {
                                GlobalExecutor.singleExecutor().execute(listener::onBecomeLeader);
                            }
                        }
                    } else {
                        //校验节点是否存在
                        Boolean hexists = TairUtil.poolExecute(jedisPool, jedis -> {
                            TairHash tairHash = new TairHash(jedisPool.getResource());
                            return tairHash.exhexists(key, exhgetwithver.getValue());
                        });
                        if (null == hexists || !hexists) {
                            ExhsetParams params = new ExhsetParams();
                            params.xx().ex(sessionTimeout).ver(exhgetwithver.getVer());//尝试版本的更新


                            Long exhset = TairUtil.poolExecute(jedisPool, jedis -> {
                                TairHash tairHash = new TairHash(jedisPool.getResource());
                                return tairHash.exhset(key, Constants.NODE_LEADER,
                                        String.valueOf(nodeId), params);
                            });
                            if (null != exhset && exhset == 1) {
                                master = true;
                            }
                        }
                    }
                }
            }
        }, ticketTimeout, ticketTimeout, TimeUnit.SECONDS);
    }

    /**
     * 注册节点信息, 这里选取 1024 因为正好能够应对 {雪花算法的工作ID}
     */
    private void registerNode() {
        int newNodeId = null == nodeId ? ThreadLocalRandom.current().nextInt(1024) : nodeId;
        int oldId = newNodeId;
        do {
            while (map.containsKey(newNodeId)) {
                newNodeId++;
                if (newNodeId >= 1024) {
                    newNodeId = 0;
                }
                if (oldId == newNodeId) {
                    throw new IllegalStateException("节点过多");
                }
            }
            String field = String.valueOf(newNodeId);
            ExhsetParams params = new ExhsetParams();
            params.nx().ex(sessionTimeout);


            Long exhset = TairUtil.poolExecute(jedisPool, jedis -> {
                TairHash tairHash = new TairHash(jedisPool.getResource());
                return tairHash.exhset(key, field,
                        String.valueOf(System.currentTimeMillis()), params);
            });
            if (null != exhset && exhset >= 0) {
                ticketKeepalive();
                nodeId = newNodeId;
                nodeVersion = 1L;
                Jedis jedis = jedisPool.getResource();
                try {
                    jedis.publish(pubsubChannel, nodeId + "@" + "ADD");
                } finally {
                    jedis.close();
                }
                break;
            }
        } while (true);
    }

    /**
     * 开启定时任务, 不断的维持ttl
     */
    private void ticketKeepalive() {
        GlobalExecutor.schedule().scheduleWithFixedDelay(() -> {
            if (isStart()) {
//                tairHash.exhexpire(key, String.valueOf(nodeId), sessionTimeout);
                Boolean exhexpire = exhexpire(key, String.valueOf(nodeId), sessionTimeout, nodeVersion);
                if (Boolean.TRUE.equals(exhexpire)) {
                    //更新当前节点的版本号
                    nodeVersion++;
                } else {
                    //说明维持注册的信息发生了变化,可能因为应用失败
                    master = false;
                    registerNode();
                }
            }
        }, ticketTimeout, ticketTimeout, TimeUnit.SECONDS);
    }


    /**
     * 由于version的参数并未在1.9.0版本开放, 所以只能手动添加version的设置了
     *
     * @param key     hashkey
     * @param field   hashfield
     * @param seconds ttl时间
     * @param version 当前的版本
     * @return
     */
    @SuppressWarnings("all")
    private Boolean exhexpire(String key, String field, int seconds, long version) {
        try {
            Jedis jedis = jedisPool.getResource();
            try {
                Object obj = jedis.sendCommand(ModuleCommand.EXHEXPIRE,
                        new byte[][]{SafeEncoder.encode(key), SafeEncoder.encode(field),
                                Protocol.toByteArray(seconds), SafeEncoder.encode("ver"),
                                SafeEncoder.encode(String.valueOf(version))});
                return BuilderFactory.BOOLEAN.build(obj);
            } finally {
                jedis.close();
            }
        } catch (Exception e) {
            if (e.getMessage().contains("version")) {
                return false;
            }
            throw new IllegalStateException("redis state failure", e);
        }
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
