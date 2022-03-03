package com.aliware.tianchi.leader;

import com.aliware.tianchi.common.*;
import com.aliyun.tair.tairhash.TairHash;
import com.aliyun.tair.tairhash.TairHashPipeline;
import com.aliyun.tair.tairhash.params.ExhsetParams;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Viber
 * @version 1.0
 * @apiNote 主节点选择器  适用于集群节点较少, 也不想引入其他额外的中间件 -- 这里实现的简单了点
 * -----------------最好能够有一个能够及时通知的中间件来管理
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
    private int sessionTimeout = 20;
    //校验节点的是否存在,单位秒
    private int ticketTimeout = 10;

    private Integer nodeId;
    private volatile boolean master = false;
    //随便些的监听
    private List<LeaderListener> listeners = new CopyOnWriteArrayList<>();
    private AtomicInteger nodes = new AtomicInteger(0);

    public LeaderSelector(JedisPool jedisPool, String name) {
        this.jedisPool = jedisPool;
        this.key = KEY_PREFIX + name;
        this.pubsubChannel = PUBSUB_PREFIX + name;
    }

    public boolean isMaster() {
        return master;
    }

    @Override
    protected void doStart() {
        //监听pub/sub消息
        this.pubSub = new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                //节点变化的通知: 添加节点不关心
                String[] split = message.split("@");
                if ("REMOVE".equals(split[0])) {
                    for (LeaderListener listener : listeners) {
                        GlobalExecutor.singleExecutor().execute(listener::onRemoveNode);
                    }
                }
            }
        };
        CommonUtil.pubsubThread(() -> {
            TairUtil.poolExecute(jedisPool, jedis -> {
                jedis.subscribe(pubSub, pubsubChannel);
                return null;
            });
        }).start();
        while (!pubSub.isSubscribed()) {
            CommonUtil.sleep(0);
        }
        //注册节点的信息
        registerNode();
        //这里一开始判断是否有主节点在, 并尝试注册主节点
        registerLeader();
    }

    @Override
    protected void doStop() {
        pubSub.unsubscribe(pubsubChannel);
        if (master) {
            //删除节点信息
            TairUtil.poolExecute(jedisPool, jedis -> {
                TairHashPipeline tairHashPipeline = new TairHashPipeline();
                tairHashPipeline.setClient(jedis.getClient());
                tairHashPipeline.exhdel(key, Constants.NODE_LEADER);
                tairHashPipeline.exhdel(key, String.valueOf(nodeId));
                tairHashPipeline.sync();
                return null;
            });
        } else {
            TairUtil.poolExecute(jedisPool, jedis -> {
                TairHash tairHash = new TairHash(jedis);
                return tairHash.exhdel(key, String.valueOf(nodeId));
            });
        }
        TairUtil.poolExecute(jedisPool, jedis -> jedis.publish(pubsubChannel, "REMOV@" + nodeId));
    }

    /**
     * 尝试注册master的信息
     */
    private void registerLeader() {
        if (isStart()) {
            startRegisterLeader();
            if (master) {
                //每隔一段时间检测一次, 主要是非正常关闭的检测, 因为后面有校验, 这里就简单点, 判断数量是否有变化
                Long len = TairUtil.poolExecute(jedisPool, jedis -> {
                    TairHash tairHash = new TairHash(jedis);
                    return tairHash.exhlen(key);
                });
                if (len != null && len > 0) {
                    if (len < nodes.get()) {
                        for (LeaderListener listener : listeners) {
                            GlobalExecutor.singleExecutor().execute(listener::onRemoveNode);
                        }
                    }
                    nodes.set(len.intValue());
                }
            }
            GlobalExecutor.schedule().schedule(this::registerLeader, ticketTimeout, TimeUnit.SECONDS);
        }
    }

    /**
     * 争抢master或延续master的超时时间
     */
    private void startRegisterLeader() {
        String value = TairUtil.poolExecute(jedisPool, jedis -> {
            TairHash tairHash = new TairHash(jedis);
            return tairHash.exhget(key, Constants.NODE_LEADER);
        });
        if (!master) {
            if (null == value) { //说明暂时没有leader注册
                ExhsetParams params = new ExhsetParams();
                params.nx().ex(sessionTimeout);
                Long exhset = TairUtil.poolExecute(jedisPool, jedis -> {
                    TairHash tairHash = new TairHash(jedis);
                    return tairHash.exhset(key, Constants.NODE_LEADER, String.valueOf(nodeId), params);
                });
                if (null != exhset && exhset == 1) { //设置成功
                    master = true;
                    for (LeaderListener listener : listeners) {
                        GlobalExecutor.singleExecutor().execute(listener::onBecomeLeader);
                    }
                }
            }
        } else {
            //延续时间 ,担心因为网络或系统原因, 加时操作被延长而节点变更
            if (null == value || !value.equals(String.valueOf(nodeId))) {
                master = false;
                startRegisterLeader();
                return;
            }
            TairUtil.poolExecute(jedisPool, jedis -> {
                TairHash tairHash = new TairHash(jedis);
                return tairHash.exhexpire(key, Constants.NODE_LEADER, sessionTimeout);
            });
        }
    }

    /**
     * 注册节点信息
     */
    private void registerNode() {
        int newNodeId = null == nodeId ? ThreadLocalRandom.current().nextInt(1024) : nodeId;
        do {
            newNodeId++;//简单点
            String field = String.valueOf(newNodeId);
            ExhsetParams params = new ExhsetParams();
            params.nx().ex(sessionTimeout);

            Long exhset = TairUtil.poolExecute(jedisPool, jedis -> {
                TairHash tairHash = new TairHash(jedis);
                return tairHash.exhset(key, field, String.valueOf(System.currentTimeMillis()), params);
            });
            if (null != exhset && exhset >= 0) {
                System.out.println("success register node:" + newNodeId);
                nodeId = newNodeId;
                TairUtil.poolExecute(jedisPool, jedis -> jedis.publish(pubsubChannel, "ADD@" + nodeId));
                break;
            }
        } while (true);
        ticketKeepalive();
    }

    /**
     * 开启定时任务, 不断的维持ttl
     */
    private void ticketKeepalive() {
        if (isStart()) {
            TairUtil.poolExecute(jedisPool, jedis -> {
                TairHash tairHash = new TairHash(jedisPool.getResource());
                return tairHash.exhexpire(key, String.valueOf(nodeId), sessionTimeout);
            });
            GlobalExecutor.schedule().schedule(this::ticketKeepalive, ticketTimeout, TimeUnit.SECONDS);
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
