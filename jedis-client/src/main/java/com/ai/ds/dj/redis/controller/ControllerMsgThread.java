package com.ai.ds.dj.redis.controller;

import com.ai.ds.dj.redis.RedisConfig;
import com.ai.ds.dj.redis.RediseAutoProxyCluster;
import redis.clients.jedis.JedisCluster;

/**
 * Copyright asiainfo.com
 *
 * @author wuwh6
 */
public class ControllerMsgThread implements Runnable {

    RediseAutoProxyCluster cluster;
    public ControllerMsgThread(RediseAutoProxyCluster cluster){
        this.cluster = cluster;


    }
    @Override
    public void run() {
        JedisCluster master = cluster.getMaster();
        if(master!=null){
            master.subscribe(new ControllerPubSub(cluster), RedisConfig.CONTROLLER_CHANNEL);

        }
        JedisCluster seconder = cluster.getSeconder();
        if(seconder!=null){
            seconder.subscribe(new ControllerPubSub(cluster), RedisConfig.CONTROLLER_CHANNEL);

        }

    }
}
