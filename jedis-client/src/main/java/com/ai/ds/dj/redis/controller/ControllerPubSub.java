package com.ai.ds.dj.redis.controller;

import com.ai.ds.dj.redis.RediseAutoProxyCluster;
import redis.clients.jedis.JedisPubSub;

/**
 * Copyright asiainfo.com
 *
 * @author wuwh6
 */
public class ControllerPubSub extends JedisPubSub {


    private RediseAutoProxyCluster cluster;
    public ControllerPubSub(RediseAutoProxyCluster cluster){
        this.cluster = cluster;
    }
    @Override
    public void onPMessage(String pattern, String channel, String message) {
        System.out.println("message:"+message);

    }
}
