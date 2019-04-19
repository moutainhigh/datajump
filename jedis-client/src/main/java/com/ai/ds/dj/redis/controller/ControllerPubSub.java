package com.ai.ds.dj.redis.controller;

import com.ai.ds.dj.redis.RedisConfig;
import com.ai.ds.dj.redis.RediseAutoProxyCluster;
import redis.clients.jedis.JedisPubSub;

/**
 * Copyright asiainfo.com
 * 处理控制消息（消息格式为(M:S)
 * M:S标示将链接切换到备中心
 * S:M将练练由备中心切换到主中心
 * @author wuwh6
 */
public class ControllerPubSub extends JedisPubSub {


    private RediseAutoProxyCluster cluster;



    public ControllerPubSub(RediseAutoProxyCluster cluster){
        this.cluster = cluster;
    }



    @Override
    public void onMessage(String channel, String message) {
        //从控制频道过来的信息时
        if(RedisConfig.CONTROLLER_CHANNEL.equals(channel)){

        }
        //System.out.println("message:"+channel+" :"+message);
    }


}
