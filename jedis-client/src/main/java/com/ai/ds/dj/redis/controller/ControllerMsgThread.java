package com.ai.ds.dj.redis.controller;

import com.ai.ds.dj.redis.RedisConfig;
import com.ai.ds.dj.redis.RediseAutoProxyCluster;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisException;

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
            try{
                master.subscribe(new ControllerPubSub(cluster), RedisConfig.CONTROLLER_CHANNEL);

            }catch (JedisException e){
                e.printStackTrace();
            }

        }
        JedisCluster seconder = cluster.getSeconder();
        if(seconder!=null){
            try{
                seconder.subscribe(new ControllerPubSub(cluster), RedisConfig.CONTROLLER_CHANNEL);

            }catch (JedisException e){
                e.printStackTrace();
            }

        }

    }
}
