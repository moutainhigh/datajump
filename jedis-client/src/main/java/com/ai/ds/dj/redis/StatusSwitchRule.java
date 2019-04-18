package com.ai.ds.dj.redis;

import redis.clients.jedis.JedisPool;

import java.util.Map;

/**
 * Copyright asiainfo.com
 * 开关说明
 * @author wuwh6
 */
public class StatusSwitchRule implements SwitchRule {
    @Override
    public boolean switchBackUp(RediseAutoProxyCluster cluster) {
        Map<String,JedisPool> pools =  cluster.getClusterNodes();
        return false;
    }
}
