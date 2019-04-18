package com.ai.ds.dj.redis.rule;

import com.ai.ds.dj.redis.RediseAutoProxyCluster;
import com.ai.ds.dj.redis.SwitchRule;
import redis.clients.jedis.JedisPool;

import java.util.Map;

/**
 * Copyright asiainfo.com
 * 开关说明 如果主节点 正常不切换，如果主节点不正常，存节点正常切换到主节点
 * 如果主节点不正常，存节点也不正常时，不切换，集群挂了
 * @author wuwh6
 */
public class StatusSwitchRule implements SwitchRule {
    @Override
    public boolean switchBackUp(RediseAutoProxyCluster cluster) {
        return false;
//        if(cluster.isMasterState()) {
//            return false;
//        }
//        if(!cluster.isMasterState()&&cluster.isBackupState()) {
//            return true;
//        }
//        return false;

    }
}
