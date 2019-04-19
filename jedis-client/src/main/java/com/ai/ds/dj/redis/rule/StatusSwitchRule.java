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

        System.out.println("当前模式为[true标示主机房中，false标示备机房中]："+cluster.isMaster());
        //主集群状态可用，不用切换到备集群
        if(cluster.isMasterState()) {
            return false;
        }
        //主集群状态不可用，被集群可用，切换到被集群
        if(!cluster.isMasterState()&&cluster.isBackupState()) {
            return true;
        }

        return false;

    }
}
