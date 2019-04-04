package com.ai.ds.dj.redis;

/**
 * Copyright asiainfo.com
 * 切换规则接口
 * @author wuwh6
 */
public interface SwitchRule {

    /**
     * 切换规则 是否需要到被节点
     * @param cluster 集群对象
     * @return 是否切换 是：切换到备机，否不需要切换到备机
     */
    public boolean switchBackUp(RediseAutoProxyCluster cluster);
}
