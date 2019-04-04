package com.ai.ds.dj.redis;

/**
 * Copyright asiainfo.com
 * 开关说明
 * @author wuwh6
 */
public class StatusSwitchRule implements SwitchRule {
    @Override
    public boolean switchBackUp(RediseAutoProxyCluster cluster) {
        return false;
    }
}
