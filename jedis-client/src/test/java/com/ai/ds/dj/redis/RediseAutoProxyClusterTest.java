package com.ai.ds.dj.redis;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Copyright asiainfo.com
 *
 * @author wuwh6
 */
public class RediseAutoProxyClusterTest {

    public final static String masterNode="192.168.23.148:10201,192.168.23.148:10202,192.168.23.148:10203,192.168.23.148:10204,192.168.23.148:10205,192.168.23.148:10206";

    @Test
    public void testCreate(){
        RediseAutoProxyCluster cluster = new RediseAutoProxyCluster(masterNode,null);
        assertTrue(cluster.CheckClusterState(cluster.getMasterHosts()));



    }

}