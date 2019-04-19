package com.ai.ds.dj.redis;

import org.junit.Test;
import redis.clients.jedis.JedisCluster;

import static org.junit.Assert.*;

/**
 * Copyright asiainfo.com
 *
 * @author wuwh6
 */
public class RediseAutoProxyClusterTest {

    public final static String masterNode="192.168.23.148:10201,192.168.23.148:10202,192.168.23.148:10203,192.168.23.148:10204,192.168.23.148:10205,192.168.23.148:10206";

    public final static String secNode="192.168.23.148:30201,192.168.23.148:30202,192.168.23.148:30203,192.168.23.148:30204,192.168.23.148:30205,192.168.23.148:30206";

    @Test
    public void testCreate(){
        RediseAutoProxyCluster cluster = new RediseAutoProxyCluster(masterNode,null);
        assertTrue(cluster.CheckClusterState(cluster.getMasterHosts()));

    }

    @Test
    public void testSet(){
        JedisCluster cluster = new RediseAutoProxyCluster(masterNode,secNode);

        for (int i = 0; i < 100; i++) {
            cluster.set("123","12378");
            System.out.println(cluster.get("123"));
            try {
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        assertEquals(cluster.get("123"),"12378");




    }

    @Test
    public void testSub(){
        RediseAutoProxyCluster cluster = new RediseAutoProxyCluster(masterNode,null);
        for (int i = 0; i <1000 ; i++) {
            cluster.publish(RedisConfig.CONTROLLER_CHANNEL,"message:"+i);
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("发送"+i+"次");

        }




    }

}