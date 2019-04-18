package com.ai.ds.dj.redis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Copyright asiainfo.com
 * 处理redis命令模版
 * @author wuwh6
 */
public  abstract  class JedisCommand <T> {

    private RediseAutoProxyCluster cluster;

    JedisCommand(RediseAutoProxyCluster cluster){
        this.cluster = cluster;
    }

    /**
     * 处理命令行
     * @param cluster 集群
     * @return
     */
    public abstract T execute(JedisCluster cluster);

    public T run(){
        cluster.changeState();
        T result = null;
        JedisCluster master=null;
        //主节点
        if(cluster.isMaster()) {
            master = cluster.getMaster();
        }
        //父节点
        else {
            master = cluster.getSeconder();
        }
        if(master!=null){
            try{
                result =  execute(master);
                cluster.addCount();
            }catch (JedisException e){
                cluster.error();
            }
        }
        return result;
    }




}
