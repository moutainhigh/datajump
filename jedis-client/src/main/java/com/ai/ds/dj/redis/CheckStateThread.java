package com.ai.ds.dj.redis;

/**
 * Copyright asiainfo.com
 * 检查集群是否可用
 * @author wuwh6
 */
public class CheckStateThread implements Runnable {

    private RediseAutoProxyCluster cluster ;
    private boolean tag = true;
    CheckStateThread(RediseAutoProxyCluster cluster){
       this.cluster = cluster;
    }
    @Override
    public void run() {
        while(tag){
           boolean master = cluster.CheckClusterState(cluster.getMasterHosts());
           System.out.println("当前master状态为："+master);
           cluster.setMasterState(master);
           boolean slaver = cluster.CheckClusterState(cluster.getSecondHosts());
           cluster.setBackupState(slaver);
           System.out.println("当前备集群状态为："+slaver);

            try {
                Thread.sleep(cluster.getCheckInterval());
            } catch (InterruptedException e) {

            }


        }

    }

    public void shutdown(){
        this.tag = false;
    }
}
