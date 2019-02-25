package com.ai.ds.jd.replication;

/**
 * Copyright asiainfo.com
 *
 * @author wuwh6
 */
public class ReplicationHandleFactory {

    private ReplicationHandleFactory(){}

    private static  ReplicationHandleFactory factory = new ReplicationHandleFactory();

    public static ReplicationHandleFactory getInstance(){
        return factory;
    }

    public ReplicationHandle getDefaultReplicationHandle(){
        return new AofReplicationHandle();
    }
}
