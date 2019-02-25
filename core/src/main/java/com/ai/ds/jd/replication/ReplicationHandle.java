package com.ai.ds.jd.replication;

import java.io.InputStream;

/**
 * Copyright asiainfo.com
 *
 * @author wuwh6
 */
public interface  ReplicationHandle {

    /**
     * 处理返回对象
     * @param input
     */
    void doHandle(InputStream input);


}
