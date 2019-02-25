package com.ai.ds.dj.server;



import com.ai.ds.dj.config.DJConfig;
import com.ai.ds.dj.config.FromNodeConfig;

import static org.junit.Assert.*;

/**
 * Copyright asiainfo.com
 *
 * @author wuwh6
 */

public class RedisServerTest {

    FromNodeConfig config = new FromNodeConfig();


    @org.junit.Test
    public void beginPSync() throws Exception {
        config.setNodeAddress("192.168.23.147");
        config.setPort(10204);
        config.setTimeout(2000);
        DJConfig dj = new DJConfig();
        dj.setFromNode(config);

        RedisServer server = new RedisServer(dj);
        server.connection();
        server.beginPSync();


    }



}
