package com.ai.ds.dj.redis.log;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Copyright asiainfo.com
 *
 * @author wuwh6
 */
public class LockFileTest {

    LockFile file =new LockFile();
    @Test
    public void isMaster() throws Exception {
        writeMaster();
        assertTrue(file.isMaster());
    }

    @Test
    public void isSencond() throws Exception {
        writeSencond();
        assertTrue(file.isSencond());
    }

    @Test
    public void writeMaster() throws Exception {
        file.writeMaster();
    }

    @Test
    public void writeSencond() throws Exception {
        file.writeSencond();
    }

}