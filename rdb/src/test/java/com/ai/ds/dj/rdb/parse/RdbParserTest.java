package com.ai.ds.dj.rdb.parse;


import com.ai.ds.dj.rdb.io.RedisInputStream;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.FileInputStream;

import static org.junit.Assert.*;

/**
 * Copyright asiainfo.com
 *
 * @author wuwh6
 */

public class RdbParserTest {

    private DefaultRdbVisitor visitor = new DefaultRdbVisitor();
    @Test
    public void parse() throws Exception {
        RedisInputStream input = new RedisInputStream(new FileInputStream("D:\\tmp\\dump.rdb"));
        RdbParser parse = new RdbParser(input,visitor);
        parse.parse();


    }



}
