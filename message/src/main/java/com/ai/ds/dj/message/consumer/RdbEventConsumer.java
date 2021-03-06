package com.ai.ds.dj.message.consumer;

import com.ai.ds.dj.datatype.*;


/**
 * Copyright asiainfo.com
 *
 * @author wuwh6
 */
public class RdbEventConsumer extends EventConsumer {




    @Override
    public void handleString(KeyStringValueString pa) {
        logger.info(" 同步[String类型]  key={},value={} 到备redis集群中",new String(pa.getKey()),new String(pa.getValue()));
    }

    @Override
    public void handleList(KeyStringValueList list) {

    }

    @Override
    public void handleHash(KeyStringValueHash hashcode) {

    }

    @Override
    public void handleSet(KeyStringValueSet sets) {

    }

    @Override
    public void handleZset(KeyStringValueZSet zset) {

    }

    @Override
    public void handleModule(KeyStringValueModule module) {

    }

    @Override
    public void handleStream(KeyStringValueStream stream) {

    }
}
