package com.ai.ds.dj.message.listener;

import com.ai.ds.dj.datatype.Event;

/**
 * Copyright asiainfo.com
 * 同步事件的处理
 * @author wuwh6
 */
public interface EventListenner {

    int EVENT_TYPE_STRING = 0;
    int EVENT_TYPE_LIST = 1;
    int EVENT_TYPE_SET = 2;
    int EVENT_TYPE_ZSET = 3;
    int EVENT_TYPE_HASH = 4;
    int EVENT_TYPE_ZSET_2 = 5;
    int EVENT_TYPE_MODULE = 6;
    int EVENT_TYPE_MODULE_2 = 7;
    int EVENT_TYPE_HASH_ZIPMAP = 9;
    int EVENT_TYPE_LIST_ZIPLIST = 10;
    int EVENT_TYPE_SET_INTSET = 11;
    int EVENT_TYPE_ZSET_ZIPLIST = 12;
    int EVENT_TYPE_HASH_ZIPLIST = 13;
    int EVENT_TYPE_LIST_QUICKLIST = 14;
    int EVENT_TYPE_STREAM_LISTPACKS = 15;

    /**
     * 事件处理
     * @param evn 事件
     */
    void handle(Event evn);

}
