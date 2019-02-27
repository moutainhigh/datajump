package com.ai.ds.dj.message.listener;

import com.ai.ds.dj.datatype.*;
import com.ai.ds.dj.datatype.Event;

/**
 * Copyright asiainfo.com
 * 事件监听处理抽象类类处理各种事件监听
 * @author wuwh6
 */
public abstract   class AbstractEventListenner implements EventListenner {



    @Override
    public void handle(Event evn) {

        int type = evn.getEventType();

        switch (type) {

            case EVENT_TYPE_STRING:
                this.handleString((KeyStringValueString)evn);
                break;
            case EVENT_TYPE_LIST:
               this.handleList((KeyStringValueList) evn);
                break;
            case EVENT_TYPE_SET:
                this.handleSet((KeyStringValueSet) evn);
                break;
            case EVENT_TYPE_ZSET:
//                event = rdbVisitor.applyZSet(in, version, kv);
                this.handleZset((KeyStringValueZSet) evn);
                break;
            case EVENT_TYPE_ZSET_2:
                this.handleZset((KeyStringValueZSet) evn);
                break;
            case EVENT_TYPE_HASH:
                this.handleHash((KeyStringValueHash) evn);
                break;
            case EVENT_TYPE_HASH_ZIPMAP:
                this.handleHash((KeyStringValueHash) evn);
                break;
            case EVENT_TYPE_LIST_ZIPLIST:
                this.handleList((KeyStringValueList) evn);
                break;
            case EVENT_TYPE_SET_INTSET:
                this.handleZset((KeyStringValueZSet) evn);
                break;
            case EVENT_TYPE_ZSET_ZIPLIST:
                this.handleList((KeyStringValueList) evn);
                break;
            case EVENT_TYPE_HASH_ZIPLIST:
                this.handleHash((KeyStringValueHash) evn);
                break;
            case EVENT_TYPE_LIST_QUICKLIST:
                this.handleList((KeyStringValueList) evn);
                break;
            case EVENT_TYPE_MODULE:
                this.handleModule((KeyStringValueModule) evn);
                break;
            case EVENT_TYPE_MODULE_2:
                this.handleModule((KeyStringValueModule) evn);
                break;
            case EVENT_TYPE_STREAM_LISTPACKS:
                this.handleStream((KeyStringValueStream)evn);
                break;
            default:
                break;
        }



    }

    /**
     * 处理String（key-value)结构
     * @param pa
     */
    public abstract  void handleString(KeyStringValueString pa);

    /**
     * 处理list数据结构
     * @param list
     */
    public abstract void handleList(KeyStringValueList list);

    /**
     * 处理hashMap结构
     * @param hashcode
     */
    public abstract void handleHash(KeyStringValueHash hashcode);

    /**
     * 处理keyString valueset
     * @param sets
     */
    public abstract void handleSet(KeyStringValueSet sets);

    /**
     * 处理zset结构
     * @param zset
     */
    public abstract  void handleZset(KeyStringValueZSet zset);

    /**
     * 处理模块
     * @param module
     */
    public abstract  void handleModule(KeyStringValueModule module);

    /**
     * 处理流对象
     * @param stream
     */
    public abstract  void handleStream(KeyStringValueStream stream);


}
