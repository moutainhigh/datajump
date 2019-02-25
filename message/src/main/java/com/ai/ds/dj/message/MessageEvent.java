package com.ai.ds.dj.message;

/**
 * Copyright asiainfo.com
 *  处理成功后的事件
 * @author wuwh6
 */
public class MessageEvent {
    public int getEventType() {
        return eventType;
    }

    public void setEventType(int eventType) {
        this.eventType = eventType;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    /**事件类型*/
    private int eventType;
    /**事件数据*/
    private Object data;

    public MessageEvent(int eventType,Object data){
        this.eventType = eventType;
        this.data = data;
    }
}
