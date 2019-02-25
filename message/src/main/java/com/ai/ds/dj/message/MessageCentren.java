package com.ai.ds.dj.message;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Copyright asiainfo.com
 * 消息中心，取消息放消息
 * @author wuwh6
 */
public class MessageCentren {

    private BlockingQueue<MessageEvent> events = new ArrayBlockingQueue<MessageEvent>(1000);

    public void addEvent(MessageEvent event){
        this.events.add(event);
    }

    public MessageEvent get(){
        try {
            return this.events.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
    private MessageCentren(){

    }
    private static MessageCentren centre = new MessageCentren();

    /**
     * 获取消息中心
     * @return
     */
    public static MessageCentren getMessageCentre(){
        return centre;
    }




}
