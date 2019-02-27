package com.ai.ds.dj.message;

import com.ai.ds.dj.datatype.Event;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Copyright asiainfo.com
 * 消息中心，取消息放消息
 * @author wuwh6
 */
public class MessageCentre {

    private BlockingQueue<Event> events = new LinkedBlockingDeque<Event>();

    public void addEvent(Event event){
        this.events.add(event);
    }

    public Event take(){
        try {
            return this.events.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
    private MessageCentre(){

    }
    private static MessageCentre centre = new MessageCentre();

    /**
     * 获取消息中心
     * @return
     */
    public static MessageCentre getMessageCentre(){
        return centre;
    }




}
