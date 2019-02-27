package com.ai.ds.dj.message.consumer;
import com.ai.ds.dj.message.MessageCentre;
import com.ai.ds.dj.message.listener.AbstractEventListenner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * Copyright asiainfo.com
 *
 * @author wuwh6
 */
public abstract  class EventConsumer  extends AbstractEventListenner{

    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    /**
     * 设置名称
     */
    private ThreadFactory factory = new ThreadFactory() {

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("EventConsumer="+t.getId());
            return t;
        }
    };

    private ExecutorService service = new ThreadPoolExecutor(1,1,
            1000, TimeUnit.MINUTES,new ArrayBlockingQueue<Runnable>(2),
            factory);


    private boolean tag =true;
    MessageCentre centre = MessageCentre.getMessageCentre();

    /**
     * 开始启动
     */
    public void start(){
        if(logger.isDebugEnabled()){
            logger.debug("开始启动消息处理线程");
        }


        service.execute(new Runnable() {
            @Override
            public void run() {
                while(tag){
                    try{
                        handle(centre.take());
                    }catch (Exception e){

                    }
                }
            }
        });

        logger.debug("消息处理线程已经启动");


    }
    public void shutdown(){
        this.tag = false;
        service.shutdown();
    }


}
