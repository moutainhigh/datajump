package com.ai.ds.dj.server;

import com.ai.ds.dj.config.DJConfig;
import com.ai.ds.dj.config.FromNodeConfig;
import com.ai.ds.dj.connection.Protocol;
import com.ai.ds.dj.connection.RedisConnection;
import com.ai.ds.dj.exception.RedisConnectionException;
import com.ai.ds.dj.message.MessageCentre;
import com.ai.ds.dj.message.consumer.RdbEventConsumer;
import com.ai.ds.dj.rdb.parse.DefaultRdbVisitor;
import com.ai.ds.dj.rdb.parse.RdbParser;

import  com.ai.ds.dj.rdb.io.RedisInputStream;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Copyright asiainfo.com
 * redis数据源
 * @author wuwh6
 */
public class RedisServer {

    public long getOffset() {
        return offset.get();
    }

    public void setOffset(long offset) {
        this.offset.set(offset);
    }

    public String getRunid() {
        return runid;
    }

    public void setRunid(String runid) {
        this.runid = runid;
    }

    private DJConfig djconfig;

    private FromNodeConfig config;

    private RedisConnection connection;
    //同步的长度
    private AtomicLong  offset = new AtomicLong(-1);

    public final static Long DEFAULT_OFFSET = -1L;
    //同步runid
    protected volatile String runid;



    /**
     * 消息中心
     */
    private MessageCentre messageCentre = MessageCentre.getMessageCentre() ;


    public RedisServer(DJConfig djConfig){
        this.config = djConfig.getFromNode();
        this.djconfig = djConfig;

    }

    public void connection(){
        this.connection = new RedisConnection(config.getNodeAddress(),config.getPort(),config.getTimeout(),config.getAuth(),-1);
        connection.connection();
    }
    //获取同步信息与runid
    public void initMaster() {
        this.connection.sendCommand(Protocol.Command.INFO);
        String info = this.connection.getBulkReply();
        String[] infos = info.split("\r\n");
        this.offset.set(DEFAULT_OFFSET);
        this.runid = "?";
        for (String i : infos) {
            if (i.toLowerCase().contains("master_repl_offset")) {
                Long index = Long.valueOf(i.split(":")[1]);
                this.setOffset(index);
            }
            if (i.toLowerCase().contains("run_id")) {
                this.runid = i.split(":")[1];
            }
        }
    }

    public void beginPSync(){
        //获取同步的便宜量和同步的主机的runid
        initMaster();
        System.out.println(String.format("start synch runid=%s,offset=%s",this.runid,offset.get()));
        //发送同步命令
        this.connection.sendCommand(Protocol.Command.PSYNC, this.runid, String.valueOf(offset.get()));
        this.connection.flush();
//
//        Timer pingtimer = new Timer();
//        pingtimer.schedule(new TimerTask() {
//            @Override
//            public void run() {
//                System.out.println("ack");
//                connection.replconf(offset.get());
//            }
//        }, 250, 250);

        String rdb = this.connection.getBulkReply();
//        byte[] rdb = this.connection.getBinaryBulkReply();
       // String info = new String(rdb);
        System.out.println("rdb========="+rdb);
//         rdb = this.connection.getBulkReply();
//        System.out.println("rdb2========="+rdb);
        //全量同步
        if(!"CONTINUE".equals(rdb)){
            byte[] rdbinfo = this.connection.getBinaryBulkReply();
            System.out.println("开始全量同步 同步数据为：");
            System.out.println(new String(rdbinfo));
            ByteArrayInputStream inputStream = new ByteArrayInputStream(rdbinfo);
            DefaultRdbVisitor visitor = new DefaultRdbVisitor();
            RdbEventConsumer consumer = new RdbEventConsumer();
            consumer.start();
            RedisInputStream input = new RedisInputStream(inputStream);
            RdbParser parse = new RdbParser(input,visitor);
            try {
                parse.parse();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        //增量同步
        else {
            try{
                System.out.println("增量同步 ");
                boolean contue=true;
                long count=0;
                while(contue){
                    try{
                        List<byte[]> obj =this.connection.getBinaryMultiBulkReply();
                        System.out.println("命令字："+new String(obj.get(0)));
                        count++;
                    }catch (RedisConnectionException e){
                        contue=false;
                    }

                }
                System.out.println("增量同步结束 共同步新增命令:"+count);



//                System.out.println("增量同步 ");
//                BufferedInputStream bis = new BufferedInputStream(this.connection.getInputStream());
//                List<Integer> list = new ArrayList<Integer>();
//                int b = -1;
//
//                boolean con = true;
//
//                while (con) {
//                    try {
//
//                        int n = 0;
//                        while ((b = bis.read()) != -1) {
//                            list.add(b);
//                            n--;
//                            if (b == 10 && n < 1) {  //判断每行结束
//                                if (list.size() < 2) {
//                                    list.clear();
//                                    continue;
//                                }
//                                byte[] bb = new byte[list.size() - 2];
//                                for (int i = 0; i < list.size() - 2; i++) {
//                                    bb[i] = list.get(i).byteValue();
//                                }
//
//
//                                String s = new String(bb);
//                               // try {
//                                  System.out.println(s);
////                                if("PING".equals(s)){
////                              this.connection.sendCommand(Protocol.Command.PONG);
////                          }
//                                    //this.syncDataQueue.put(new SyncData(s, System.currentTimeMillis()));  //将解析出来aof记录往队列里添加
////                                } catch (InterruptedException e) {
////                                    e.printStackTrace();
////                                } finally {
////                                    System.out.println(list.size());
////                                     //this.incrOffset(list.size());
////                                }
//
//                                if (list.get(0) == 36) {
//                                    //当前行是$开头，则$后面的数字表示参数的字节数
//                                    n = Integer.parseInt(s.replace("\r\n", "").substring(1)) + 2;
//                                }
//                                list.clear();
//                            }
//                            b = -1;
//                        }
//                    } catch (IOException e) {
//                       // e.printStackTrace();
//                        con = false;
//                    }
//                    catch (RedisConnectionException e){
//                        con = false;
//                    }
//                }






//                InputStream input = this.connection.getInputStream();
//                ByteArrayOutputStream out = new ByteArrayOutputStream();
//                int i = input.read();
//                while(i!=-1){
//                    out.write(i);
//                    i = input.read();
//                }
//               byte[] bytes =  out.toByteArray();
//                if(bytes!=null){
//                    System.out.println(">>>>>"+new String(bytes));
//                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }


//        BufferedInputStream bis = new BufferedInputStream(this.connection.getInputStream());
//        List<Integer> list = new ArrayList<Integer>();
//        int b = -1;
//        while (true) {
//            try {
//
//                int n = 0;
//                while ((b = bis.read()) != -1) {
//                    list.add(b);
//                    n--;
//                    if (b == 10 && n < 1) {  //判断每行结束
//                        if (list.size() < 2) {
//                            list.clear();
//                            continue;
//                        }
//                        byte[] bb = new byte[list.size() - 2];
//                        for (int i = 0; i < list.size() - 2; i++) {
//                            bb[i] = list.get(i).byteValue();
//                        }
//
//                        String s = new String(bb);
////                        try {
//                          System.out.println(s);
//                          if("PING".equals(s)){
//                              this.connection.sendCommand(Protocol.Command.PONG);
//                          }
//                            //this.syncDataQueue.put(new SyncData(s, System.currentTimeMillis()));  //将解析出来aof记录往队列里添加
////                        } catch (InterruptedException e) {
////                            e.printStackTrace();
////                        } finally {
//                            this.offset.set(list.size());
////                        }
//
//                        if (list.get(0) == 36) {
//                            //当前行是$开头，则$后面的数字表示参数的字节数
//                            n = Integer.parseInt(s.replace("\r\n", "").substring(1)) + 2;
//                        }
//                        list.clear();
//                    }
//                    b = -1;
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }


    }




}
