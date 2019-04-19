package com.ai.ds.dj.redis.log;

import java.io.*;

/**
 * Copyright asiainfo.com
 * 主要实现保存当前的使用的中心
 * @author wuwh6
 */
public class LockFile {
    public final static String LOCK_FILE="redis.lock";
    public final static String LOGSYS="redis.lockfile";
    private File lockFile ;

    public LockFile(){
        String lockfile = System.getProperty(LOGSYS);
        if(lockfile!=null){
            lockFile = new File(lockfile);
        }
        else {
            lockFile = new File(LOCK_FILE);
        }
    }

    /**
     * 写入当前节点
     * @param node
     */
    private void logNode(char node){
        FileWriter writer =null;
        try {
            writer = new FileWriter(lockFile);
            writer.write(node);
        } catch (IOException e) {
        }
        finally {
            if(writer!=null){
                try {
                    writer.close();
                    writer=null;
                } catch (IOException e) {
                }

            }
        }
    }

    private int  getContent(){
        FileReader reder = null;
        int node=-1;
        try {
            reder= new FileReader(lockFile);
            node = reder.read();
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if(reder!=null){
                try {
                    reder.close();
                    reder=null;
                } catch (IOException e) {

                }

            }
        }
        return node;
    }

    public boolean isMaster(){
      return this.getContent()=='M';
    }
    public boolean isSencond(){
        return this.getContent()=='S';
    }

    public void writeMaster(){
        System.out.println("发送主机切换");

        this.logNode('M');
    }

    public void writeSencond(){
        System.out.println("发送备机切换");
        this.logNode('S');
    }



}
