package com.ai.ds.dj.connection;

import com.ai.ds.dj.exception.RedisConnectionException;
import com.ai.ds.dj.stream.RedisInputStream;
import com.ai.ds.dj.stream.RedisOutputStream;
import com.ai.ds.dj.util.SafeEncoder;
import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

/**
 * Copyright asiainfo.com
 * 链接redis集群并进行处理
 * @author wuwh6
 */
public class RedisConnection {

    private String master;
    int timeout;
    private Socket masterSocket;
    private String auth;
    private int pipelinedCommands = 0;
    private int port;
    private int soTimeout;

    public RedisInputStream getInputStream() {
        return inputStream;
    }

    public void setInputStream(RedisInputStream inputStream) {
        this.inputStream = inputStream;
    }

    public RedisOutputStream getOutputStream() {
        return outputStream;
    }

    public void setOutputStream(RedisOutputStream outputStream) {
        this.outputStream = outputStream;
    }

    private RedisInputStream inputStream;
    private RedisOutputStream outputStream;
    private boolean broken = false;

    public long getOffset() {
        return offset;
    }

    private long offset;
    public RedisConnection(String masterip,int port,int timeout,String auth,int soTimeout){
        this.master = masterip;
        this.timeout = timeout;
        this.auth = auth;
        this.port = port;
        this.soTimeout = soTimeout;
    }

    /**
     * 链接远程主机
     */
    public   void connection(){


        if(!this.isConnected()){
            try {
                this.masterSocket = new Socket();
                this.masterSocket.setReuseAddress(true);
                this.masterSocket.setKeepAlive(true);
                this.masterSocket.setTcpNoDelay(true);
                this.masterSocket.setSoLinger(true, 0);
                this.masterSocket.connect(new InetSocketAddress(this.master, this.port),86400);
//                this.masterSocket.setSoTimeout(this.soTimeout);
                this.inputStream = new RedisInputStream(this.masterSocket.getInputStream());
                this.outputStream = new RedisOutputStream(this.masterSocket.getOutputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 是否已经关闭
     * @return
     */
    public boolean isConnected() {
        if(this.masterSocket==null) return false;
        return  this.masterSocket.isBound() && !this.masterSocket.isClosed()
                && this.masterSocket.isConnected() && !this.masterSocket.isInputShutdown()
                && !this.masterSocket.isOutputShutdown();
    }

    public void flush(){
        if(this.outputStream!=null){
            try {
                this.outputStream.flush();
            } catch (IOException e) {
                this.broken = true;
                throw new RedisConnectionException(e);


            }
        }
    }

    public String getStatusCodeReply() {
        flush();
        final byte[] resp = (byte[]) readProtocolWithCheckingBroken();
        if (null == resp) {
            return null;
        } else {
            return SafeEncoder.encode(resp);
        }
    }

    public String getBulkReply() {
        final byte[] result = getBinaryBulkReply();
        if (null != result) {
            return SafeEncoder.encode(result);
        } else {
            return null;
        }
    }

    protected Object readProtocolWithCheckingBroken() {
        try {
            return Protocol.read(inputStream);
        } catch (JedisConnectionException exc) {
            broken = true;
            throw exc;
        }
    }

    public byte[] getBinaryBulkReply() {
        flush();
        return (byte[]) readProtocolWithCheckingBroken();
    }

    public Long getIntegerReply() {
        flush();
        return (Long) readProtocolWithCheckingBroken();
    }

    public List<String> getMultiBulkReply() {
        return BuilderFactory.STRING_LIST.build(getBinaryMultiBulkReply());
    }

    @SuppressWarnings("unchecked")
    public List<byte[]> getBinaryMultiBulkReply() {
        flush();
        return (List<byte[]>) readProtocolWithCheckingBroken();
    }

    @SuppressWarnings("unchecked")
    public List<Object> getRawObjectMultiBulkReply() {
        return (List<Object>) readProtocolWithCheckingBroken();
    }

    public List<Object> getObjectMultiBulkReply() {
        flush();
        return getRawObjectMultiBulkReply();
    }

    @SuppressWarnings("unchecked")
    public List<Long> getIntegerMultiBulkReply() {
        flush();
        return (List<Long>) readProtocolWithCheckingBroken();
    }

    public Object getOne() {
        flush();
        return readProtocolWithCheckingBroken();
    }

    public void sendCommand(Protocol.Command cmd, String... args) {
        byte[][] bargs = new byte[args.length][];

        for (int i = 0; i < args.length; ++i) {
            bargs[i] = SafeEncoder.encode(args[i]);
        }

         this.sendCommand(cmd, bargs);
    }

    protected void sendCommand(Protocol.Command cmd, byte[]... args) {
        try {
            this.connection();
            Protocol.sendCommand(this.outputStream, cmd, args);
            ++this.pipelinedCommands;
        } catch (RedisConnectionException var6) {
            RedisConnectionException ex = var6;

            try {
                String errorMessage = Protocol.readErrorLineIfPossible(this.inputStream);
                if (errorMessage != null && errorMessage.length() > 0) {
                    ex = new RedisConnectionException(errorMessage, ex.getCause());
                }
            } catch (Exception var5) {
                ;
            }

            this.broken = true;
            throw ex;
        }
    }

    /**
     * 发送确认
     * @param offset
     * @return
     */
    public RedisConnection replconf(Long offset) {
        this.setOffset(offset);
        this.sendCommand(Protocol.Command.REPLCONF, "ACK", String.valueOf(offset));

        return this;
    }

    public void sendCommand(String command){
        Jedis j;


    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
