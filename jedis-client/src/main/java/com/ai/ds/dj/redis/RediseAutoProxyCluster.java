package com.ai.ds.dj.redis;

import com.ai.ds.dj.connection.*;
import com.ai.ds.dj.redis.controller.ControllerMsgThread;
import com.ai.ds.dj.redis.log.LockFile;
import com.ai.ds.dj.redis.rule.StatusSwitchRule;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.params.set.SetParams;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.jedis.params.sortedset.ZIncrByParams;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Copyright asiainfo.com
 * redis自动代理客户端，实现客户端的自动切换
 *
 * @author wuwh6
 */
public class RediseAutoProxyCluster extends JedisCluster {

    private JedisCluster seconder;
    private  JedisCluster master;
    /**
     * 记录当前使用节点的lock文件
     */
    private LockFile lock = new LockFile();

    /**
     * 检查集群是否可用的周期时间（毫秒级别）
     */
    private long checkInterval=20*1000L;
    /**
     * 主中心的地址
     */
    private String masterAddress;
    /**
     * 备中心的地址
     */
    private String secondAddress;

    /**主节点host信息**/
    private Set<HostAndPort> masterHosts;
    /**被节点host信息**/
    private Set<HostAndPort> secondHosts;
    /**
     * 切换规则
     **/
    private SwitchRule rules = new StatusSwitchRule() ;
    /**
     * 是否当前可用的链接是主集群
     */
    private volatile  boolean isMaster=true;
    /**备集群的状态**/
    private volatile  boolean backupState=true;
    /**主集群的状态**/
    private volatile  boolean masterState=true;
    /**手动与自动标示，如果false标示是手动模式下，如果true标示是自动模式下**/
    private volatile  boolean isAuto=true;

    /**
     * 统计调用量
     */
    private AtomicLong  execAllcount = new AtomicLong(0);
    /**
     * 统计失败调用量
     */
    private AtomicLong execErrorcount = new AtomicLong(0);
    /**
     * 链接池设置
     */
    private GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    /**
     * 链接超时时间
     */
    private  int timeout = 2000;
    /**
     * 默认尝试次数
     */
    protected static final int DEFAULT_MAX_ATTEMPTS = 5;


    public RediseAutoProxyCluster() {
        super(Collections.emptySet());
        super.close();

    }
    public RediseAutoProxyCluster(String maserNode,String sencodeNode){
        this();
        this.masterAddress = maserNode;
        this.secondAddress= sencodeNode;
        init();
    }

    public RediseAutoProxyCluster(String maserNode, String sencodeNode, GenericObjectPoolConfig config){
        this();
        this.masterAddress = maserNode;
        this.secondAddress= sencodeNode;
        if(config!=null){
            this.config = config;
        }
        init();
    }

    public RediseAutoProxyCluster(String maserNode, String sencodeNode,int timeout, GenericObjectPoolConfig config){
        this();
        this.masterAddress = maserNode;
        this.secondAddress= sencodeNode;
        if(config!=null){
            this.config = config;
        }
        if(timeout!=-1){
            this.timeout = timeout;
        }

        init();
    }

    /**
     *
     * @param sets
     * @param address
     */
    private void initSet(Set<HostAndPort> sets,String address){
        if(address!=null){
            String[] str = address.split(",");
            for(String addport:str){
                String[] add_port = addport.split(":");
                if(add_port.length==2){
                    HostAndPort port = new HostAndPort(add_port[0],Integer.parseInt(add_port[1]));
                    sets.add(port);
                }
            }
        }

    }

    /**
     * 启动集群状态检查
     */
    private void startCheckState(){
        Thread task = new Thread(new CheckStateThread(this));
        task.setName("redis-check-thread");
        task.start();
    }

    /**
     * 控制字监听
     */
    private void startControllerThread(){
        //启动控制字监听
        Thread task = new Thread(new ControllerMsgThread(this));
        task.setName("redis-controller-Msg");
        task.start();
    }

    public void init(){
        this.masterHosts  =new HashSet<>();
        this. secondHosts  =new HashSet<>() ;
        initSet(masterHosts,this.masterAddress);
        initSet(secondHosts,this.secondAddress);
        if(!masterHosts.isEmpty()){
            this.master = new JedisCluster(masterHosts,this.timeout,this.config);

        }
        if(!secondHosts.isEmpty()){
            this.seconder = new JedisCluster(secondHosts,this.timeout,this.config);

        }
        //开启检查状态的线程
        startCheckState();
        //控制消息监听器
        startControllerThread();
        //使用主集群
        this.isMaster = true;
        lock.writeMaster();

    }

    /**
     * 切换手动模式主模式先
     *
     */
    public void swtichMaster(){
        this.isAuto = false;
        this.isMaster = true;
        this.lock.writeMaster();
    }

    /**
     * 切换手动模式下的备集群
     */
    public void swtichSencond(){
        this.isAuto = false;
        this.isMaster = false;
        this.lock.writeSencond();
    }

    /**
     * 处理失败
     */
    public  void error(){
        execErrorcount.addAndGet(1);
    }

    /**
     * 处理成功
     */
    public void addCount(){
        this.execAllcount.addAndGet(1);
    }


    /**
     * 自动手动切换主备切换：
     */
    public synchronized  void changeState(){

        //自动模式下通过规则去切换
        if(this.isAuto){
            if(this.rules!=null) {
                boolean isbackup = rules.switchBackUp(this);
                //如果到达主备切换时，需要切换为备机器时
                if (isbackup) {
                    //当前是主时，切换到被
                    if(this.isMaster){
                        this.isMaster = false;
                        this.lock.writeSencond();
                    }
                }
                //为到达主备切换
                else {
                    if(!this.isMaster){
                        this.isMaster = true;
                        this.lock.writeMaster();
                    }
                }
            }
        }

    }

    public boolean isMaster(){
        //如果是主节点
       return isMaster;
    }



    @Override
    public String set(String key, String value) {
        changeState();
        String result = null;
        if(this.isMaster) {
            try{
                if(this.master!=null){
                    result =  master.set(key, value);
                }
            }catch (Exception e){
                //失败调用
                this.execErrorcount.getAndIncrement();
            }
        }
        else {
            if(this.seconder!=null){
                result =   this.seconder.set(key,value);
            }
        }
        return result;
    }




    @Override
    public String set(String key, String value, SetParams params) {

        changeState();
        String result = null;
        if(this.isMaster) {
            try{
                if(this.master!=null){
                    result =  master.set(key, value, params);
                }
            }catch (Exception e){
                //失败调用
                this.execErrorcount.getAndIncrement();
            }
        }
        else {
            if(this.seconder!=null){
                result =   this.seconder.set(key, value, params);
            }
        }
        return result;
    }

    @Override
    public String get(String key) {
         return new JedisCommand<String>(this) {
             @Override
             public String execute(JedisCluster cluster) {
                 return cluster.get(key);
             }
         }.run();

    }

    @Override
    public Boolean exists(String key) {
        return new JedisCommand<Boolean>(this) {
            @Override
            public Boolean execute(JedisCluster cluster) {
                return cluster.exists(key);
            }
        }.run();
    }

    @Override
    public Long exists(String... keys) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.exists(keys);
            }
        }.run();
    }

    @Override
    public Long persist(String key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.persist(key);
            }
        }.run();
    }

    @Override
    public String type(String key) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.type(key);
            }
        }.run();
    }

    @Override
    public Long expire(String key, int seconds) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.expire(key,seconds);
            }
        }.run();
    }

    @Override
    public Long pexpire(String key, long milliseconds) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.pexpire(key,milliseconds);
            }
        }.run();
    }

    @Override
    public Long expireAt(String key, long unixTime) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.expireAt(key,unixTime);
            }
        }.run();
    }

    @Override
    public Long pexpireAt(String key, long millisecondsTimestamp) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.expireAt(key,millisecondsTimestamp);
            }
        }.run();
    }

    @Override
    public Long ttl(String key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.ttl(key);
            }
        }.run();
    }

    @Override
    public Boolean setbit(String key, long offset, boolean value) {
        return new JedisCommand<Boolean>(this) {
            @Override
            public Boolean execute(JedisCluster cluster) {
                return cluster.setbit(key,offset,value);
            }
        }.run();
    }

    @Override
    public Boolean setbit(String key, long offset, String value) {
        return new JedisCommand<Boolean>(this) {
            @Override
            public Boolean execute(JedisCluster cluster) {
                return cluster.setbit(key,offset,value);
            }
        }.run();
    }

    @Override
    public Boolean getbit(String key, long offset) {
        return new JedisCommand<Boolean>(this) {
            @Override
            public Boolean execute(JedisCluster cluster) {
                return cluster.getbit(key,offset);
            }
        }.run();
    }

    @Override
    public Long setrange(String key, long offset, String value) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.setrange(key,offset,value);
            }
        }.run();
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.getrange(key,startOffset,endOffset);
            }
        }.run();
    }

    @Override
    public String getSet(String key, String value) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.getSet(key,value);
            }
        }.run();
    }

    @Override
    public Long setnx(String key, String value) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.setnx(key,value);
            }
        }.run();
    }

    @Override
    public String setex(String key, int seconds, String value) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.setex(key,seconds,value);
            }
        }.run();
    }

    @Override
    public Long decrBy(String key, long integer) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.decrBy(key,integer);
            }
        }.run();
    }

    @Override
    public Long decr(String key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.decr(key);
            }
        }.run();
    }

    @Override
    public Long incrBy(String key, long integer) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.incrBy(key,integer);
            }
        }.run();
    }

    @Override
    public Double incrByFloat(String key, double value) {
        return new JedisCommand<Double>(this) {
            @Override
            public Double execute(JedisCluster cluster) {
                return cluster.incrByFloat(key,value);
            }
        }.run();
    }

    @Override
    public Long incr(String key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.incr(key);
            }
        }.run();
    }

    @Override
    public Long append(String key, String value) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.append(key,value);
            }
        }.run();
    }

    @Override
    public String substr(String key, int start, int end) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.substr(key,start,end);
            }
        }.run();
    }

    @Override
    public Long hset(String key, String field, String value) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.hset(key,field,value);
            }
        }.run();
    }

    @Override
    public String hget(String key, String field) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.hget(key,field);
            }
        }.run();
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.hsetnx(key,field,value);
            }
        }.run();
    }

    @Override
    public String hmset(String key, Map<String, String> hash) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.hmset(key,hash);
            }
        }.run();
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        return new JedisCommand<List<String>>(this) {
            @Override
            public List<String> execute(JedisCluster cluster) {
                return cluster.hmget(key,fields);
            }
        }.run();
    }

    @Override
    public Long hincrBy(String key, String field, long value) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.hincrBy(key,field,value);
            }
        }.run();
    }

    @Override
    public Boolean hexists(String key, String field) {
        return new JedisCommand<Boolean>(this) {
            @Override
            public Boolean execute(JedisCluster cluster) {
                return cluster.hexists(key,field);
            }
        }.run();
    }

    @Override
    public Long hdel(String key, String... field) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.hdel(key,field);
            }
        }.run();
    }

    @Override
    public Long hlen(String key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.hlen(key);
            }
        }.run();
    }

    @Override
    public Set<String> hkeys(String key) {
        return new JedisCommand<Set<String>>(this) {
            @Override
            public Set<String> execute(JedisCluster cluster) {
                return cluster.hkeys(key);
            }
        }.run();
    }

    @Override
    public List<String> hvals(String key) {
        return new JedisCommand<List<String>>(this) {
            @Override
            public List<String> execute(JedisCluster cluster) {
                return cluster.hvals(key);
            }
        }.run();
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return new JedisCommand<Map<String, String>>(this) {
            @Override
            public Map<String, String> execute(JedisCluster cluster) {
                return cluster.hgetAll(key);
            }
        }.run();
    }

    @Override
    public Long rpush(String key, String... string) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.rpush(key,string);
            }
        }.run();
    }

    @Override
    public Long lpush(String key, String... string) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.lpush(key,string);
            }
        }.run();
    }

    @Override
    public Long llen(String key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.llen(key);
            }
        }.run();
    }

    @Override
    public List<String> lrange(String key, long start, long end) {
        return new JedisCommand<List<String>>(this) {
            @Override
            public List<String> execute(JedisCluster cluster) {
                return cluster.lrange(key,start,end);
            }
        }.run();
    }

    @Override
    public String ltrim(String key, long start, long end) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.ltrim(key,start,end);
            }
        }.run();
    }

    @Override
    public String lindex(String key, long index) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.lindex(key,index);
            }
        }.run();
    }

    @Override
    public String lset(String key, long index, String value) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.lset(key,index,value);
            }
        }.run();
    }

    @Override
    public Long lrem(String key, long count, String value) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.lrem(key,count,value);
            }
        }.run();
    }

    @Override
    public String lpop(String key) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.lpop(key);
            }
        }.run();
    }

    @Override
    public String rpop(String key) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.rpop(key);
            }
        }.run();
    }

    @Override
    public Long sadd(String key, String... member) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.sadd(key,member);
            }
        }.run();
    }

    @Override
    public Set<String> smembers(String key) {
        return new JedisCommand<Set<String>>(this) {
            @Override
            public Set<String> execute(JedisCluster cluster) {
                return cluster.smembers(key);
            }
        }.run();
    }

    @Override
    public Long srem(String key, String... member) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.srem(key,member);
            }
        }.run();
    }

    @Override
    public String spop(String key) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.spop(key);
            }
        }.run();
    }

    @Override
    public Set<String> spop(String key, long count) {
        return new JedisCommand<Set<String>>(this) {
            @Override
            public Set<String> execute(JedisCluster cluster) {
                return cluster.spop(key,count);
            }
        }.run();
    }

    @Override
    public Long scard(String key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.scard(key);
            }
        }.run();
    }

    @Override
    public Boolean sismember(String key, String member) {
        return new JedisCommand<Boolean>(this) {
            @Override
            public Boolean execute(JedisCluster cluster) {
                return cluster.sismember(key,member);
            }
        }.run();
    }

    @Override
    public String srandmember(String key) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.srandmember(key);
            }
        }.run();
    }

    @Override
    public List<String> srandmember(String key, int count) {
        return new JedisCommand<List<String>>(this) {
            @Override
            public List<String> execute(JedisCluster cluster) {
                return cluster.srandmember(key,count);
            }
        }.run();
    }

    @Override
    public Long strlen(String key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.strlen(key);
            }
        }.run();
    }

    @Override
    public Long zadd(String key, double score, String member) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zadd(key,score,member);
            }
        }.run();
    }

    @Override
    public Long zadd(String key, double score, String member, ZAddParams params) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zadd(key,score,member,params);
            }
        }.run();
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zadd(key,scoreMembers);
            }
        }.run();
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers, ZAddParams params) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zadd(key,scoreMembers,params);
            }
        }.run();
    }

    @Override
    public Set<String> zrange(String key, long start, long end) {
        return new JedisCommand<Set<String>>(this) {
            @Override
            public Set<String> execute(JedisCluster cluster) {
                return cluster.zrange(key,start,end);
            }
        }.run();
    }

    @Override
    public Long zrem(String key, String... member) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zrem(key,member);
            }
        }.run();
    }

    @Override
    public Double zincrby(String key, double score, String member) {
        return new JedisCommand<Double>(this) {
            @Override
            public Double execute(JedisCluster cluster) {
                return cluster.zincrby(key,score,member);
            }
        }.run();
    }

    @Override
    public Double zincrby(String key, double score, String member, ZIncrByParams params) {
        return new JedisCommand<Double>(this) {
            @Override
            public Double execute(JedisCluster cluster) {
                return cluster.zincrby(key,score,member,params);
            }
        }.run();
    }

    @Override
    public Long zrank(String key, String member) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zrank(key,member);
            }
        }.run();
    }

    @Override
    public Long zrevrank(String key, String member) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zrevrank(key,member);
            }
        }.run();
    }

    @Override
    public Set<String> zrevrange(String key, long start, long end) {
        return new JedisCommand<Set<String>>(this) {
            @Override
            public Set<String> execute(JedisCluster cluster) {
                return cluster.zrevrange(key,start,end);
            }
        }.run();
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        return new JedisCommand<Set<Tuple>>(this) {
            @Override
            public Set<Tuple> execute(JedisCluster cluster) {
                return cluster.zrangeWithScores(key,start,end);
            }
        }.run();
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        return new JedisCommand<Set<Tuple>>(this) {
            @Override
            public Set<Tuple> execute(JedisCluster cluster) {
                return cluster.zrangeWithScores(key,start,end);
            }
        }.run();
    }

    @Override
    public Long zcard(String key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zcard(key);
            }
        }.run();
    }

    @Override
    public Double zscore(String key, String member) {
        return new JedisCommand<Double>(this) {
            @Override
            public Double execute(JedisCluster cluster) {
                return cluster.zscore(key,member);
            }
        }.run();
    }

    @Override
    public List<String> sort(String key) {
        return new JedisCommand<List<String>>(this) {
            @Override
            public List<String> execute(JedisCluster cluster) {
                return cluster.sort(key);
            }
        }.run();
    }

    @Override
    public List<String> sort(String key, SortingParams sortingParameters) {
        return new JedisCommand<List<String>>(this) {
            @Override
            public List<String> execute(JedisCluster cluster) {
                return cluster.sort(key,sortingParameters);
            }
        }.run();
    }

    @Override
    public Long zcount(String key, double min, double max) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zcount(key,min,max);
            }
        }.run();
    }

    @Override
    public Long zcount(String key, String min, String max) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zcount(key,min,max);
            }
        }.run();
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) {
        return new JedisCommand<Set<String> >(this) {
            @Override
            public Set<String>  execute(JedisCluster cluster) {
                return cluster.zrangeByScore(key,min,max);
            }
        }.run();
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) {
        return new JedisCommand<Set<String> >(this) {
            @Override
            public Set<String>  execute(JedisCluster cluster) {
                return cluster.zrangeByScore(key,min,max);
            }
        }.run();
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min) {
        return new JedisCommand<Set<String> >(this) {
            @Override
            public Set<String>  execute(JedisCluster cluster) {
                return cluster.zrevrangeByScore(key,min,max);
            }
        }.run();
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        return new JedisCommand<Set<String> >(this) {
            @Override
            public Set<String>  execute(JedisCluster cluster) {
                return cluster.zrangeByScore(key,min,max,offset,count);
            }
        }.run();
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min) {
        return new JedisCommand<Set<String> >(this) {
            @Override
            public Set<String>  execute(JedisCluster cluster) {
                return cluster.zrevrangeByScore(key,max,min);
            }
        }.run();
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
        return new JedisCommand<Set<String> >(this) {
            @Override
            public Set<String>  execute(JedisCluster cluster) {
                return cluster.zrangeByScore(key,min,max,offset,count);
            }
        }.run();
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return new JedisCommand<Set<String> >(this) {
            @Override
            public Set<String>  execute(JedisCluster cluster) {
                return cluster.zrevrangeByScore(key,max,min,offset,count);
            }
        }.run();
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        return new JedisCommand<Set<Tuple> >(this) {
            @Override
            public Set<Tuple>  execute(JedisCluster cluster) {
                return cluster.zrangeByScoreWithScores(key,min,max);
            }
        }.run();
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        return new JedisCommand<Set<Tuple> >(this) {
            @Override
            public Set<Tuple>  execute(JedisCluster cluster) {
                return cluster.zrevrangeByScoreWithScores(key,min,max);
            }
        }.run();
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return new JedisCommand<Set<Tuple> >(this) {
            @Override
            public Set<Tuple>  execute(JedisCluster cluster) {
                return cluster.zrangeByScoreWithScores(key,min,max,offset,count);
            }
        }.run();
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        return new JedisCommand<Set<String> >(this) {
            @Override
            public Set<String>  execute(JedisCluster cluster) {
                return cluster.zrevrangeByScore(key,max,min,offset,count);
            }
        }.run();
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        return new JedisCommand<Set<Tuple> >(this) {
            @Override
            public Set<Tuple>  execute(JedisCluster cluster) {
                return cluster.zrangeByScoreWithScores(key,min,max);
            }
        }.run();
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        return new JedisCommand<Set<Tuple> >(this) {
            @Override
            public Set<Tuple>  execute(JedisCluster cluster) {
                return cluster.zrevrangeByScoreWithScores(key,max,min);
            }
        }.run();
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        return new JedisCommand<Set<Tuple> >(this) {
            @Override
            public Set<Tuple>  execute(JedisCluster cluster) {
                return cluster.zrangeByScoreWithScores(key,min,max,offset,count);
            }
        }.run();
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return new JedisCommand<Set<Tuple> >(this) {
            @Override
            public Set<Tuple>  execute(JedisCluster cluster) {
                return cluster.zrevrangeByScoreWithScores(key,max,min,offset,count);
            }
        }.run();
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        return new JedisCommand<Set<Tuple> >(this) {
            @Override
            public Set<Tuple>  execute(JedisCluster cluster) {
                return cluster.zrevrangeByScoreWithScores(key,max,min,offset,count);
            }
        }.run();
    }

    @Override
    public Long zremrangeByRank(String key, long start, long end) {
        return new JedisCommand<Long >(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.zremrangeByRank(key,start,end);
            }
        }.run();
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) {
        return new JedisCommand<Long >(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.zremrangeByScore(key,start,end);
            }
        }.run();
    }

    @Override
    public Long zremrangeByScore(String key, String start, String end) {
        return new JedisCommand<Long >(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.zremrangeByScore(key,start,end);
            }
        }.run();
    }

    @Override
    public Long zlexcount(String key, String min, String max) {
        return new JedisCommand<Long >(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.zlexcount(key,min,max);
            }
        }.run();
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max) {
        return new JedisCommand<Set<String> >(this) {
            @Override
            public Set<String>  execute(JedisCluster cluster) {
                return cluster.zrangeByLex(key,min,max);
            }
        }.run();
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
        return new JedisCommand<Set<String> >(this) {
            @Override
            public Set<String>  execute(JedisCluster cluster) {
                return cluster.zrangeByLex(key,min,max,offset,count);
            }
        }.run();
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min) {
        return new JedisCommand< Set<String> >(this) {
            @Override
            public Set<String>  execute(JedisCluster cluster) {
                return cluster.zrevrangeByLex(key,max,min);
            }
        }.run();
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
        return new JedisCommand< Set<String> >(this) {
            @Override
            public Set<String>  execute(JedisCluster cluster) {
                return cluster.zrevrangeByLex(key,max,min,offset,count);
            }
        }.run();
    }

    @Override
    public Long zremrangeByLex(String key, String min, String max) {
        return new JedisCommand< Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.zremrangeByLex(key,min,max);
            }
        }.run();
    }

    @Override
    public Long linsert(String key, BinaryClient.LIST_POSITION where, String pivot, String value) {
        return new JedisCommand< Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.linsert(key,where,pivot,value);
            }
        }.run();
    }

    @Override
    public Long lpushx(String key, String... string) {
        return new JedisCommand< Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.lpushx(key,string);
            }
        }.run();
    }

    @Override
    public Long rpushx(String key, String... string) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.rpushx(key,string);
            }
        }.run();
    }

    @Override
    public Long del(String key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.del(key);
            }
        }.run();
    }

    @Override
    public String echo(String string) {
        return new JedisCommand<String>(this) {
            @Override
            public String  execute(JedisCluster cluster) {
                return cluster.echo(string);
            }
        }.run();
    }

    @Override
    public Long bitcount(String key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.bitcount(key);
            }
        }.run();
    }

    @Override
    public Long bitcount(String key, long start, long end) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.bitcount(key,start,end);
            }
        }.run();
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor) {
        return new JedisCommand<ScanResult<Map.Entry<String, String>>>(this) {
            @Override
            public ScanResult<Map.Entry<String, String>>  execute(JedisCluster cluster) {
                return cluster.hscan(key,cursor);
            }
        }.run();
    }

    @Override
    public ScanResult<String> sscan(String key, String cursor) {
        return new JedisCommand<ScanResult<String>>(this) {
            @Override
            public ScanResult<String>  execute(JedisCluster cluster) {
                return cluster.sscan(key,cursor);
            }
        }.run();
    }

    @Override
    public ScanResult<Tuple> zscan(String key, String cursor) {
        return new JedisCommand<ScanResult<Tuple>>(this) {
            @Override
            public ScanResult<Tuple>  execute(JedisCluster cluster) {
                return cluster.zscan(key,cursor);
            }
        }.run();
    }

    @Override
    public Long pfadd(String key, String... elements) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.pfadd(key,elements);
            }
        }.run();
    }

    @Override
    public long pfcount(String key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.pfcount(key);
            }
        }.run();
    }

    @Override
    public List<String> blpop(int timeout, String key) {
        return new JedisCommand<List<String>>(this) {
            @Override
            public List<String>  execute(JedisCluster cluster) {
                return cluster.blpop(timeout,key);
            }
        }.run();
    }

    @Override
    public List<String> brpop(int timeout, String key) {
        return new JedisCommand<List<String>>(this) {
            @Override
            public List<String>  execute(JedisCluster cluster) {
                return cluster.brpop(timeout,key);
            }
        }.run();
    }

    @Override
    public Long del(String... keys) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.del(keys);
            }
        }.run();
    }

    @Override
    public List<String> blpop(int timeout, String... keys) {
        return new JedisCommand<List<String>>(this) {
            @Override
            public List<String>  execute(JedisCluster cluster) {
                return cluster.blpop(timeout,keys);
            }
        }.run();
    }

    @Override
    public List<String> brpop(int timeout, String... keys) {
        return new JedisCommand<List<String>>(this) {
            @Override
            public List<String>  execute(JedisCluster cluster) {
                return cluster.brpop(timeout,keys);
            }
        }.run();
    }

    @Override
    public List<String> mget(String... keys) {
        return new JedisCommand<List<String>>(this) {
            @Override
            public List<String>  execute(JedisCluster cluster) {
                return cluster.mget(keys);
            }
        }.run();
    }

    @Override
    public String mset(String... keysvalues) {
        return new JedisCommand<String>(this) {
            @Override
            public String  execute(JedisCluster cluster) {
                return cluster.mset(keysvalues);
            }
        }.run();
    }

    @Override
    public Long msetnx(String... keysvalues) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.msetnx(keysvalues);
            }
        }.run();
    }

    @Override
    public String rename(String oldkey, String newkey) {
        return new JedisCommand<String>(this) {
            @Override
            public String  execute(JedisCluster cluster) {
                return cluster.rename(oldkey,newkey);
            }
        }.run();
    }

    @Override
    public Long renamenx(String oldkey, String newkey) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.renamenx(oldkey,newkey);
            }
        }.run();
    }

    @Override
    public String rpoplpush(String srckey, String dstkey) {
        return new JedisCommand<String>(this) {
            @Override
            public String  execute(JedisCluster cluster) {
                return cluster.rpoplpush(srckey,dstkey);
            }
        }.run();
    }

    @Override
    public Set<String> sdiff(String... keys) {
        return new JedisCommand<Set<String>>(this) {
            @Override
            public Set<String>  execute(JedisCluster cluster) {
                return cluster.sdiff(keys);
            }
        }.run();
    }

    @Override
    public Long sdiffstore(String dstkey, String... keys) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.sdiffstore(dstkey,keys);
            }
        }.run();
    }

    @Override
    public Set<String> sinter(String... keys) {
        return new JedisCommand<Set<String>>(this) {
            @Override
            public Set<String>  execute(JedisCluster cluster) {
                return cluster.sinter(keys);
            }
        }.run();
    }

    @Override
    public Long sinterstore(String dstkey, String... keys) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.sinterstore(dstkey,keys);
            }
        }.run();
    }

    @Override
    public Long smove(String srckey, String dstkey, String member) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.smove(srckey,dstkey,member);
            }
        }.run();
    }

    @Override
    public Long sort(String key, SortingParams sortingParameters, String dstkey) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.sort(key,sortingParameters,dstkey);
            }
        }.run();
    }

    @Override
    public Long sort(String key, String dstkey) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.sort(key,dstkey);
            }
        }.run();
    }

    @Override
    public Set<String> sunion(String... keys) {
        return new JedisCommand<Set<String>>(this) {
            @Override
            public Set<String>  execute(JedisCluster cluster) {
                return cluster.sunion(keys);
            }
        }.run();
    }

    @Override
    public Long sunionstore(String dstkey, String... keys) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.sunionstore(dstkey,keys);
            }
        }.run();
    }

    @Override
    public Long zinterstore(String dstkey, String... sets) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.zinterstore(dstkey, sets);
            }
        }.run();

    }

    @Override
    public Long zinterstore(String dstkey, ZParams params, String... sets) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.zinterstore(dstkey, params, sets);
            }
        }.run();

    }

    @Override
    public Long zunionstore(String dstkey, String... sets) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.zunionstore(dstkey, sets);
            }
        }.run();

    }

    @Override
    public Long zunionstore(String dstkey, ZParams params, String... sets) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.zunionstore(dstkey, params, sets);
            }
        }.run();

    }

    @Override
    public String brpoplpush(String source, String destination, int timeout) {

        return new JedisCommand<String>(this) {
            @Override
            public String  execute(JedisCluster cluster) {
                return cluster.brpoplpush(source, destination, timeout);
            }
        }.run();
    }

    @Override
    public Long publish(String channel, String message) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.publish(channel, message);
            }
        }.run();

    }

    @Override
    public void subscribe(JedisPubSub jedisPubSub, String... channels) {
         new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                 cluster.subscribe(jedisPubSub, channels);
                 return 0L;
            }
        }.run();

    }

    @Override
    public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {

        new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                cluster.psubscribe(jedisPubSub, patterns);
                return 0L;
            }
        }.run();
    }

    @Override
    public Long bitop(BitOP op, String destKey, String... srcKeys) {


        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.bitop(op, destKey, srcKeys);
            }
        }.run();
    }

    @Override
    public String pfmerge(String destkey, String... sourcekeys) {

        return new JedisCommand<String>(this) {
            @Override
            public String  execute(JedisCluster cluster) {
                return cluster.pfmerge(destkey, sourcekeys);
            }
        }.run();
    }

    @Override
    public long pfcount(String... keys) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long  execute(JedisCluster cluster) {
                return cluster.pfcount(keys);
            }
        }.run();

    }

    @Override
    public Object eval(String script, int keyCount, String... params) {
        return new JedisCommand<Object>(this) {
            @Override
            public Object  execute(JedisCluster cluster) {
                return cluster.eval(script, keyCount, params);
            }
        }.run();

    }

    @Override
    public Object eval(String script, String key) {

        return new JedisCommand<Object>(this) {
            @Override
            public Object  execute(JedisCluster cluster) {
                return cluster.eval(script, key);
            }
        }.run();
    }

    @Override
    public Object eval(String script, List<String> keys, List<String> args) {

        return new JedisCommand<Object>(this) {
            @Override
            public Object  execute(JedisCluster cluster) {
                return cluster.eval(script, keys, args);
            }
        }.run();
    }

    @Override
    public Object evalsha(String sha1, int keyCount, String... params) {
        return new JedisCommand<Object>(this) {
            @Override
            public Object  execute(JedisCluster cluster) {
                return cluster.evalsha(sha1, keyCount, params);
            }
        }.run();
    }

    @Override
    public Object evalsha(String sha1, List<String> keys, List<String> args) {

        return new JedisCommand<Object>(this) {
            @Override
            public Object  execute(JedisCluster cluster) {
                return cluster.evalsha(sha1, keys, args);
            }
        }.run();
    }

    @Override
    public Object evalsha(String script, String key) {
        return new JedisCommand<Object>(this) {
            @Override
            public Object  execute(JedisCluster cluster) {
                return cluster.evalsha(script, key);
            }
        }.run();
    }

    @Override
    public Boolean scriptExists(String sha1, String key) {
        return new JedisCommand<Boolean>(this) {
            @Override
            public Boolean  execute(JedisCluster cluster) {
                return cluster.scriptExists(sha1, key);
            }
        }.run();

    }

    @Override
    public List<Boolean> scriptExists(String key, String... sha1) {

        return new JedisCommand<List<Boolean>>(this) {
            @Override
            public List<Boolean>  execute(JedisCluster cluster) {
                return cluster.scriptExists(key, sha1);
            }
        }.run();

    }

    @Override
    public String scriptLoad(String script, String key) {
        return new JedisCommand<String>(this) {
            @Override
            public String  execute(JedisCluster cluster) {
                return cluster.scriptLoad(script, key);
            }
        }.run();
    }

    public RediseAutoProxyCluster(HostAndPort master,HostAndPort senconde) {
        super(master);
        seconder = new JedisCluster(senconde);


    }

    @Override
    public void close() {
        if(this.seconder!=null){
            this.seconder.close();
        }
        if(this.master!=null){
            master.close();
        }
    }

    @Override
    public Map<String, JedisPool> getClusterNodes() {
        if(this.isMaster){
            return master.getClusterNodes();
        }
        else {
            return seconder.getClusterNodes();
        }
    }

    @Override
    public String set(byte[] key, byte[] value) {
        return new JedisCommand<String>(this) {
            @Override
            public String  execute(JedisCluster cluster) {
                return cluster.set(key, value);
            }
        }.run();

    }

    @Override
    public String set(byte[] key, byte[] value, SetParams params) {
        return new JedisCommand<String>(this) {
            @Override
            public String  execute(JedisCluster cluster) {
                return cluster.set(key, value, params);
            }
        }.run();

    }

    @Override
    public byte[] get(byte[] key) {
        return new JedisCommand<byte[]>(this) {
            @Override
            public byte[]  execute(JedisCluster cluster) {
                return cluster.get(key);
            }
        }.run();

    }

    @Override
    public Long exists(byte[]... keys) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.exists(keys);
            }
        }.run();

    }

    @Override
    public Boolean exists(byte[] key) {
        return new JedisCommand<Boolean>(this) {
            @Override
            public Boolean execute(JedisCluster cluster) {
                return cluster.exists(key);
            }
        }.run();
    }

    @Override
    public Long persist(byte[] key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.persist(key);
            }
        }.run();
    }

    @Override
    public String type(byte[] key) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.type(key);
            }
        }.run();

    }

    @Override
    public Long expire(byte[] key, int seconds) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.expire(key, seconds);
            }
        }.run();

    }

    @Override
    public Long pexpire(byte[] key, long milliseconds) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.pexpire(key, milliseconds);
            }
        }.run();
    }

    @Override
    public Long expireAt(byte[] key, long unixTime) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.expireAt(key, unixTime);
            }
        }.run();
    }

    @Override
    public Long pexpireAt(byte[] key, long millisecondsTimestamp) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.pexpireAt(key, millisecondsTimestamp);
            }
        }.run();
    }

    @Override
    public Long ttl(byte[] key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.ttl(key);
            }
        }.run();
    }

    @Override
    public Boolean setbit(byte[] key, long offset, boolean value) {
        return new JedisCommand<Boolean>(this) {
            @Override
            public Boolean execute(JedisCluster cluster) {
                return cluster.setbit(key, offset, value);
            }
        }.run();

    }

    @Override
    public Boolean setbit(byte[] key, long offset, byte[] value) {
        return new JedisCommand<Boolean>(this) {
            @Override
            public Boolean execute(JedisCluster cluster) {
                return cluster.setbit(key, offset, value);
            }
        }.run();

    }

    @Override
    public Boolean getbit(byte[] key, long offset) {

        return new JedisCommand<Boolean>(this) {
            @Override
            public Boolean execute(JedisCluster cluster) {
                return cluster.getbit(key, offset);
            }
        }.run();
    }

    @Override
    public Long setrange(byte[] key, long offset, byte[] value) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.setrange(key, offset, value);
            }
        }.run();

    }

    @Override
    public byte[] getrange(byte[] key, long startOffset, long endOffset) {

        return new JedisCommand<byte[]>(this) {
            @Override
            public byte[] execute(JedisCluster cluster) {
                return cluster.getrange(key, startOffset, endOffset);
            }
        }.run();

    }

    @Override
    public byte[] getSet(byte[] key, byte[] value) {
        return new JedisCommand<byte[]>(this) {
            @Override
            public byte[] execute(JedisCluster cluster) {
                return cluster.getSet(key, value);
            }
        }.run();
    }

    @Override
    public Long setnx(byte[] key, byte[] value) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.setnx(key, value);
            }
        }.run();
    }

    @Override
    public String setex(byte[] key, int seconds, byte[] value) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.setex(key, seconds, value);
            }
        }.run();
    }

    @Override
    public Long decrBy(byte[] key, long integer) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.decrBy(key, integer);
            }
        }.run();
    }

    @Override
    public Long decr(byte[] key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.decr(key);
            }
        }.run();
    }

    @Override
    public Long incrBy(byte[] key, long integer) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.incrBy(key, integer);
            }
        }.run();
    }

    @Override
    public Double incrByFloat(byte[] key, double value) {
        return new JedisCommand<Double>(this) {
            @Override
            public Double execute(JedisCluster cluster) {
                return cluster.incrByFloat(key, value);
            }
        }.run();

    }

    @Override
    public Long incr(byte[] key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.incr(key);
            }
        }.run();
    }

    @Override
    public Long append(byte[] key, byte[] value) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.append(key, value);
            }
        }.run();

    }

    @Override
    public byte[] substr(byte[] key, int start, int end) {
        return new JedisCommand<byte[]>(this) {
            @Override
            public byte[] execute(JedisCluster cluster) {
                return cluster.substr(key, start, end);
            }
        }.run();

    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.hset(key, field, value);
            }
        }.run();

    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        return new JedisCommand<byte[]>(this) {
            @Override
            public byte[] execute(JedisCluster cluster) {
                return cluster.hget(key, field);
            }
        }.run();

    }

    @Override
    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.hsetnx(key, field, value);
            }
        }.run();

    }

    @Override
    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.hmset(key, hash);
            }
        }.run();
    }

    @Override
    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        return new JedisCommand<List<byte[]>>(this) {
            @Override
            public List<byte[]> execute(JedisCluster cluster) {
                return cluster.hmget(key, fields);
            }
        }.run();

    }

    @Override
    public Long hincrBy(byte[] key, byte[] field, long value) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.hincrBy(key, field, value);
            }
        }.run();
    }

    @Override
    public Double hincrByFloat(byte[] key, byte[] field, double value) {
        return new JedisCommand<Double>(this) {
            @Override
            public Double execute(JedisCluster cluster) {
                return cluster.hincrByFloat(key, field, value);
            }
        }.run();

    }

    @Override
    public Boolean hexists(byte[] key, byte[] field) {
        return new JedisCommand<Boolean>(this) {
            @Override
            public Boolean execute(JedisCluster cluster) {
                return cluster.hexists(key, field);
            }
        }.run();

    }

    @Override
    public Long hdel(byte[] key, byte[]... field) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.hdel(key, field);
            }
        }.run();


    }

    @Override
    public Long hlen(byte[] key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.hlen(key);
            }
        }.run();
    }

    @Override
    public Set<byte[]> hkeys(byte[] key) {
        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.hkeys(key);
            }
        }.run();

    }

    @Override
    public Collection<byte[]> hvals(byte[] key) {

        return new JedisCommand<Collection<byte[]>>(this) {
            @Override
            public Collection<byte[]> execute(JedisCluster cluster) {
                return cluster.hvals(key);
            }
        }.run();

    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {

        return new JedisCommand<Map<byte[], byte[]>>(this) {
            @Override
            public Map<byte[], byte[]> execute(JedisCluster cluster) {
                return cluster.hgetAll(key);
            }
        }.run();
    }

    @Override
    public Long rpush(byte[] key, byte[]... args) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.rpush(key, args);
            }
        }.run();

    }

    @Override
    public Long lpush(byte[] key, byte[]... args) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.lpush(key, args);
            }
        }.run();

    }

    @Override
    public Long llen(byte[] key) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.llen(key);
            }
        }.run();

    }

    @Override
    public List<byte[]> lrange(byte[] key, long start, long end) {

        return new JedisCommand<List<byte[]>>(this) {
            @Override
            public List<byte[]> execute(JedisCluster cluster) {
                return cluster.lrange(key, start, end);
            }
        }.run();
    }

    @Override
    public String ltrim(byte[] key, long start, long end) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.ltrim(key, start, end);
            }
        }.run();
    }

    @Override
    public byte[] lindex(byte[] key, long index) {
        return new JedisCommand<byte[]>(this) {
            @Override
            public byte[] execute(JedisCluster cluster) {
                return cluster.lindex(key, index);
            }
        }.run();

    }

    @Override
    public String lset(byte[] key, long index, byte[] value) {

        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.lset(key, index, value);
            }
        }.run();
    }

    @Override
    public Long lrem(byte[] key, long count, byte[] value) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.lrem(key, count, value);
            }
        }.run();

    }

    @Override
    public byte[] lpop(byte[] key) {
        return new JedisCommand<byte[]>(this) {
            @Override
            public byte[] execute(JedisCluster cluster) {
                return cluster.lpop(key);
            }
        }.run();

    }

    @Override
    public byte[] rpop(byte[] key) {
        return new JedisCommand<byte[]>(this) {
            @Override
            public byte[] execute(JedisCluster cluster) {
                return cluster.rpop(key);
            }
        }.run();
    }

    @Override
    public Long sadd(byte[] key, byte[]... member) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.sadd(key, member);
            }
        }.run();
    }

    @Override
    public Set<byte[]> smembers(byte[] key) {
        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.smembers(key);
            }
        }.run();

    }

    @Override
    public Long srem(byte[] key, byte[]... member) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.srem(key, member);
            }
        }.run();
    }

    @Override
    public byte[] spop(byte[] key) {

        return new JedisCommand<byte[]>(this) {
            @Override
            public byte[] execute(JedisCluster cluster) {
                return cluster.spop(key);
            }
        }.run();

    }

    @Override
    public Set<byte[]> spop(byte[] key, long count) {

        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.spop(key, count);
            }
        }.run();
    }

    @Override
    public Long scard(byte[] key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.scard(key);
            }
        }.run();

    }

    @Override
    public Boolean sismember(byte[] key, byte[] member) {
        return new JedisCommand<Boolean>(this) {
            @Override
            public Boolean execute(JedisCluster cluster) {
                return cluster.sismember(key, member);
            }
        }.run();

    }

    @Override
    public byte[] srandmember(byte[] key) {
        return new JedisCommand<byte[]>(this) {
            @Override
            public byte[] execute(JedisCluster cluster) {
                return cluster.srandmember(key);
            }
        }.run();

    }

    @Override
    public Long strlen(byte[] key) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.strlen(key);
            }
        }.run();

    }

    @Override
    public Long zadd(byte[] key, double score, byte[] member) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zadd(key, score, member);
            }
        }.run();

    }

    @Override
    public Long zadd(byte[] key, double score, byte[] member, ZAddParams params) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zadd(key, score, member, params);
            }
        }.run();

    }

    @Override
    public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zadd(key, scoreMembers);
            }
        }.run();

    }

    @Override
    public Long zadd(byte[] key, Map<byte[], Double> scoreMembers, ZAddParams params) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zadd(key, scoreMembers, params);
            }
        }.run();

    }

    @Override
    public Set<byte[]> zrange(byte[] key, long start, long end) {
        return new JedisCommand<Set<byte[]> >(this) {
            @Override
            public Set<byte[]>  execute(JedisCluster cluster) {
                return cluster.zrange(key, start, end);
            }
        }.run();

    }

    @Override
    public Long zrem(byte[] key, byte[]... member) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zrem(key, member);
            }
        }.run();

    }

    @Override
    public Double zincrby(byte[] key, double score, byte[] member) {
        return new JedisCommand<Double>(this) {
            @Override
            public Double execute(JedisCluster cluster) {
                return cluster.zincrby(key, score, member);
            }
        }.run();
    }

    @Override
    public Double zincrby(byte[] key, double score, byte[] member, ZIncrByParams params) {

        return new JedisCommand<Double>(this) {
            @Override
            public Double execute(JedisCluster cluster) {
                return cluster.zincrby(key, score, member, params);
            }
        }.run();

    }

    @Override
    public Long zrank(byte[] key, byte[] member) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zrank(key, member);
            }
        }.run();

    }

    @Override
    public Long zrevrank(byte[] key, byte[] member) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zrevrank(key, member);
            }
        }.run();

    }

    @Override
    public Set<byte[]> zrevrange(byte[] key, long start, long end) {

        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.zrevrange(key, start, end);
            }
        }.run();

    }

    @Override
    public Set<Tuple> zrangeWithScores(byte[] key, long start, long end) {

        return new JedisCommand<Set<Tuple>>(this) {
            @Override
            public Set<Tuple> execute(JedisCluster cluster) {
                return cluster.zrangeWithScores(key, start, end);
            }
        }.run();
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(byte[] key, long start, long end) {

        return new JedisCommand<Set<Tuple>>(this) {
            @Override
            public Set<Tuple> execute(JedisCluster cluster) {
                return cluster.zrevrangeWithScores(key, start, end);
            }
        }.run();

    }

    @Override
    public Long zcard(byte[] key) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zcard(key);
            }
        }.run();

    }

    @Override
    public Double zscore(byte[] key, byte[] member) {

        return new JedisCommand<Double>(this) {
            @Override
            public Double execute(JedisCluster cluster) {
                return cluster.zscore(key, member);
            }
        }.run();

    }

    @Override
    public List<byte[]> sort(byte[] key) {
        return new JedisCommand<List<byte[]>>(this) {
            @Override
            public List<byte[]> execute(JedisCluster cluster) {
                return cluster.sort(key);
            }
        }.run();

    }

    @Override
    public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
        return new JedisCommand<List<byte[]>>(this) {
            @Override
            public List<byte[]> execute(JedisCluster cluster) {
                return cluster.sort(key, sortingParameters);
            }
        }.run();

    }

    @Override
    public Long zcount(byte[] key, double min, double max) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zcount(key, min, max);
            }
        }.run();

    }

    @Override
    public Long zcount(byte[] key, byte[] min, byte[] max) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zcount(key, min, max);
            }
        }.run();

    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {

        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.zrangeByScore(key, min, max);
            }
        }.run();

    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max) {

        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.zrangeByScore(key, min, max);
            }
        }.run();

    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.zrevrangeByScore(key, max, min);
            }
        }.run();

    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {

        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.zrangeByScore(key, min, max, offset, count);
            }
        }.run();

    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.zrevrangeByScore(key, max, min);
            }
        }.run();

    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.zrangeByScore(key, min, max, offset, count);
            }
        }.run();

    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {

        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.zrevrangeByScore(key, max, min, offset, count);
            }
        }.run();

    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        return new JedisCommand<Set<Tuple>>(this) {
            @Override
            public Set<Tuple> execute(JedisCluster cluster) {
                return cluster.zrangeByScoreWithScores(key, min, max);
            }
        }.run();

    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        return new JedisCommand<Set<Tuple>>(this) {
            @Override
            public Set<Tuple> execute(JedisCluster cluster) {
                return cluster.zrevrangeByScoreWithScores(key, max, min);
            }
        }.run();

    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {

        return new JedisCommand<Set<Tuple>>(this) {
            @Override
            public Set<Tuple> execute(JedisCluster cluster) {
                return cluster.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        }.run();

    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {

        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.zrevrangeByScore(key, max, min, offset, count);
            }
        }.run();

    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
        return new JedisCommand<Set<Tuple>>(this) {
            @Override
            public Set<Tuple> execute(JedisCluster cluster) {
                return cluster.zrangeByScoreWithScores(key, min, max);
            }
        }.run();
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {

        return new JedisCommand<Set<Tuple>>(this) {
            @Override
            public Set<Tuple> execute(JedisCluster cluster) {
                return cluster.zrevrangeByScoreWithScores(key, max, min);
            }
        }.run();

    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset, int count) {

        return new JedisCommand<Set<Tuple>>(this) {
            @Override
            public Set<Tuple> execute(JedisCluster cluster) {
                return cluster.zrangeByScoreWithScores(key, min, max, offset, count);
            }
        }.run();

    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {

        return new JedisCommand<Set<Tuple>>(this) {
            @Override
            public Set<Tuple> execute(JedisCluster cluster) {
                return cluster.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        }.run();
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset, int count) {

        return new JedisCommand<Set<Tuple>>(this) {
            @Override
            public Set<Tuple> execute(JedisCluster cluster) {
                return cluster.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }
        }.run();

    }

    @Override
    public Long zremrangeByRank(byte[] key, long start, long end) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zremrangeByRank(key, start, end);
            }
        }.run();

    }

    @Override
    public Long zremrangeByScore(byte[] key, double start, double end) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zremrangeByScore(key, start, end);
            }
        }.run();

    }

    @Override
    public Long zremrangeByScore(byte[] key, byte[] start, byte[] end) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zremrangeByScore(key, start, end);
            }
        }.run();

    }

    @Override
    public Long linsert(byte[] key, BinaryClient.LIST_POSITION where, byte[] pivot, byte[] value) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.linsert(key, where, pivot, value);
            }
        }.run();

    }

    @Override
    public Long lpushx(byte[] key, byte[]... arg) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.lpushx(key, arg);
            }
        }.run();

    }

    @Override
    public Long rpushx(byte[] key, byte[]... arg) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.rpushx(key, arg);
            }
        }.run();
    }

    @Override
    public Long del(byte[] key) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.del(key);
            }
        }.run();

    }

    @Override
    public byte[] echo(byte[] arg) {
        return new JedisCommand<byte[]>(this) {
            @Override
            public byte[] execute(JedisCluster cluster) {
                return cluster.echo(arg);
            }
        }.run();

    }

    @Override
    public Long bitcount(byte[] key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.bitcount(key);
            }
        }.run();

    }

    @Override
    public Long bitcount(byte[] key, long start, long end) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.bitcount(key, start, end);
            }
        }.run();
    }

    @Override
    public Long pfadd(byte[] key, byte[]... elements) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.pfadd(key, elements);
            }
        }.run();

    }

    @Override
    public long pfcount(byte[] key) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.pfcount(key);
            }
        }.run();

    }

    @Override
    public List<byte[]> srandmember(byte[] key, int count) {

        return new JedisCommand<List<byte[]>>(this) {
            @Override
            public List<byte[]> execute(JedisCluster cluster) {
                return cluster.srandmember(key, count);
            }
        }.run();

    }

    @Override
    public Long zlexcount(byte[] key, byte[] min, byte[] max) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zlexcount(key, min, max);
            }
        }.run();

    }

    @Override
    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max) {
        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.zrangeByLex(key, min, max);
            }
        }.run();

    }

    @Override
    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max, int offset, int count) {

        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.zrangeByLex(key, min, max, offset, count);
            }
        }.run();


    }

    @Override
    public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min) {

        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.zrevrangeByLex(key, max, min);
            }
        }.run();

    }

    @Override
    public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min, int offset, int count) {
        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.zrevrangeByLex(key, max, min, offset, count);
            }
        }.run();

    }

    @Override
    public Long zremrangeByLex(byte[] key, byte[] min, byte[] max) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zremrangeByLex(key, min, max);
            }
        }.run();

    }

    @Override
    public Object eval(byte[] script, byte[] keyCount, byte[]... params) {

        return new JedisCommand<Object>(this) {
            @Override
            public Object execute(JedisCluster cluster) {
                return cluster.eval(script, keyCount, params);
            }
        }.run();

    }

    @Override
    public Object eval(byte[] script, int keyCount, byte[]... params) {

        return new JedisCommand<Object>(this) {
            @Override
            public Object execute(JedisCluster cluster) {
                return cluster.eval(script, keyCount, params);
            }
        }.run();

    }

    @Override
    public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
        return new JedisCommand<Object>(this) {
            @Override
            public Object execute(JedisCluster cluster) {
                return cluster.eval(script, keys, args);
            }
        }.run();

    }

    @Override
    public Object eval(byte[] script, byte[] key) {

        return new JedisCommand<Object>(this) {
            @Override
            public Object execute(JedisCluster cluster) {
                return cluster.eval(script, key);
            }
        }.run();

    }

    @Override
    public Object evalsha(byte[] script, byte[] key) {
        return new JedisCommand<Object>(this) {
            @Override
            public Object execute(JedisCluster cluster) {
                return cluster.evalsha(script, key);
            }
        }.run();

    }

    @Override
    public Object evalsha(byte[] sha1, List<byte[]> keys, List<byte[]> args) {

        return new JedisCommand<Object>(this) {
            @Override
            public Object execute(JedisCluster cluster) {
                return cluster.evalsha(sha1, keys, args);
            }
        }.run();

    }

    @Override
    public Object evalsha(byte[] sha1, int keyCount, byte[]... params) {

        return new JedisCommand<Object>(this) {
            @Override
            public Object execute(JedisCluster cluster) {
                return cluster.evalsha(sha1, keyCount, params);
            }
        }.run();
    }

    @Override
    public List<Long> scriptExists(byte[] key, byte[][] sha1) {

        return new JedisCommand<List<Long>>(this) {
            @Override
            public List<Long> execute(JedisCluster cluster) {
                return cluster.scriptExists(key, sha1);
            }
        }.run();

    }

    @Override
    public byte[] scriptLoad(byte[] script, byte[] key) {

        return new JedisCommand<byte[]>(this) {
            @Override
            public byte[] execute(JedisCluster cluster) {
                return cluster.scriptLoad(script, key);
            }
        }.run();

    }

    @Override
    public String scriptFlush(byte[] key) {

        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.scriptFlush(key);
            }
        }.run();
    }

    @Override
    public String scriptKill(byte[] key) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.scriptKill(key);
            }
        }.run();

    }

    @Override
    public Long del(byte[]... keys) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.del(keys);
            }
        }.run();

    }

    @Override
    public List<byte[]> blpop(int timeout, byte[]... keys) {
        return new JedisCommand<List<byte[]>>(this) {
            @Override
            public List<byte[]> execute(JedisCluster cluster) {
                return cluster.blpop(timeout, keys);
            }
        }.run();

    }

    @Override
    public List<byte[]> brpop(int timeout, byte[]... keys) {
        return new JedisCommand<List<byte[]>>(this) {
            @Override
            public List<byte[]> execute(JedisCluster cluster) {
                return cluster.brpop(timeout, keys);
            }
        }.run();

    }

    @Override
    public List<byte[]> mget(byte[]... keys) {

        return new JedisCommand<List<byte[]>>(this) {
            @Override
            public List<byte[]> execute(JedisCluster cluster) {
                return cluster.mget(keys);
            }
        }.run();

    }

    @Override
    public String mset(byte[]... keysvalues) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.mset(keysvalues);
            }
        }.run();

    }

    @Override
    public Long msetnx(byte[]... keysvalues) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.msetnx(keysvalues);
            }
        }.run();

    }

    @Override
    public String rename(byte[] oldkey, byte[] newkey) {

        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.rename(oldkey, newkey);
            }
        }.run();

    }

    @Override
    public Long renamenx(byte[] oldkey, byte[] newkey) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.renamenx(oldkey, newkey);
            }
        }.run();

    }

    @Override
    public byte[] rpoplpush(byte[] srckey, byte[] dstkey) {

        return new JedisCommand<byte[]>(this) {
            @Override
            public byte[] execute(JedisCluster cluster) {
                return cluster.rpoplpush(srckey, dstkey);
            }
        }.run();

    }

    @Override
    public Set<byte[]> sdiff(byte[]... keys) {

        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.sdiff(keys);
            }
        }.run();

    }

    @Override
    public Long sdiffstore(byte[] dstkey, byte[]... keys) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.sdiffstore(dstkey, keys);
            }
        }.run();

    }

    @Override
    public Set<byte[]> sinter(byte[]... keys) {

        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.sinter(keys);
            }
        }.run();

    }

    @Override
    public Long sinterstore(byte[] dstkey, byte[]... keys) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.sinterstore(dstkey, keys);
            }
        }.run();

    }

    @Override
    public Long smove(byte[] srckey, byte[] dstkey, byte[] member) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.smove(srckey, dstkey, member);
            }
        }.run();

    }

    @Override
    public Long sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.sort(key, sortingParameters, dstkey);
            }
        }.run();

    }

    @Override
    public Long sort(byte[] key, byte[] dstkey) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.sort(key, dstkey);
            }
        }.run();
    }

    @Override
    public Set<byte[]> sunion(byte[]... keys) {
        return new JedisCommand<Set<byte[]>>(this) {
            @Override
            public Set<byte[]> execute(JedisCluster cluster) {
                return cluster.sunion(keys);
            }
        }.run();
    }

    @Override
    public Long sunionstore(byte[] dstkey, byte[]... keys) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.sunionstore(dstkey, keys);
            }
        }.run();

    }

    @Override
    public Long zinterstore(byte[] dstkey, byte[]... sets) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zinterstore(dstkey, sets);
            }
        }.run();

    }

    @Override
    public Long zinterstore(byte[] dstkey, ZParams params, byte[]... sets) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zinterstore(dstkey, params, sets);
            }
        }.run();

    }

    @Override
    public Long zunionstore(byte[] dstkey, byte[]... sets) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zunionstore(dstkey, sets);
            }
        }.run();

    }

    @Override
    public Long zunionstore(byte[] dstkey, ZParams params, byte[]... sets) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.zunionstore(dstkey, params, sets);
            }
        }.run();

    }

    @Override
    public byte[] brpoplpush(byte[] source, byte[] destination, int timeout) {

        return new JedisCommand<byte[]>(this) {
            @Override
            public byte[] execute(JedisCluster cluster) {
                return cluster.brpoplpush(source, destination, timeout);
            }
        }.run();

    }

    @Override
    public Long publish(byte[] channel, byte[] message) {

        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.publish(channel, message);
            }
        }.run();

    }

    @Override
    public void subscribe(BinaryJedisPubSub jedisPubSub, byte[]... channels) {
         new JedisCommand<Integer>(this) {
            @Override
            public Integer execute(JedisCluster cluster) {
                 cluster.subscribe(jedisPubSub, channels);
                 return 1;
            }
        }.run();

    }

    @Override
    public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {

        new JedisCommand<Integer>(this) {
            @Override
            public Integer execute(JedisCluster cluster) {
                cluster.subscribe(jedisPubSub, patterns);
                return 0;
            }
        }.run();
    }

    @Override
    public Long bitop(BitOP op, byte[] destKey, byte[]... srcKeys) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.bitop(op, destKey, srcKeys);

            }
        }.run();

    }

    @Override
    public String pfmerge(byte[] destkey, byte[]... sourcekeys) {
        return new JedisCommand<String>(this) {
            @Override
            public String execute(JedisCluster cluster) {
                return cluster.pfmerge(destkey, sourcekeys);

            }
        }.run();

    }

    @Override
    public Long pfcount(byte[]... keys) {
        return new JedisCommand<Long>(this) {
            @Override
            public Long execute(JedisCluster cluster) {
                return cluster.pfcount(keys);

            }
        }.run();

    }



    public SwitchRule getRule() {
        return rules;
    }

    public void setRules(SwitchRule rules) {
        this.rules = rules;
    }

    public boolean isBackupState() {
        return backupState;
    }

    public void setBackupState(boolean backupState) {
        this.backupState = backupState;
    }

    public boolean isMasterState() {
        return masterState;
    }

    public void setMasterState(boolean masterState) {
        this.masterState = masterState;
    }

    /**
     * 检查集群状态是否可用
     * @return 是否可用
     */
    public boolean CheckClusterState(Set<HostAndPort> hosts){
        boolean  result = false;
        for (HostAndPort host:hosts) {
            RedisConnection connection  = new RedisConnection(host.getHost(),host.getPort());
            try{
                connection.connection();
                connection.getOutputStream().write("cluster info".getBytes());
                connection.getOutputStream().writeCrLf();
                String info = connection.getBulkReply();
                String[] infos = info.split("\r\n");
                for (String i : infos) {
                    if (i.toLowerCase().contains("cluster_state")) {
                        String state = i.split(":")[1];
                        if("ok".equals(state)){
                            return true;
                        }
                    }
                }
            }catch (IOException e){

            }
            finally {
                connection.close();
            }
        }
        return result;
    }

    public long getCheckInterval() {
        return checkInterval;
    }

    public void setCheckInterval(long checkInterval) {
        this.checkInterval = checkInterval;
    }

    public JedisCluster getSeconder() {
        return seconder;
    }

    public JedisCluster getMaster() {
        return master;
    }

    public String getMasterAddress() {
        return masterAddress;
    }

    public void setMasterAddress(String masterAddress) {
        this.masterAddress = masterAddress;
    }

    public String getSecondAddress() {
        return secondAddress;
    }

    public void setSecondAddress(String secondAddress) {
        this.secondAddress = secondAddress;
    }

    /**
     * 主节点列表
     * @return
     */
    public Set<HostAndPort> getMasterHosts() {
        return masterHosts;
    }

    /**
     * 备节点地址列表
     * @return
     */
    public Set<HostAndPort> getSecondHosts() {
        return secondHosts;
    }


    public boolean isAuto() {
        return isAuto;
    }

    public void setAuto(boolean auto) {
        isAuto = auto;
    }

    public GenericObjectPoolConfig getConfig() {
        return config;
    }

    public void setConfig(GenericObjectPoolConfig config) {
        this.config = config;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
}
