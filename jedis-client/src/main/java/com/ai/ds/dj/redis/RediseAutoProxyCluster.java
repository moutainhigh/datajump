package com.ai.ds.dj.redis;

import redis.clients.jedis.*;
import redis.clients.jedis.params.set.SetParams;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.jedis.params.sortedset.ZIncrByParams;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Copyright asiainfo.com
 * redis自动代理客户端，实现客户端的自动
 *
 * @author wuwh6
 */
public class RediseAutoProxyCluster extends JedisCluster {

    private JedisCluster seconder;
    private List<SwitchRule> rules = new ArrayList<>();

    /**
     * 是否当前可用的练级是master
     */
    private volatile  boolean isMaster=true;

    /**
     * 统计调用量
     */
    private AtomicLong  execAllcount;
    /**
     * 统计失败调用量
     */

    private AtomicLong execErrorcount;
    public RediseAutoProxyCluster(HostAndPort node) {
            super(node);
    }

    private boolean isMaster(){
        //如果是主节点
       return isMaster;
    }


    @Override
    public String set(String key, String value) {
        String result = null;
        if(this.isMaster) {
            try{
                result =  super.set(key, value);
            }catch (Exception e){
                //失败调用
                this.execErrorcount.getAndIncrement();
            }

        }
        else {
           return  this.seconder.set(key,value);
        }
        return null;
    }

    @Override
    public String set(String key, String value, SetParams params) {
        return super.set(key, value, params);
    }

    @Override
    public String get(String key) {
        return super.get(key);
    }

    @Override
    public Boolean exists(String key) {
        return super.exists(key);
    }

    @Override
    public Long exists(String... keys) {
        return super.exists(keys);
    }

    @Override
    public Long persist(String key) {
        return super.persist(key);
    }

    @Override
    public String type(String key) {
        return super.type(key);
    }

    @Override
    public Long expire(String key, int seconds) {
        return super.expire(key, seconds);
    }

    @Override
    public Long pexpire(String key, long milliseconds) {
        return super.pexpire(key, milliseconds);
    }

    @Override
    public Long expireAt(String key, long unixTime) {
        return super.expireAt(key, unixTime);
    }

    @Override
    public Long pexpireAt(String key, long millisecondsTimestamp) {
        return super.pexpireAt(key, millisecondsTimestamp);
    }

    @Override
    public Long ttl(String key) {
        return super.ttl(key);
    }

    @Override
    public Boolean setbit(String key, long offset, boolean value) {
        return super.setbit(key, offset, value);
    }

    @Override
    public Boolean setbit(String key, long offset, String value) {
        return super.setbit(key, offset, value);
    }

    @Override
    public Boolean getbit(String key, long offset) {
        return super.getbit(key, offset);
    }

    @Override
    public Long setrange(String key, long offset, String value) {
        return super.setrange(key, offset, value);
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset) {
        return super.getrange(key, startOffset, endOffset);
    }

    @Override
    public String getSet(String key, String value) {
        return super.getSet(key, value);
    }

    @Override
    public Long setnx(String key, String value) {
        return super.setnx(key, value);
    }

    @Override
    public String setex(String key, int seconds, String value) {
        return super.setex(key, seconds, value);
    }

    @Override
    public Long decrBy(String key, long integer) {
        return super.decrBy(key, integer);
    }

    @Override
    public Long decr(String key) {
        return super.decr(key);
    }

    @Override
    public Long incrBy(String key, long integer) {
        return super.incrBy(key, integer);
    }

    @Override
    public Double incrByFloat(String key, double value) {
        return super.incrByFloat(key, value);
    }

    @Override
    public Long incr(String key) {
        return super.incr(key);
    }

    @Override
    public Long append(String key, String value) {
        return super.append(key, value);
    }

    @Override
    public String substr(String key, int start, int end) {
        return super.substr(key, start, end);
    }

    @Override
    public Long hset(String key, String field, String value) {
        return super.hset(key, field, value);
    }

    @Override
    public String hget(String key, String field) {
        return super.hget(key, field);
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        return super.hsetnx(key, field, value);
    }

    @Override
    public String hmset(String key, Map<String, String> hash) {
        return super.hmset(key, hash);
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        return super.hmget(key, fields);
    }

    @Override
    public Long hincrBy(String key, String field, long value) {
        return super.hincrBy(key, field, value);
    }

    @Override
    public Boolean hexists(String key, String field) {
        return super.hexists(key, field);
    }

    @Override
    public Long hdel(String key, String... field) {
        return super.hdel(key, field);
    }

    @Override
    public Long hlen(String key) {
        return super.hlen(key);
    }

    @Override
    public Set<String> hkeys(String key) {
        return super.hkeys(key);
    }

    @Override
    public List<String> hvals(String key) {
        return super.hvals(key);
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return super.hgetAll(key);
    }

    @Override
    public Long rpush(String key, String... string) {
        return super.rpush(key, string);
    }

    @Override
    public Long lpush(String key, String... string) {
        return super.lpush(key, string);
    }

    @Override
    public Long llen(String key) {
        return super.llen(key);
    }

    @Override
    public List<String> lrange(String key, long start, long end) {
        return super.lrange(key, start, end);
    }

    @Override
    public String ltrim(String key, long start, long end) {
        return super.ltrim(key, start, end);
    }

    @Override
    public String lindex(String key, long index) {
        return super.lindex(key, index);
    }

    @Override
    public String lset(String key, long index, String value) {
        return super.lset(key, index, value);
    }

    @Override
    public Long lrem(String key, long count, String value) {
        return super.lrem(key, count, value);
    }

    @Override
    public String lpop(String key) {
        return super.lpop(key);
    }

    @Override
    public String rpop(String key) {
        return super.rpop(key);
    }

    @Override
    public Long sadd(String key, String... member) {
        return super.sadd(key, member);
    }

    @Override
    public Set<String> smembers(String key) {
        return super.smembers(key);
    }

    @Override
    public Long srem(String key, String... member) {
        return super.srem(key, member);
    }

    @Override
    public String spop(String key) {
        return super.spop(key);
    }

    @Override
    public Set<String> spop(String key, long count) {
        return super.spop(key, count);
    }

    @Override
    public Long scard(String key) {
        return super.scard(key);
    }

    @Override
    public Boolean sismember(String key, String member) {
        return super.sismember(key, member);
    }

    @Override
    public String srandmember(String key) {
        return super.srandmember(key);
    }

    @Override
    public List<String> srandmember(String key, int count) {
        return super.srandmember(key, count);
    }

    @Override
    public Long strlen(String key) {
        return super.strlen(key);
    }

    @Override
    public Long zadd(String key, double score, String member) {
        return super.zadd(key, score, member);
    }

    @Override
    public Long zadd(String key, double score, String member, ZAddParams params) {
        return super.zadd(key, score, member, params);
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) {
        return super.zadd(key, scoreMembers);
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers, ZAddParams params) {
        return super.zadd(key, scoreMembers, params);
    }

    @Override
    public Set<String> zrange(String key, long start, long end) {
        return super.zrange(key, start, end);
    }

    @Override
    public Long zrem(String key, String... member) {
        return super.zrem(key, member);
    }

    @Override
    public Double zincrby(String key, double score, String member) {
        return super.zincrby(key, score, member);
    }

    @Override
    public Double zincrby(String key, double score, String member, ZIncrByParams params) {
        return super.zincrby(key, score, member, params);
    }

    @Override
    public Long zrank(String key, String member) {
        return super.zrank(key, member);
    }

    @Override
    public Long zrevrank(String key, String member) {
        return super.zrevrank(key, member);
    }

    @Override
    public Set<String> zrevrange(String key, long start, long end) {
        return super.zrevrange(key, start, end);
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        return super.zrangeWithScores(key, start, end);
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        return super.zrevrangeWithScores(key, start, end);
    }

    @Override
    public Long zcard(String key) {
        return super.zcard(key);
    }

    @Override
    public Double zscore(String key, String member) {
        return super.zscore(key, member);
    }

    @Override
    public List<String> sort(String key) {
        return super.sort(key);
    }

    @Override
    public List<String> sort(String key, SortingParams sortingParameters) {
        return super.sort(key, sortingParameters);
    }

    @Override
    public Long zcount(String key, double min, double max) {
        return super.zcount(key, min, max);
    }

    @Override
    public Long zcount(String key, String min, String max) {
        return super.zcount(key, min, max);
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) {
        return super.zrangeByScore(key, min, max);
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) {
        return super.zrangeByScore(key, min, max);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min) {
        return super.zrevrangeByScore(key, max, min);
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        return super.zrangeByScore(key, min, max, offset, count);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min) {
        return super.zrevrangeByScore(key, max, min);
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
        return super.zrangeByScore(key, min, max, offset, count);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return super.zrevrangeByScore(key, max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        return super.zrangeByScoreWithScores(key, min, max);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        return super.zrevrangeByScoreWithScores(key, max, min);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return super.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        return super.zrevrangeByScore(key, max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        return super.zrangeByScoreWithScores(key, min, max);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        return super.zrevrangeByScoreWithScores(key, max, min);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        return super.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return super.zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        return super.zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    @Override
    public Long zremrangeByRank(String key, long start, long end) {
        return super.zremrangeByRank(key, start, end);
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) {
        return super.zremrangeByScore(key, start, end);
    }

    @Override
    public Long zremrangeByScore(String key, String start, String end) {
        return super.zremrangeByScore(key, start, end);
    }

    @Override
    public Long zlexcount(String key, String min, String max) {
        return super.zlexcount(key, min, max);
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max) {
        return super.zrangeByLex(key, min, max);
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
        return super.zrangeByLex(key, min, max, offset, count);
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min) {
        return super.zrevrangeByLex(key, max, min);
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
        return super.zrevrangeByLex(key, max, min, offset, count);
    }

    @Override
    public Long zremrangeByLex(String key, String min, String max) {
        return super.zremrangeByLex(key, min, max);
    }

    @Override
    public Long linsert(String key, BinaryClient.LIST_POSITION where, String pivot, String value) {
        return super.linsert(key, where, pivot, value);
    }

    @Override
    public Long lpushx(String key, String... string) {
        return super.lpushx(key, string);
    }

    @Override
    public Long rpushx(String key, String... string) {
        return super.rpushx(key, string);
    }

    @Override
    public Long del(String key) {
        return super.del(key);
    }

    @Override
    public String echo(String string) {
        return super.echo(string);
    }

    @Override
    public Long bitcount(String key) {
        return super.bitcount(key);
    }

    @Override
    public Long bitcount(String key, long start, long end) {
        return super.bitcount(key, start, end);
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String key, String cursor) {
        return super.hscan(key, cursor);
    }

    @Override
    public ScanResult<String> sscan(String key, String cursor) {
        return super.sscan(key, cursor);
    }

    @Override
    public ScanResult<Tuple> zscan(String key, String cursor) {
        return super.zscan(key, cursor);
    }

    @Override
    public Long pfadd(String key, String... elements) {
        return super.pfadd(key, elements);
    }

    @Override
    public long pfcount(String key) {
        return super.pfcount(key);
    }

    @Override
    public List<String> blpop(int timeout, String key) {
        return super.blpop(timeout, key);
    }

    @Override
    public List<String> brpop(int timeout, String key) {
        return super.brpop(timeout, key);
    }

    @Override
    public Long del(String... keys) {
        return super.del(keys);
    }

    @Override
    public List<String> blpop(int timeout, String... keys) {
        return super.blpop(timeout, keys);
    }

    @Override
    public List<String> brpop(int timeout, String... keys) {
        return super.brpop(timeout, keys);
    }

    @Override
    public List<String> mget(String... keys) {
        return super.mget(keys);
    }

    @Override
    public String mset(String... keysvalues) {
        return super.mset(keysvalues);
    }

    @Override
    public Long msetnx(String... keysvalues) {
        return super.msetnx(keysvalues);
    }

    @Override
    public String rename(String oldkey, String newkey) {
        return super.rename(oldkey, newkey);
    }

    @Override
    public Long renamenx(String oldkey, String newkey) {
        return super.renamenx(oldkey, newkey);
    }

    @Override
    public String rpoplpush(String srckey, String dstkey) {
        return super.rpoplpush(srckey, dstkey);
    }

    @Override
    public Set<String> sdiff(String... keys) {
        return super.sdiff(keys);
    }

    @Override
    public Long sdiffstore(String dstkey, String... keys) {
        return super.sdiffstore(dstkey, keys);
    }

    @Override
    public Set<String> sinter(String... keys) {
        return super.sinter(keys);
    }

    @Override
    public Long sinterstore(String dstkey, String... keys) {
        return super.sinterstore(dstkey, keys);
    }

    @Override
    public Long smove(String srckey, String dstkey, String member) {
        return super.smove(srckey, dstkey, member);
    }

    @Override
    public Long sort(String key, SortingParams sortingParameters, String dstkey) {
        return super.sort(key, sortingParameters, dstkey);
    }

    @Override
    public Long sort(String key, String dstkey) {
        return super.sort(key, dstkey);
    }

    @Override
    public Set<String> sunion(String... keys) {
        return super.sunion(keys);
    }

    @Override
    public Long sunionstore(String dstkey, String... keys) {
        return super.sunionstore(dstkey, keys);
    }

    @Override
    public Long zinterstore(String dstkey, String... sets) {
        return super.zinterstore(dstkey, sets);
    }

    @Override
    public Long zinterstore(String dstkey, ZParams params, String... sets) {
        return super.zinterstore(dstkey, params, sets);
    }

    @Override
    public Long zunionstore(String dstkey, String... sets) {
        return super.zunionstore(dstkey, sets);
    }

    @Override
    public Long zunionstore(String dstkey, ZParams params, String... sets) {
        return super.zunionstore(dstkey, params, sets);
    }

    @Override
    public String brpoplpush(String source, String destination, int timeout) {
        return super.brpoplpush(source, destination, timeout);
    }

    @Override
    public Long publish(String channel, String message) {
        return super.publish(channel, message);
    }

    @Override
    public void subscribe(JedisPubSub jedisPubSub, String... channels) {
        super.subscribe(jedisPubSub, channels);
    }

    @Override
    public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
        super.psubscribe(jedisPubSub, patterns);
    }

    @Override
    public Long bitop(BitOP op, String destKey, String... srcKeys) {
        return super.bitop(op, destKey, srcKeys);
    }

    @Override
    public String pfmerge(String destkey, String... sourcekeys) {
        return super.pfmerge(destkey, sourcekeys);
    }

    @Override
    public long pfcount(String... keys) {
        return super.pfcount(keys);
    }

    @Override
    public Object eval(String script, int keyCount, String... params) {
        return super.eval(script, keyCount, params);
    }

    @Override
    public Object eval(String script, String key) {
        return super.eval(script, key);
    }

    @Override
    public Object eval(String script, List<String> keys, List<String> args) {
        return super.eval(script, keys, args);
    }

    @Override
    public Object evalsha(String sha1, int keyCount, String... params) {
        return super.evalsha(sha1, keyCount, params);
    }

    @Override
    public Object evalsha(String sha1, List<String> keys, List<String> args) {
        return super.evalsha(sha1, keys, args);
    }

    @Override
    public Object evalsha(String script, String key) {
        return super.evalsha(script, key);
    }

    @Override
    public Boolean scriptExists(String sha1, String key) {
        return super.scriptExists(sha1, key);
    }

    @Override
    public List<Boolean> scriptExists(String key, String... sha1) {
        return super.scriptExists(key, sha1);
    }

    @Override
    public String scriptLoad(String script, String key) {
        return super.scriptLoad(script, key);
    }

    public RediseAutoProxyCluster(HostAndPort master,HostAndPort senconde) {
        super(master);
        seconder = new JedisCluster(senconde);


    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public Map<String, JedisPool> getClusterNodes() {
        return super.getClusterNodes();
    }

    @Override
    public String set(byte[] key, byte[] value) {
        return super.set(key, value);
    }

    @Override
    public String set(byte[] key, byte[] value, SetParams params) {
        return super.set(key, value, params);
    }

    @Override
    public byte[] get(byte[] key) {
        return super.get(key);
    }

    @Override
    public Long exists(byte[]... keys) {
        return super.exists(keys);
    }

    @Override
    public Boolean exists(byte[] key) {
        return super.exists(key);
    }

    @Override
    public Long persist(byte[] key) {
        return super.persist(key);
    }

    @Override
    public String type(byte[] key) {
        return super.type(key);
    }

    @Override
    public Long expire(byte[] key, int seconds) {
        return super.expire(key, seconds);
    }

    @Override
    public Long pexpire(byte[] key, long milliseconds) {
        return super.pexpire(key, milliseconds);
    }

    @Override
    public Long expireAt(byte[] key, long unixTime) {
        return super.expireAt(key, unixTime);
    }

    @Override
    public Long pexpireAt(byte[] key, long millisecondsTimestamp) {
        return super.pexpireAt(key, millisecondsTimestamp);
    }

    @Override
    public Long ttl(byte[] key) {
        return super.ttl(key);
    }

    @Override
    public Boolean setbit(byte[] key, long offset, boolean value) {
        return super.setbit(key, offset, value);
    }

    @Override
    public Boolean setbit(byte[] key, long offset, byte[] value) {
        return super.setbit(key, offset, value);
    }

    @Override
    public Boolean getbit(byte[] key, long offset) {
        return super.getbit(key, offset);
    }

    @Override
    public Long setrange(byte[] key, long offset, byte[] value) {
        return super.setrange(key, offset, value);
    }

    @Override
    public byte[] getrange(byte[] key, long startOffset, long endOffset) {
        return super.getrange(key, startOffset, endOffset);
    }

    @Override
    public byte[] getSet(byte[] key, byte[] value) {
        return super.getSet(key, value);
    }

    @Override
    public Long setnx(byte[] key, byte[] value) {
        return super.setnx(key, value);
    }

    @Override
    public String setex(byte[] key, int seconds, byte[] value) {
        return super.setex(key, seconds, value);
    }

    @Override
    public Long decrBy(byte[] key, long integer) {
        return super.decrBy(key, integer);
    }

    @Override
    public Long decr(byte[] key) {
        return super.decr(key);
    }

    @Override
    public Long incrBy(byte[] key, long integer) {
        return super.incrBy(key, integer);
    }

    @Override
    public Double incrByFloat(byte[] key, double value) {
        return super.incrByFloat(key, value);
    }

    @Override
    public Long incr(byte[] key) {
        return super.incr(key);
    }

    @Override
    public Long append(byte[] key, byte[] value) {
        return super.append(key, value);
    }

    @Override
    public byte[] substr(byte[] key, int start, int end) {
        return super.substr(key, start, end);
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        return super.hset(key, field, value);
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        return super.hget(key, field);
    }

    @Override
    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        return super.hsetnx(key, field, value);
    }

    @Override
    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        return super.hmset(key, hash);
    }

    @Override
    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        return super.hmget(key, fields);
    }

    @Override
    public Long hincrBy(byte[] key, byte[] field, long value) {
        return super.hincrBy(key, field, value);
    }

    @Override
    public Double hincrByFloat(byte[] key, byte[] field, double value) {
        return super.hincrByFloat(key, field, value);
    }

    @Override
    public Boolean hexists(byte[] key, byte[] field) {
        return super.hexists(key, field);
    }

    @Override
    public Long hdel(byte[] key, byte[]... field) {
        return super.hdel(key, field);
    }

    @Override
    public Long hlen(byte[] key) {
        return super.hlen(key);
    }

    @Override
    public Set<byte[]> hkeys(byte[] key) {
        return super.hkeys(key);
    }

    @Override
    public Collection<byte[]> hvals(byte[] key) {
        return super.hvals(key);
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return super.hgetAll(key);
    }

    @Override
    public Long rpush(byte[] key, byte[]... args) {
        return super.rpush(key, args);
    }

    @Override
    public Long lpush(byte[] key, byte[]... args) {
        return super.lpush(key, args);
    }

    @Override
    public Long llen(byte[] key) {
        return super.llen(key);
    }

    @Override
    public List<byte[]> lrange(byte[] key, long start, long end) {
        return super.lrange(key, start, end);
    }

    @Override
    public String ltrim(byte[] key, long start, long end) {
        return super.ltrim(key, start, end);
    }

    @Override
    public byte[] lindex(byte[] key, long index) {
        return super.lindex(key, index);
    }

    @Override
    public String lset(byte[] key, long index, byte[] value) {
        return super.lset(key, index, value);
    }

    @Override
    public Long lrem(byte[] key, long count, byte[] value) {
        return super.lrem(key, count, value);
    }

    @Override
    public byte[] lpop(byte[] key) {
        return super.lpop(key);
    }

    @Override
    public byte[] rpop(byte[] key) {
        return super.rpop(key);
    }

    @Override
    public Long sadd(byte[] key, byte[]... member) {
        return super.sadd(key, member);
    }

    @Override
    public Set<byte[]> smembers(byte[] key) {
        return super.smembers(key);
    }

    @Override
    public Long srem(byte[] key, byte[]... member) {
        return super.srem(key, member);
    }

    @Override
    public byte[] spop(byte[] key) {
        return super.spop(key);
    }

    @Override
    public Set<byte[]> spop(byte[] key, long count) {
        return super.spop(key, count);
    }

    @Override
    public Long scard(byte[] key) {
        return super.scard(key);
    }

    @Override
    public Boolean sismember(byte[] key, byte[] member) {
        return super.sismember(key, member);
    }

    @Override
    public byte[] srandmember(byte[] key) {
        return super.srandmember(key);
    }

    @Override
    public Long strlen(byte[] key) {
        return super.strlen(key);
    }

    @Override
    public Long zadd(byte[] key, double score, byte[] member) {
        return super.zadd(key, score, member);
    }

    @Override
    public Long zadd(byte[] key, double score, byte[] member, ZAddParams params) {
        return super.zadd(key, score, member, params);
    }

    @Override
    public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
        return super.zadd(key, scoreMembers);
    }

    @Override
    public Long zadd(byte[] key, Map<byte[], Double> scoreMembers, ZAddParams params) {
        return super.zadd(key, scoreMembers, params);
    }

    @Override
    public Set<byte[]> zrange(byte[] key, long start, long end) {
        return super.zrange(key, start, end);
    }

    @Override
    public Long zrem(byte[] key, byte[]... member) {
        return super.zrem(key, member);
    }

    @Override
    public Double zincrby(byte[] key, double score, byte[] member) {
        return super.zincrby(key, score, member);
    }

    @Override
    public Double zincrby(byte[] key, double score, byte[] member, ZIncrByParams params) {
        return super.zincrby(key, score, member, params);
    }

    @Override
    public Long zrank(byte[] key, byte[] member) {
        return super.zrank(key, member);
    }

    @Override
    public Long zrevrank(byte[] key, byte[] member) {
        return super.zrevrank(key, member);
    }

    @Override
    public Set<byte[]> zrevrange(byte[] key, long start, long end) {
        return super.zrevrange(key, start, end);
    }

    @Override
    public Set<Tuple> zrangeWithScores(byte[] key, long start, long end) {
        return super.zrangeWithScores(key, start, end);
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(byte[] key, long start, long end) {
        return super.zrevrangeWithScores(key, start, end);
    }

    @Override
    public Long zcard(byte[] key) {
        return super.zcard(key);
    }

    @Override
    public Double zscore(byte[] key, byte[] member) {
        return super.zscore(key, member);
    }

    @Override
    public List<byte[]> sort(byte[] key) {
        return super.sort(key);
    }

    @Override
    public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
        return super.sort(key, sortingParameters);
    }

    @Override
    public Long zcount(byte[] key, double min, double max) {
        return super.zcount(key, min, max);
    }

    @Override
    public Long zcount(byte[] key, byte[] min, byte[] max) {
        return super.zcount(key, min, max);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        return super.zrangeByScore(key, min, max);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max) {
        return super.zrangeByScore(key, min, max);
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
        return super.zrevrangeByScore(key, max, min);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        return super.zrangeByScore(key, min, max, offset, count);
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
        return super.zrevrangeByScore(key, max, min);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
        return super.zrangeByScore(key, min, max, offset, count);
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
        return super.zrevrangeByScore(key, max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        return super.zrangeByScoreWithScores(key, min, max);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        return super.zrevrangeByScoreWithScores(key, max, min);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        return super.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
        return super.zrevrangeByScore(key, max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
        return super.zrangeByScoreWithScores(key, min, max);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
        return super.zrevrangeByScoreWithScores(key, max, min);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset, int count) {
        return super.zrangeByScoreWithScores(key, min, max, offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        return super.zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset, int count) {
        return super.zrevrangeByScoreWithScores(key, max, min, offset, count);
    }

    @Override
    public Long zremrangeByRank(byte[] key, long start, long end) {
        return super.zremrangeByRank(key, start, end);
    }

    @Override
    public Long zremrangeByScore(byte[] key, double start, double end) {
        return super.zremrangeByScore(key, start, end);
    }

    @Override
    public Long zremrangeByScore(byte[] key, byte[] start, byte[] end) {
        return super.zremrangeByScore(key, start, end);
    }

    @Override
    public Long linsert(byte[] key, BinaryClient.LIST_POSITION where, byte[] pivot, byte[] value) {
        return super.linsert(key, where, pivot, value);
    }

    @Override
    public Long lpushx(byte[] key, byte[]... arg) {
        return super.lpushx(key, arg);
    }

    @Override
    public Long rpushx(byte[] key, byte[]... arg) {
        return super.rpushx(key, arg);
    }

    @Override
    public Long del(byte[] key) {
        return super.del(key);
    }

    @Override
    public byte[] echo(byte[] arg) {
        return super.echo(arg);
    }

    @Override
    public Long bitcount(byte[] key) {
        return super.bitcount(key);
    }

    @Override
    public Long bitcount(byte[] key, long start, long end) {
        return super.bitcount(key, start, end);
    }

    @Override
    public Long pfadd(byte[] key, byte[]... elements) {
        return super.pfadd(key, elements);
    }

    @Override
    public long pfcount(byte[] key) {
        return super.pfcount(key);
    }

    @Override
    public List<byte[]> srandmember(byte[] key, int count) {
        return super.srandmember(key, count);
    }

    @Override
    public Long zlexcount(byte[] key, byte[] min, byte[] max) {
        return super.zlexcount(key, min, max);
    }

    @Override
    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max) {
        return super.zrangeByLex(key, min, max);
    }

    @Override
    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max, int offset, int count) {
        return super.zrangeByLex(key, min, max, offset, count);
    }

    @Override
    public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min) {
        return super.zrevrangeByLex(key, max, min);
    }

    @Override
    public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min, int offset, int count) {
        return super.zrevrangeByLex(key, max, min, offset, count);
    }

    @Override
    public Long zremrangeByLex(byte[] key, byte[] min, byte[] max) {
        return super.zremrangeByLex(key, min, max);
    }

    @Override
    public Object eval(byte[] script, byte[] keyCount, byte[]... params) {
        return super.eval(script, keyCount, params);
    }

    @Override
    public Object eval(byte[] script, int keyCount, byte[]... params) {
        return super.eval(script, keyCount, params);
    }

    @Override
    public Object eval(byte[] script, List<byte[]> keys, List<byte[]> args) {
        return super.eval(script, keys, args);
    }

    @Override
    public Object eval(byte[] script, byte[] key) {
        return super.eval(script, key);
    }

    @Override
    public Object evalsha(byte[] script, byte[] key) {
        return super.evalsha(script, key);
    }

    @Override
    public Object evalsha(byte[] sha1, List<byte[]> keys, List<byte[]> args) {
        return super.evalsha(sha1, keys, args);
    }

    @Override
    public Object evalsha(byte[] sha1, int keyCount, byte[]... params) {
        return super.evalsha(sha1, keyCount, params);
    }

    @Override
    public List<Long> scriptExists(byte[] key, byte[][] sha1) {
        return super.scriptExists(key, sha1);
    }

    @Override
    public byte[] scriptLoad(byte[] script, byte[] key) {
        return super.scriptLoad(script, key);
    }

    @Override
    public String scriptFlush(byte[] key) {
        return super.scriptFlush(key);
    }

    @Override
    public String scriptKill(byte[] key) {
        return super.scriptKill(key);
    }

    @Override
    public Long del(byte[]... keys) {
        return super.del(keys);
    }

    @Override
    public List<byte[]> blpop(int timeout, byte[]... keys) {
        return super.blpop(timeout, keys);
    }

    @Override
    public List<byte[]> brpop(int timeout, byte[]... keys) {
        return super.brpop(timeout, keys);
    }

    @Override
    public List<byte[]> mget(byte[]... keys) {
        return super.mget(keys);
    }

    @Override
    public String mset(byte[]... keysvalues) {
        return super.mset(keysvalues);
    }

    @Override
    public Long msetnx(byte[]... keysvalues) {
        return super.msetnx(keysvalues);
    }

    @Override
    public String rename(byte[] oldkey, byte[] newkey) {
        return super.rename(oldkey, newkey);
    }

    @Override
    public Long renamenx(byte[] oldkey, byte[] newkey) {
        return super.renamenx(oldkey, newkey);
    }

    @Override
    public byte[] rpoplpush(byte[] srckey, byte[] dstkey) {
        return super.rpoplpush(srckey, dstkey);
    }

    @Override
    public Set<byte[]> sdiff(byte[]... keys) {
        return super.sdiff(keys);
    }

    @Override
    public Long sdiffstore(byte[] dstkey, byte[]... keys) {
        return super.sdiffstore(dstkey, keys);
    }

    @Override
    public Set<byte[]> sinter(byte[]... keys) {
        return super.sinter(keys);
    }

    @Override
    public Long sinterstore(byte[] dstkey, byte[]... keys) {
        return super.sinterstore(dstkey, keys);
    }

    @Override
    public Long smove(byte[] srckey, byte[] dstkey, byte[] member) {
        return super.smove(srckey, dstkey, member);
    }

    @Override
    public Long sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
        return super.sort(key, sortingParameters, dstkey);
    }

    @Override
    public Long sort(byte[] key, byte[] dstkey) {
        return super.sort(key, dstkey);
    }

    @Override
    public Set<byte[]> sunion(byte[]... keys) {
        return super.sunion(keys);
    }

    @Override
    public Long sunionstore(byte[] dstkey, byte[]... keys) {
        return super.sunionstore(dstkey, keys);
    }

    @Override
    public Long zinterstore(byte[] dstkey, byte[]... sets) {
        return super.zinterstore(dstkey, sets);
    }

    @Override
    public Long zinterstore(byte[] dstkey, ZParams params, byte[]... sets) {
        return super.zinterstore(dstkey, params, sets);
    }

    @Override
    public Long zunionstore(byte[] dstkey, byte[]... sets) {
        return super.zunionstore(dstkey, sets);
    }

    @Override
    public Long zunionstore(byte[] dstkey, ZParams params, byte[]... sets) {
        return super.zunionstore(dstkey, params, sets);
    }

    @Override
    public byte[] brpoplpush(byte[] source, byte[] destination, int timeout) {
        return super.brpoplpush(source, destination, timeout);
    }

    @Override
    public Long publish(byte[] channel, byte[] message) {
        return super.publish(channel, message);
    }

    @Override
    public void subscribe(BinaryJedisPubSub jedisPubSub, byte[]... channels) {
        super.subscribe(jedisPubSub, channels);
    }

    @Override
    public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
        super.psubscribe(jedisPubSub, patterns);
    }

    @Override
    public Long bitop(BitOP op, byte[] destKey, byte[]... srcKeys) {
        return super.bitop(op, destKey, srcKeys);
    }

    @Override
    public String pfmerge(byte[] destkey, byte[]... sourcekeys) {
        return super.pfmerge(destkey, sourcekeys);
    }

    @Override
    public Long pfcount(byte[]... keys) {
        return super.pfcount(keys);
    }

    public RediseAutoProxyCluster(){
        this(null);
    }

    public List<SwitchRule> getRules() {
        return rules;
    }

    public void setRules(List<SwitchRule> rules) {
        this.rules = rules;
    }
}
