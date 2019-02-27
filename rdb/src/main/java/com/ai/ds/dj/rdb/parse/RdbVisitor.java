package com.ai.ds.dj.rdb.parse;

import com.ai.ds.dj.datatype.ContextKeyValuePair;
import com.ai.ds.dj.datatype.DB;

import com.ai.ds.dj.datatype.Event;
import com.ai.ds.dj.rdb.io.RedisInputStream;

import java.io.IOException;

/**
 * 处理rbd文件访问者：rbd解析到对应的方法时触发对应的方法进行解析
 * author:wuwh6
 */
public interface  RdbVisitor {

    /*
     * 解析魔术字
     */
    public String applyMagic(RedisInputStream in) throws IOException ;

    /**
     * 解析版本
     * @param in
     * @return
     * @throws IOException
     */
    public int applyVersion(RedisInputStream in) throws IOException ;

    /**
     * 获取类型
     * @param in
     * @return
     * @throws IOException
     */
    public int applyType(RedisInputStream in) throws IOException ;


    /*
     * 获取select数据库命令
     */
    public DB applySelectDB(RedisInputStream in, int version) throws IOException ;

    /**
     * 数据库大小重制
     * @param in
     * @param version
     * @param context
     * @return
     * @throws IOException
     */
    public DB applyResizeDB(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    /*
     * checksum 结束后crc校验
     */
    public long applyEof(RedisInputStream in, int version) throws IOException ;

    /*
     * aux
     */
    public Event applyAux(RedisInputStream in, int version) throws IOException ;

    public Event applyModuleAux(RedisInputStream in, int version) throws IOException ;

    /**
     * 过期时间秒
     * @param in
     * @param version
     * @param context
     * @return
     * @throws IOException
     */
    public Event applyExpireTime(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    /**
     * 过期时间毫秒
     * @param in
     * @param version
     * @param context
     * @return
     * @throws IOException
     */
    public Event applyExpireTimeMs(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    public Event applyFreq(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    public Event applyIdle(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    /**
     * 获取String对象
     * @param in
     * @param version
     * @param context
     * @return
     * @throws IOException
     */
    public Event applyString(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    /**
     * 获取list对象
     * @param in
     * @param version
     * @param context
     * @return
     * @throws IOException
     */
    public Event applyList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    /**
     * 解析set对象
     * @param in
     * @param version
     * @param context
     * @return
     * @throws IOException
     */
    public Event applySet(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    /**
     * 解析zset对象
     * @param in
     * @param version
     * @param context
     * @return
     * @throws IOException
     */
    public Event applyZSet(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    /**
     *
     * @param in
     * @param version
     * @param context
     * @return
     * @throws IOException
     */
    public Event applyZSet2(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    /**
     * 解析hash对象
     * @param in
     * @param version
     * @param context
     * @return
     * @throws IOException
     */
    public Event applyHash(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    /**
     * 解析zip对象
     * @param in
     * @param version
     * @param context
     * @return
     * @throws IOException
     */
    public Event applyHashZipMap(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    /**
     * ListZipList
     * @param in
     * @param version
     * @param context
     * @return
     * @throws IOException
     */
    public Event applyListZipList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    public Event applySetIntSet(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    public Event applyZSetZipList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    public Event applyHashZipList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    public Event applyListQuickList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    public Event applyModule(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    public Event applyModule2(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;

    public Event applyStreamListPacks(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException ;
}
