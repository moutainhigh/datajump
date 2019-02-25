package com.ai.ds.dj.rdb.parse;
import com.ai.ds.dj.datatype.ContextKeyValuePair;
import com.ai.ds.dj.datatype.KeyValuePair;

import com.ai.ds.dj.message.Event;
import com.ai.ds.dj.rdb.io.RedisInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import static com.ai.ds.dj.rdb.Constants.*;

/**
 * rdb解析
 * @author wuwenhui
 */
public class RdbParser {

    private   RedisInputStream in;
    private   RdbVisitor rdbVisitor=null;
    protected static final Logger logger = LoggerFactory.getLogger(RdbParser.class);

    public RdbParser(RedisInputStream in,RdbVisitor rdbVisitor) {
        this.in = in;
        this.rdbVisitor = rdbVisitor;

    }

    /**
     * The RDB E-BNF
     * <p>
     * RDB        =    'REDIS', $version, [AUX], [MODULE_AUX], {SELECTDB, [RESIZEDB], {RECORD}}, '0xFF', [$checksum];
     * <p>
     * RECORD     =    [EXPIRED], [IDLE | FREQ], KEY, VALUE;
     * <p>
     * SELECTDB   =    '0xFE', $length;
     * <p>
     * AUX        =    {'0xFA', $string, $string};            (*Introduced in rdb version 7*)
     * <p>
     * MODULE_AUX =    {'0xF7', $length};                     (*Introduced in rdb version 9*)
     * <p>
     * RESIZEDB   =    '0xFB', $length, $length;              (*Introduced in rdb version 7*)
     * <p>
     * EXPIRED    =    ('0xFD', $second) | ('0xFC', $millisecond);
     * <p>
     * IDLE       =    {'0xF8', $value-type};                 (*Introduced in rdb version 9*)
     * <p>
     * FREQ       =    {'0xF9', $length};                     (*Introduced in rdb version 9*)
     * <p>
     * KEY        =    $string;
     * <p>
     * VALUE      =    $value-type, ( $string
     * <p>
     * | $list
     * <p>
     * | $set
     * <p>
     * | $zset
     * <p>
     * | $hash
     * <p>
     * | $zset2                  (*Introduced in rdb version 8*)
     * <p>
     * | $module                 (*Introduced in rdb version 8*)
     * <p>
     * | $module2                (*Introduced in rdb version 8*)
     * <p>
     * | $hashzipmap
     * <p>
     * | $listziplist
     * <p>
     * | $setintset
     * <p>
     * | $zsetziplist
     * <p>
     * | $hashziplist
     * <p>
     * | $listquicklist          (*Introduced in rdb version 7*)
     * <p>
     * | $streamlistpacks);      (*Introduced in rdb version 9*)
     * <p>
     *
     * @return read bytes
     * @throws IOException when read timeout
     */
    public long parse() throws IOException {

        if(logger.isDebugEnabled()){
            logger.debug("开始解析RDB文件");
        }

        /*
         * ----------------------------
         * 52 45 44 49 53              # Magic String "REDIS"
         * 30 30 30 37                 # RDB Version Number in big endian. In this case, version = 0007 = 7
         * ----------------------------
         */


        rdbVisitor.applyMagic(in);
        int version = rdbVisitor.applyVersion(in);

        if(logger.isDebugEnabled()){
            logger.debug("版本号为：{}",version);
        }

        /*
         * rdb
         */
        loop:
        while (true) {
            Event event = null;
            int type = in.read();
            ContextKeyValuePair kv = new ContextKeyValuePair();
            switch (type) {
                case RDB_OPCODE_EXPIRETIME:
                    event = rdbVisitor.applyExpireTime(in, version, kv);
                    break;
                case RDB_OPCODE_EXPIRETIME_MS:
                    event =rdbVisitor.applyExpireTimeMs(in, version, kv);
                    break;
                case RDB_OPCODE_FREQ:
                    event =rdbVisitor.applyFreq(in, version, kv);
                    break;
                case RDB_OPCODE_IDLE:
                    event =rdbVisitor.applyIdle(in, version, kv);
                    break;
                case RDB_OPCODE_AUX:
                    event = rdbVisitor.applyAux(in, version);
                    break;
                case RDB_OPCODE_MODULE_AUX:
                    event = rdbVisitor.applyModuleAux(in, version);
                    break;
                case RDB_OPCODE_RESIZEDB:
                    rdbVisitor.applyResizeDB(in, version, kv);
                    break;
                case RDB_OPCODE_SELECTDB:
                    rdbVisitor.applySelectDB(in, version);
                    break;
                case RDB_OPCODE_EOF:
                    long checksum = rdbVisitor.applyEof(in, version);
//                    this.replicator.submitEvent(new PostRdbSyncEvent(checksum));
                    break loop;
                case RDB_TYPE_STRING:
                    event =rdbVisitor.applyString(in, version, kv);
                    break;
                case RDB_TYPE_LIST:
                    event =rdbVisitor.applyList(in, version, kv);
                    break;
                case RDB_TYPE_SET:
                    event = rdbVisitor.applySet(in, version, kv);
                    break;
                case RDB_TYPE_ZSET:
                    event = rdbVisitor.applyZSet(in, version, kv);
                    break;
                case RDB_TYPE_ZSET_2:
                    event = rdbVisitor.applyZSet2(in, version, kv);
                    break;
                case RDB_TYPE_HASH:
                    event = rdbVisitor.applyHash(in, version, kv);
                    break;
                case RDB_TYPE_HASH_ZIPMAP:
                    event = rdbVisitor.applyHashZipMap(in, version, kv);
                    break;
                case RDB_TYPE_LIST_ZIPLIST:
                    event = rdbVisitor.applyListZipList(in, version, kv);
                    break;
                case RDB_TYPE_SET_INTSET:
                    event = rdbVisitor.applySetIntSet(in, version, kv);
                    break;
                case RDB_TYPE_ZSET_ZIPLIST:
                    event = rdbVisitor.applyZSetZipList(in, version, kv);
                    break;
                case RDB_TYPE_HASH_ZIPLIST:
                    event = rdbVisitor.applyHashZipList(in, version, kv);
                    break;
                case RDB_TYPE_LIST_QUICKLIST:
                    event = rdbVisitor.applyListQuickList(in, version, kv);
                    break;
                case RDB_TYPE_MODULE:
                    event = rdbVisitor.applyModule(in, version, kv);
                    break;
                case RDB_TYPE_MODULE_2:
                    event = rdbVisitor.applyModule2(in, version, kv);
                    break;
                case RDB_TYPE_STREAM_LISTPACKS:
                    event = rdbVisitor.applyStreamListPacks(in, version, kv);
                    break;
                default:
                    throw new AssertionError("unexpected value type:" + type + ", check your ModuleParser or ValueIterableRdbVisitor.");
            }

            if( event instanceof KeyValuePair){
                KeyValuePair str = (KeyValuePair)event;
                logger.debug("事件处理key->{} value={}",new String((byte[])str.getKey()));
            }

        }

        if(logger.isDebugEnabled()){
            logger.debug("处理rdb 结束");
        }
        return in.total();
    }
}

