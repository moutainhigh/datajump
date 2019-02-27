
package com.ai.ds.dj.rdb.parse;
import com.ai.ds.dj.datatype.*;
import com.ai.ds.dj.datatype.Event;
import com.ai.ds.dj.rdb.io.RedisInputStream;
import com.ai.ds.dj.rdb.skip.SkipRdbParser;
import com.ai.ds.dj.rdb.util.ByteArrayList;
import com.ai.ds.dj.rdb.util.ByteArrayMap;
import com.ai.ds.dj.rdb.util.ByteArraySet;
import com.ai.ds.dj.rdb.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;
import static com.ai.ds.dj.rdb.Constants.*;
import static com.ai.ds.dj.rdb.parse.BaseRdbParser.StringHelper.listPackEntry;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

/**
 * 默认处理器
 */
public class DefaultRdbVisitor implements RdbVisitor {

    protected static final Logger logger = LoggerFactory.getLogger(DefaultRdbVisitor.class);


    @Override
    public String applyMagic(RedisInputStream in) throws IOException {
        String magic = BaseRdbParser.StringHelper.str(in, 5);//REDIS
        if (!magic.equals("REDIS")) {
            throw new UnsupportedOperationException("can't read MAGIC STRING [REDIS] ,value:" + magic);
        }
        return magic;
    }

    @Override
    public int applyVersion(RedisInputStream in) throws IOException {
        int version = parseInt(BaseRdbParser.StringHelper.str(in, 4));
        if (version < 2 || version > 9) {
            throw new UnsupportedOperationException(String.valueOf("can't handle RDB format version " + version));
        }
        return version;
    }

    @Override
    public int applyType(RedisInputStream in) throws IOException {
        return in.read();
    }

    @Override
    public DB applySelectDB(RedisInputStream in, int version) throws IOException {
        /*
         * ----------------------------
         * FE $length-encoding         # Previous db ends, next db starts. Database number read using length encoding.
         * ----------------------------
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        long dbNumber = parser.rdbLoadLen().len;
        return new DB(dbNumber);
    }

    @Override
    public DB applyResizeDB(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        long dbsize = parser.rdbLoadLen().len;
        long expiresSize = parser.rdbLoadLen().len;
        DB db = context.getDb();
        if (db != null) db.setDbsize(dbsize);
        if (db != null) db.setExpires(expiresSize);
        return db;
    }

    @Override
    public Event applyAux(RedisInputStream in, int version) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        String auxKey = Strings.toString(parser.rdbLoadEncodedStringObject().first());
        String auxValue = Strings.toString(parser.rdbLoadEncodedStringObject().first());
        if (!auxKey.startsWith("%")) {
            if (logger.isInfoEnabled()) {
                logger.info("RDB {}: {}", auxKey, auxValue);
            }

//            if (auxKey.equals("repl-id")) setReplId(auxValue);
//            if (auxKey.equals("repl-offset")) setReplOffset(parseLong(auxValue));
//            if (auxKey.equals("repl-stream-db")) setReplStreamDB(parseInt(auxValue));
            return new AuxField(auxKey, auxValue);
        } else {
            if (logger.isWarnEnabled()) {
                logger.warn("unrecognized RDB AUX field: {}, value: {}", auxKey, auxValue);
            }
            return null;
        }
    }

    @Override
    public Event applyModuleAux(RedisInputStream in, int version) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadLen();
        parser.rdbLoadCheckModuleValue();
        return null;
    }

    @Override
    public long applyEof(RedisInputStream in, int version) throws IOException {
        /*
         * ----------------------------
         * ...                         # Key value pairs for this database, additonal database
         * FF                          ## End of RDB file indicator
         * 8 byte checksum             ## CRC 64 checksum of the entire file.
         * ----------------------------
         */
        if (version >= 5) return in.readLong(8);
        return 0L;
    }

    @Override
    public Event applyExpireTime(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        /*
         * ----------------------------
         * FD $unsigned int            # FD indicates "expiry time in seconds". After that, expiry time is read as a 4 byte unsigned int
         * $value-type                 # 1 byte flag indicating the type of value - set, map, sorted set etc.
         * $string-encoded-name         # The name, encoded as a redis string
         * $encoded-value              # The value. Encoding depends on $value-type
         * ----------------------------
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        long expiredSec = parser.rdbLoadTime();
        int type = applyType(in);
        context.setExpiredType(ExpiredType.SECOND);
        context.setExpiredValue(expiredSec);
        context.setValueRdbType(type);
        KeyValuePair<?, ?> kv;
        if (type == RDB_OPCODE_FREQ) {
            kv = (KeyValuePair<?, ?>) applyFreq(in, version, context);
        } else if (type == RDB_OPCODE_IDLE) {
            kv = (KeyValuePair<?, ?>) applyIdle(in, version, context);
        } else {
            kv = rdbLoadObject(in, version, context);
        }
        return kv;
    }

    @Override
    public Event applyExpireTimeMs(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        /*
         * ----------------------------
         * FC $unsigned long           # FC indicates "expiry time in ms". After that, expiry time is read as a 8 byte unsigned long
         * $value-type                 # 1 byte flag indicating the type of value - set, map, sorted set etc.
         * $string-encoded-name         # The name, encoded as a redis string
         * $encoded-value              # The value. Encoding depends on $value-type
         * ----------------------------
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        long expiredMs = parser.rdbLoadMillisecondTime();
        int type = applyType(in);
        context.setExpiredType(ExpiredType.MS);
        context.setExpiredValue(expiredMs);
        context.setValueRdbType(type);
        KeyValuePair<?, ?> kv;
        if (type == RDB_OPCODE_FREQ) {
            kv = (KeyValuePair<?, ?>) applyFreq(in, version, context);
        } else if (type == RDB_OPCODE_IDLE) {
            kv = (KeyValuePair<?, ?>) applyIdle(in, version, context);
        } else {
            kv = rdbLoadObject(in, version, context);
        }
        return kv;
    }

    @Override
    public Event applyFreq(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        long lfuFreq = in.read();
        int valueType = applyType(in);
        context.setValueRdbType(valueType);
        context.setEvictType(EvictType.LFU);
        context.setEvictValue(lfuFreq);
        KeyValuePair<?, ?> kv = rdbLoadObject(in, version, context);
        return kv;
    }

    @Override
    public Event applyIdle(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        long lruIdle = parser.rdbLoadLen().len;
        int valueType = applyType(in);
        context.setValueRdbType(valueType);
        context.setEvictType(EvictType.LRU);
        context.setEvictValue(lruIdle);
        KeyValuePair<?, ?> kv = rdbLoadObject(in, version, context);
        return kv;
    }

    @Override
    public Event applyString(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        /*
         * |       <content>       |
         * |    string contents    |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], byte[]> o0 = new KeyStringValueString();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        byte[] val = parser.rdbLoadEncodedStringObject().first();
        o0.setValueRdbType(RDB_TYPE_STRING);
        o0.setValue(val);
        o0.setKey(key);
        return context.valueOf(o0);
    }

    @Override
    public Event applyList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        /*
         * |    <len>     |       <content>       |
         * | 1 or 5 bytes |    string contents    |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], List<byte[]>> o1 = new KeyStringValueList();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ByteArrayList();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            list.add(element);
            len--;
        }
        o1.setValueRdbType(RDB_TYPE_LIST);
        o1.setValue(list);
        o1.setKey(key);
        return context.valueOf(o1);
    }

    @Override
    public Event applySet(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        /*
         * |    <len>     |       <content>       |
         * | 1 or 5 bytes |    string contents    |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], Set<byte[]>> o2 = new KeyStringValueSet();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        long len = parser.rdbLoadLen().len;
        Set<byte[]> set = new ByteArraySet();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            set.add(element);
            len--;
        }
        o2.setValueRdbType(RDB_TYPE_SET);
        o2.setValue(set);
        o2.setKey(key);
        return context.valueOf(o2);
    }

    @Override
    public Event applyZSet(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        /*
         * |    <len>     |       <content>       |        <score>       |
         * | 1 or 5 bytes |    string contents    |    double content    |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], Set<ZSetEntry>> o3 = new KeyStringValueZSet();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        long len = parser.rdbLoadLen().len;
        Set<ZSetEntry> zset = new LinkedHashSet<>();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            double score = parser.rdbLoadDoubleValue();
            zset.add(new ZSetEntry(element, score));
            len--;
        }
        o3.setValueRdbType(RDB_TYPE_ZSET);
        o3.setValue(zset);
        o3.setKey(key);
        return context.valueOf(o3);
    }

    @Override
    public Event applyZSet2(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        /*
         * |    <len>     |       <content>       |        <score>       |
         * | 1 or 5 bytes |    string contents    |    binary double     |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], Set<ZSetEntry>> o5 = new KeyStringValueZSet();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        /* rdb version 8*/
        long len = parser.rdbLoadLen().len;
        Set<ZSetEntry> zset = new LinkedHashSet<>();
        while (len > 0) {
            byte[] element = parser.rdbLoadEncodedStringObject().first();
            double score = parser.rdbLoadBinaryDoubleValue();
            zset.add(new ZSetEntry(element, score));
            len--;
        }
        o5.setValueRdbType(RDB_TYPE_ZSET_2);
        o5.setValue(zset);
        o5.setKey(key);
        return context.valueOf(o5);
    }

    @Override
    public Event applyHash(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        /*
         * |    <len>     |       <content>       |
         * | 1 or 5 bytes |    string contents    |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], Map<byte[], byte[]>> o4 = new KeyStringValueHash();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        long len = parser.rdbLoadLen().len;
        ByteArrayMap map = new ByteArrayMap();
        while (len > 0) {
            byte[] field = parser.rdbLoadEncodedStringObject().first();
            byte[] value = parser.rdbLoadEncodedStringObject().first();
            map.put(field, value);
            len--;
        }
        o4.setValueRdbType(RDB_TYPE_HASH);
        o4.setValue(map);
        o4.setKey(key);
        return context.valueOf(o4);
    }

    @Override
    public Event applyHashZipMap(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        /*
         * |<zmlen> |   <len>     |"foo"    |    <len>   | <free> |   "bar" |<zmend> |
         * | 1 byte | 1 or 5 byte | content |1 or 5 byte | 1 byte | content | 1 byte |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], Map<byte[], byte[]>> o9 = new KeyStringValueHash();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());
        ByteArrayMap map = new ByteArrayMap();
        BaseRdbParser.LenHelper.zmlen(stream); // zmlen
        while (true) {
            int zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
            if (zmEleLen == 255) {
                o9.setValueRdbType(RDB_TYPE_HASH_ZIPMAP);
                o9.setValue(map);
                o9.setKey(key);
                return context.valueOf(o9);
            }
            byte[] field = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
            zmEleLen = BaseRdbParser.LenHelper.zmElementLen(stream);
            if (zmEleLen == 255) {
                //value is null
                map.put(field, null);
                o9.setValueRdbType(RDB_TYPE_HASH_ZIPMAP);
                o9.setValue(map);
                o9.setKey(key);
                return context.valueOf(o9);
            }
            int free = BaseRdbParser.LenHelper.free(stream);
            byte[] value = BaseRdbParser.StringHelper.bytes(stream, zmEleLen);
            BaseRdbParser.StringHelper.skip(stream, free);
            map.put(field, value);
        }
    }

    @Override
    public Event applyListZipList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        /*
         * |<zlbytes>| <zltail>| <zllen>| <entry> ...<entry> | <zlend>|
         * | 4 bytes | 4 bytes | 2bytes | zipListEntry ...   | 1byte  |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], List<byte[]>> o10 = new KeyStringValueList();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());

        List<byte[]> list = new ByteArrayList();
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        for (int i = 0; i < zllen; i++) {
            byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
            list.add(e);
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        o10.setValueRdbType(RDB_TYPE_LIST_ZIPLIST);
        o10.setValue(list);
        o10.setKey(key);
        return context.valueOf(o10);
    }

    @Override
    public Event applySetIntSet(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        /*
         * |<encoding>| <length-of-contents>|              <contents>                            |
         * | 4 bytes  |            4 bytes  | 2 bytes element| 4 bytes element | 8 bytes element |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], Set<byte[]>> o11 = new KeyStringValueSet();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());

        Set<byte[]> set = new ByteArraySet();
        int encoding = BaseRdbParser.LenHelper.encoding(stream);
        long lenOfContent = BaseRdbParser.LenHelper.lenOfContent(stream);
        for (long i = 0; i < lenOfContent; i++) {
            switch (encoding) {
                case 2:
                    String element = String.valueOf(stream.readInt(2));
                    set.add(element.getBytes());
                    break;
                case 4:
                    element = String.valueOf(stream.readInt(4));
                    set.add(element.getBytes());
                    break;
                case 8:
                    element = String.valueOf(stream.readLong(8));
                    set.add(element.getBytes());
                    break;
                default:
                    throw new AssertionError("expect encoding [2,4,8] but:" + encoding);
            }
        }
        o11.setValueRdbType(RDB_TYPE_SET_INTSET);
        o11.setValue(set);
        o11.setKey(key);
        return context.valueOf(o11);
    }

    @Override
    public Event applyZSetZipList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        /*
         * |<zlbytes>| <zltail>| <zllen>| <entry> ...<entry> | <zlend>|
         * | 4 bytes | 4 bytes | 2bytes | zipListEntry ...   | 1byte  |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], Set<ZSetEntry>> o12 = new KeyStringValueZSet();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());

        Set<ZSetEntry> zset = new LinkedHashSet<>();
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        while (zllen > 0) {
            byte[] element = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            double score = Double.valueOf(Strings.toString(BaseRdbParser.StringHelper.zipListEntry(stream)));
            zllen--;
            zset.add(new ZSetEntry(element, score));
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        o12.setValueRdbType(RDB_TYPE_ZSET_ZIPLIST);
        o12.setValue(zset);
        o12.setKey(key);
        return context.valueOf(o12);
    }

    @Override
    public Event applyHashZipList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        /*
         * |<zlbytes>| <zltail>| <zllen>| <entry> ...<entry> | <zlend>|
         * | 4 bytes | 4 bytes | 2bytes | zipListEntry ...   | 1byte  |
         */
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], Map<byte[], byte[]>> o13 = new KeyStringValueHash();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        RedisInputStream stream = new RedisInputStream(parser.rdbLoadPlainStringObject());

        ByteArrayMap map = new ByteArrayMap();
        BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
        BaseRdbParser.LenHelper.zltail(stream); // zltail
        int zllen = BaseRdbParser.LenHelper.zllen(stream);
        while (zllen > 0) {
            byte[] field = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            byte[] value = BaseRdbParser.StringHelper.zipListEntry(stream);
            zllen--;
            map.put(field, value);
        }
        int zlend = BaseRdbParser.LenHelper.zlend(stream);
        if (zlend != 255) {
            throw new AssertionError("zlend expect 255 but " + zlend);
        }
        o13.setValueRdbType(RDB_TYPE_HASH_ZIPLIST);
        o13.setValue(map);
        o13.setKey(key);
        return context.valueOf(o13);
    }

    @Override
    public Event applyListQuickList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], List<byte[]>> o14 = new KeyStringValueList();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        long len = parser.rdbLoadLen().len;
        List<byte[]> list = new ByteArrayList();
        for (long i = 0; i < len; i++) {
            RedisInputStream stream = new RedisInputStream(parser.rdbGenericLoadStringObject(RDB_LOAD_NONE));

            BaseRdbParser.LenHelper.zlbytes(stream); // zlbytes
            BaseRdbParser.LenHelper.zltail(stream); // zltail
            int zllen = BaseRdbParser.LenHelper.zllen(stream);
            for (int j = 0; j < zllen; j++) {
                byte[] e = BaseRdbParser.StringHelper.zipListEntry(stream);
                list.add(e);
            }
            int zlend = BaseRdbParser.LenHelper.zlend(stream);
            if (zlend != 255) {
                throw new AssertionError("zlend expect 255 but " + zlend);
            }
        }
        o14.setValueRdbType(RDB_TYPE_LIST_QUICKLIST);
        o14.setValue(list);
        o14.setKey(key);
        return context.valueOf(o14);
    }

    @Override
    public Event applyModule(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        //|6|6|6|6|6|6|6|6|6|10|
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], Module> o6 = new KeyStringValueModule();
        byte[] key = parser.rdbLoadEncodedStringObject().first();
        char[] c = new char[9];
        long moduleid = parser.rdbLoadLen().len;
        for (int i = 0; i < c.length; i++) {
            c[i] = MODULE_SET[(int) (moduleid >>> (10 + (c.length - 1 - i) * 6) & 63)];
        }
        String moduleName = new String(c);
        int moduleVersion = (int) (moduleid & 1023);
        ModuleParser<Module> moduleParser = (ModuleParser<Module>) lookupModuleParser(moduleName, moduleVersion);
        if (moduleParser == null) {
            throw new NoSuchElementException("module parser[" + moduleName + ", " + moduleVersion + "] not register. rdb type: [RDB_TYPE_MODULE]");
        }
        o6.setValueRdbType(RDB_TYPE_MODULE);
        o6.setValue(moduleParser.parse(in, 1));
        o6.setKey(key);
        return context.valueOf(o6);
    }

    @Override
    public Event applyModule2(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        //|6|6|6|6|6|6|6|6|6|10|
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], Module> o7 = new KeyStringValueModule();
        byte[] rawKey = parser.rdbLoadEncodedStringObject().first();
        char[] c = new char[9];
        long moduleid = parser.rdbLoadLen().len;
        for (int i = 0; i < c.length; i++) {
            c[i] = MODULE_SET[(int) (moduleid >>> (10 + (c.length - 1 - i) * 6) & 63)];
        }
        String moduleName = new String(c);
        int moduleVersion = (int) (moduleid & 1023);
        ModuleParser<? extends Module> moduleParser = lookupModuleParser(moduleName, moduleVersion);
        Module module = null;
        if (moduleParser == null) {
            logger.warn("module parser[{}, {}] not register. rdb type: [RDB_TYPE_MODULE_2]. key: [{}]. module parse skipped.", moduleName, moduleVersion, Strings.toString(rawKey));
            SkipRdbParser skipRdbParser = new SkipRdbParser(in);
            skipRdbParser.rdbLoadCheckModuleValue();
        } else {
            module = moduleParser.parse(in, 2);
            long eof = parser.rdbLoadLen().len;
            if (eof != RDB_MODULE_OPCODE_EOF) {
                throw new UnsupportedOperationException("The RDB file contains module data for the module '" + moduleName + "' that is not terminated by the proper module value EOF marker");
            }
        }
        o7.setValueRdbType(RDB_TYPE_MODULE_2);
        o7.setValue(module);
        o7.setKey(rawKey);
        return context.valueOf(o7);
    }

    protected ModuleParser<? extends Module> lookupModuleParser(String moduleName, int moduleVersion) {
//        return replicator.getModuleParser(moduleName, moduleVersion);
        return null;
    }

    @Override
    @SuppressWarnings("resource")
    public Event applyStreamListPacks(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        BaseRdbParser parser = new BaseRdbParser(in);
        KeyValuePair<byte[], Stream> o15 = new KeyStringValueStream();
        byte[] key = parser.rdbLoadEncodedStringObject().first();

        Stream stream = new Stream();

        // Entries
        NavigableMap<Stream.ID, Stream.Entry> entries = new TreeMap<>(Stream.ID.COMPARATOR);
        long listPacks = parser.rdbLoadLen().len;
        while (listPacks-- > 0) {
            RedisInputStream rawId = new RedisInputStream(parser.rdbLoadPlainStringObject());
            Stream.ID baseId = new Stream.ID(rawId.readLong(8, false), rawId.readLong(8, false));
            RedisInputStream listPack = new RedisInputStream(parser.rdbLoadPlainStringObject());
            listPack.skip(4); // total-bytes
            listPack.skip(2); // num-elements
            /*
             * Master entry
             * +-------+---------+------------+---------+--/--+---------+---------+-+
             * | count | deleted | num-fields | field_1 | field_2 | ... | field_N |0|
             * +-------+---------+------------+---------+--/--+---------+---------+-+
             */
            long count = Long.parseLong(Strings.toString(listPackEntry(listPack))); // count
            long deleted = Long.parseLong(Strings.toString(listPackEntry(listPack))); // deleted
            int numFields = Integer.parseInt(Strings.toString(listPackEntry(listPack))); // num-fields
            byte[][] tempFields = new byte[numFields][];
            for (int i = 0; i < numFields; i++) {
                tempFields[i] = listPackEntry(listPack);
            }
            listPackEntry(listPack); // 0

            long total = count + deleted;
            while (total-- > 0) {
                Map<byte[], byte[]> fields = new ByteArrayMap();
                /*
                 * FLAG
                 * +-----+--------+
                 * |flags|entry-id|
                 * +-----+--------+
                 */
                int flag = Integer.parseInt(Strings.toString(listPackEntry(listPack)));
                long ms = Long.parseLong(Strings.toString(listPackEntry(listPack)));
                long seq = Long.parseLong(Strings.toString(listPackEntry(listPack)));
                Stream.ID id = baseId.delta(ms, seq);
                boolean delete = (flag & STREAM_ITEM_FLAG_DELETED) != 0;
                if ((flag & STREAM_ITEM_FLAG_SAMEFIELDS) != 0) {
                    /*
                     * SAMEFIELD
                     * +-------+-/-+-------+--------+
                     * |value-1|...|value-N|lp-count|
                     * +-------+-/-+-------+--------+
                     */
                    for (int i = 0; i < numFields; i++) {
                        byte[] value = listPackEntry(listPack);
                        byte[] field = tempFields[i];
                        fields.put(field, value);
                    }
                    entries.put(id, new Stream.Entry(id, delete, fields));
                } else {
                    /*
                     * NONEFIELD
                     * +----------+-------+-------+-/-+-------+-------+--------+
                     * |num-fields|field-1|value-1|...|field-N|value-N|lp-count|
                     * +----------+-------+-------+-/-+-------+-------+--------+
                     */
                    numFields = Integer.parseInt(Strings.toString(listPackEntry(listPack)));
                    for (int i = 0; i < numFields; i++) {
                        byte[] field = listPackEntry(listPack);
                        byte[] value = listPackEntry(listPack);
                        fields.put(field, value);
                    }
                    entries.put(id, new Stream.Entry(id, delete, fields));
                }
                listPackEntry(listPack); // lp-count
            }
            int lpend = listPack.read(); // lp-end
            if (lpend != 255) {
                throw new AssertionError("listpack expect 255 but " + lpend);
            }
        }

        long length = parser.rdbLoadLen().len;
        Stream.ID lastId = new Stream.ID(parser.rdbLoadLen().len, parser.rdbLoadLen().len);

        // Group
        List<Stream.Group> groups = new ArrayList<>();
        long groupCount = parser.rdbLoadLen().len;
        while (groupCount-- > 0) {
            Stream.Group group = new Stream.Group();
            byte[] groupName = parser.rdbLoadPlainStringObject().first();
            Stream.ID groupLastId = new Stream.ID(parser.rdbLoadLen().len, parser.rdbLoadLen().len);

            // Group PEL
            NavigableMap<Stream.ID, Stream.Nack> groupPendingEntries = new TreeMap<>(Stream.ID.COMPARATOR);
            long globalPel = parser.rdbLoadLen().len;
            while (globalPel-- > 0) {
                Stream.ID rawId = new Stream.ID(in.readLong(8, false), in.readLong(8, false));
                long deliveryTime = parser.rdbLoadMillisecondTime();
                long deliveryCount = parser.rdbLoadLen().len;
                groupPendingEntries.put(rawId, new Stream.Nack(rawId, null, deliveryTime, deliveryCount));
            }

            // Consumer
            List<Stream.Consumer> consumers = new ArrayList<>();
            long consumerCount = parser.rdbLoadLen().len;
            while (consumerCount-- > 0) {
                Stream.Consumer consumer = new Stream.Consumer();
                byte[] consumerName = parser.rdbLoadPlainStringObject().first();
                long seenTime = parser.rdbLoadMillisecondTime();

                // Consumer PEL
                NavigableMap<Stream.ID, Stream.Nack> consumerPendingEntries = new TreeMap<>(Stream.ID.COMPARATOR);
                long pel = parser.rdbLoadLen().len;
                while (pel-- > 0) {
                    Stream.ID rawId = new Stream.ID(in.readLong(8, false), in.readLong(8, false));
                    Stream.Nack nack = groupPendingEntries.get(rawId);
                    nack.setConsumer(consumer);
                    consumerPendingEntries.put(rawId, nack);
                }

                consumer.setName(consumerName);
                consumer.setSeenTime(seenTime);
                consumer.setPendingEntries(consumerPendingEntries);
                consumers.add(consumer);
            }

            group.setName(groupName);
            group.setLastId(groupLastId);
            group.setPendingEntries(groupPendingEntries);
            group.setConsumers(consumers);
            groups.add(group);
        }

        stream.setLastId(lastId);
        stream.setEntries(entries);
        stream.setLength(length);
        stream.setGroups(groups);

        o15.setValueRdbType(RDB_TYPE_STREAM_LISTPACKS);
        o15.setValue(stream);
        o15.setKey(key);
        return context.valueOf(o15);
    }

    protected KeyValuePair<?, ?> rdbLoadObject(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        /*
         * ----------------------------
         * $value-type                 # This name value pair doesn't have an expiry. $value_type guaranteed != to FD, FC, FE and FF
         * $string-encoded-name
         * $encoded-value
         * ----------------------------
         */
        int valueType = context.getValueRdbType();
        switch (valueType) {
            case RDB_TYPE_STRING:
                return (KeyValuePair<?, ?>) applyString(in, version, context);
            case RDB_TYPE_LIST:
                return (KeyValuePair<?, ?>) applyList(in, version, context);
            case RDB_TYPE_SET:
                return (KeyValuePair<?, ?>) applySet(in, version, context);
            case RDB_TYPE_ZSET:
                return (KeyValuePair<?, ?>) applyZSet(in, version, context);
            case RDB_TYPE_ZSET_2:
                return (KeyValuePair<?, ?>) applyZSet2(in, version, context);
            case RDB_TYPE_HASH:
                return (KeyValuePair<?, ?>) applyHash(in, version, context);
            case RDB_TYPE_HASH_ZIPMAP:
                return (KeyValuePair<?, ?>) applyHashZipMap(in, version, context);
            case RDB_TYPE_LIST_ZIPLIST:
                return (KeyValuePair<?, ?>) applyListZipList(in, version, context);
            case RDB_TYPE_SET_INTSET:
                return (KeyValuePair<?, ?>) applySetIntSet(in, version, context);
            case RDB_TYPE_ZSET_ZIPLIST:
                return (KeyValuePair<?, ?>) applyZSetZipList(in, version, context);
            case RDB_TYPE_HASH_ZIPLIST:
                return (KeyValuePair<?, ?>) applyHashZipList(in, version, context);
            case RDB_TYPE_LIST_QUICKLIST:
                return (KeyValuePair<?, ?>) applyListQuickList(in, version, context);
            case RDB_TYPE_MODULE:
                return (KeyValuePair<?, ?>) applyModule(in, version, context);
            case RDB_TYPE_MODULE_2:
                return (KeyValuePair<?, ?>) applyModule2(in, version, context);
            case RDB_TYPE_STREAM_LISTPACKS:
                return (KeyValuePair<?, ?>) applyStreamListPacks(in, version, context);
            default:
                throw new AssertionError("unexpected value type:" + valueType);
        }
    }
}
