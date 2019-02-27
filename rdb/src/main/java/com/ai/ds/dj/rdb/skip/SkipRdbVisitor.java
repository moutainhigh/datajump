
package com.ai.ds.dj.rdb.skip;
import com.ai.ds.dj.datatype.ContextKeyValuePair;
import com.ai.ds.dj.datatype.DB;
import com.ai.ds.dj.datatype.Module;
import com.ai.ds.dj.datatype.Event;
import com.ai.ds.dj.rdb.io.RedisInputStream;
import com.ai.ds.dj.rdb.parse.DefaultRdbVisitor;
import com.ai.ds.dj.rdb.parse.ModuleParser;

import java.io.IOException;
import java.util.NoSuchElementException;

import static com.ai.ds.dj.rdb.Constants.*;


/**
 * @author Leon Chen
 * @since 2.4.6
 */
public class SkipRdbVisitor extends DefaultRdbVisitor {

    public SkipRdbVisitor() {

    }

    @Override
    public DB applySelectDB(RedisInputStream in, int version) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadLen();
        return null;
    }

    @Override
    public DB applyResizeDB(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadLen();
        parser.rdbLoadLen();
        return null;
    }

    @Override
    public Event applyExpireTime(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadTime();
        int type = applyType(in);
        context.setValueRdbType(type);
        if (type == RDB_OPCODE_FREQ) {
            applyFreq(in, version, context);
        } else if (type == RDB_OPCODE_IDLE) {
            applyIdle(in, version, context);
        } else {
            rdbLoadObject(in, version, context);
        }
        return null;
    }

    @Override
    public Event applyExpireTimeMs(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadMillisecondTime();
        int type = applyType(in);
        context.setValueRdbType(type);
        if (type == RDB_OPCODE_FREQ) {
            applyFreq(in, version, context);
        } else if (type == RDB_OPCODE_IDLE) {
            applyIdle(in, version, context);
        } else {
            rdbLoadObject(in, version, context);
        }
        return null;
    }

    @Override
    public Event applyFreq(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        in.read();
        int valueType = applyType(in);
        context.setValueRdbType(valueType);
        rdbLoadObject(in, version, context);
        return null;
    }

    @Override
    public Event applyIdle(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadLen();
        int valueType = applyType(in);
        context.setValueRdbType(valueType);
        rdbLoadObject(in, version, context);
        return null;
    }

    @Override
    public Event applyAux(RedisInputStream in, int version) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadEncodedStringObject();
        parser.rdbLoadEncodedStringObject();
        return null;
    }

    @Override
    public Event applyModuleAux(RedisInputStream in, int version) throws IOException {
        return super.applyModuleAux(in, version);
    }

    @Override
    public Event applyString(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadEncodedStringObject();
        parser.rdbLoadEncodedStringObject();
        return null;
    }

    @Override
    public Event applyList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadEncodedStringObject();
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            parser.rdbLoadEncodedStringObject();
            len--;
        }
        return null;
    }

    @Override
    public Event applySet(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadEncodedStringObject();
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            parser.rdbLoadEncodedStringObject();
            len--;
        }
        return null;
    }

    @Override
    public Event applyZSet(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadEncodedStringObject();
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            parser.rdbLoadEncodedStringObject();
            parser.rdbLoadDoubleValue();
            len--;
        }
        return null;
    }

    @Override
    public Event applyZSet2(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadEncodedStringObject();
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            parser.rdbLoadEncodedStringObject();
            parser.rdbLoadBinaryDoubleValue();
            len--;
        }
        return null;
    }

    @Override
    public Event applyHash(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadEncodedStringObject();
        long len = parser.rdbLoadLen().len;
        while (len > 0) {
            parser.rdbLoadEncodedStringObject();
            parser.rdbLoadEncodedStringObject();
            len--;
        }
        return null;
    }

    @Override
    public Event applyHashZipMap(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadEncodedStringObject();
        parser.rdbLoadPlainStringObject();
        return null;
    }

    @Override
    public Event applyListZipList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadEncodedStringObject();
        parser.rdbLoadPlainStringObject();
        return null;
    }

    @Override
    public Event applySetIntSet(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadEncodedStringObject();
        parser.rdbLoadPlainStringObject();
        return null;
    }

    @Override
    public Event applyZSetZipList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadEncodedStringObject();
        parser.rdbLoadPlainStringObject();
        return null;
    }

    @Override
    public Event applyHashZipList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadEncodedStringObject();
        parser.rdbLoadPlainStringObject();
        return null;
    }

    @Override
    public Event applyListQuickList(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadEncodedStringObject();
        long len = parser.rdbLoadLen().len;
        for (long i = 0; i < len; i++) {
            parser.rdbGenericLoadStringObject();
        }
        return null;
    }

    @Override
    public Event applyModule(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadEncodedStringObject();
        char[] c = new char[9];
        long moduleid = parser.rdbLoadLen().len;
        for (int i = 0; i < c.length; i++) {
            c[i] = MODULE_SET[(int) (moduleid >>> (10 + (c.length - 1 - i) * 6) & 63)];
        }
        String moduleName = new String(c);
        int moduleVersion = (int) (moduleid & 1023);
        ModuleParser<? extends Module> moduleParser = lookupModuleParser(moduleName, moduleVersion);
        if (moduleParser == null) {
            throw new NoSuchElementException("module parser[" + moduleName + ", " + moduleVersion + "] not register. rdb type: [RDB_TYPE_MODULE]");
        }
        moduleParser.parse(in, 1);
        return null;
    }

    @Override
    public Event applyModule2(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadEncodedStringObject();
        char[] c = new char[9];
        long moduleid = parser.rdbLoadLen().len;
        for (int i = 0; i < c.length; i++) {
            c[i] = MODULE_SET[(int) (moduleid >>> (10 + (c.length - 1 - i) * 6) & 63)];
        }
        String moduleName = new String(c);
        int moduleVersion = (int) (moduleid & 1023);
        ModuleParser<? extends Module> moduleParser = lookupModuleParser(moduleName, moduleVersion);
        if (moduleParser == null) {
            SkipRdbParser skipRdbParser = new SkipRdbParser(in);
            skipRdbParser.rdbLoadCheckModuleValue();
        } else {
            moduleParser.parse(in, 2);
            long eof = parser.rdbLoadLen().len;
            if (eof != RDB_MODULE_OPCODE_EOF) {
                throw new UnsupportedOperationException("The RDB file contains module data for the module '" + moduleName + "' that is not terminated by the proper module value EOF marker");
            }
        }
        return null;
    }

    @Override
    public Event applyStreamListPacks(RedisInputStream in, int version, ContextKeyValuePair context) throws IOException {
        SkipRdbParser parser = new SkipRdbParser(in);
        parser.rdbLoadEncodedStringObject();
        long listPacks = parser.rdbLoadLen().len;
        while (listPacks-- > 0) {
            parser.rdbLoadPlainStringObject();
            parser.rdbLoadPlainStringObject();
        }
        parser.rdbLoadLen();
        parser.rdbLoadLen();
        parser.rdbLoadLen();
        long groupCount = parser.rdbLoadLen().len;
        while (groupCount-- > 0) {
            parser.rdbLoadPlainStringObject();
            parser.rdbLoadLen();
            parser.rdbLoadLen();
            long groupPel = parser.rdbLoadLen().len;
            while (groupPel-- > 0) {
                in.skip(16);
                parser.rdbLoadMillisecondTime();
                parser.rdbLoadLen();
            }
            long consumerCount = parser.rdbLoadLen().len;
            while (consumerCount-- > 0) {
                parser.rdbLoadPlainStringObject();
                parser.rdbLoadMillisecondTime();
                long consumerPel = parser.rdbLoadLen().len;
                while (consumerPel-- > 0) {
                    in.skip(16);
                }
            }
        }
        return null;
    }
}
