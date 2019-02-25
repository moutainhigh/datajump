
package com.ai.ds.dj.rdb.parse;

import com.ai.ds.dj.rdb.Constants;
import com.ai.ds.dj.rdb.io.RedisInputStream;
import com.ai.ds.dj.rdb.util.ByteArray;
import com.ai.ds.dj.rdb.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.math.BigInteger;
import static com.ai.ds.dj.rdb.Constants.*;

public class DefaultRdbModuleParser {
    protected static final Logger logger = LoggerFactory.getLogger(DefaultRdbModuleParser.class);

    private final RedisInputStream in;
    private final BaseRdbParser parser;

    public DefaultRdbModuleParser(RedisInputStream in) {
        this.in = in;
        this.parser = new BaseRdbParser(in);
    }

    public RedisInputStream inputStream() {
        return this.in;
    }

    /**
     * @param version param version of {@link ModuleParser#(RedisInputStream, int)}
     * @return signed long
     * @throws IOException IOException
     * @since 2.3.0
     */
    public long loadSigned(int version) throws IOException {
        if (version == 2) {
            long opcode = parser.rdbLoadLen().len;
            if (opcode != RDB_MODULE_OPCODE_UINT)
                throw new UnsupportedOperationException("Error loading signed or unsigned long from RDB.");
        }
        return parser.rdbLoadLen().len;
    }

    /**
     * @param version param version of {@link ModuleParser#(RedisInputStream, int)}
     * @return unsigned long
     * @throws IOException IOException
     * @since 2.3.0
     */
    public BigInteger loadUnsigned(int version) throws IOException {
        byte[] ary = new byte[8];
        long value = loadSigned(version);
        for (int i = 0; i < 8; i++) {
            ary[7 - i] = (byte) ((value >>> (i << 3)) & 0xFF);
        }
        return new BigInteger(1, ary);
    }

    /**
     * @param version param version of {@link ModuleParser#(RedisInputStream, int)}
     * @return string
     * @throws IOException IOException
     * @since 2.3.0
     */
    public String loadString(int version) throws IOException {
        if (version == 2) {
            long opcode = parser.rdbLoadLen().len;
            if (opcode != RDB_MODULE_OPCODE_STRING)
                throw new UnsupportedOperationException("Error loading string from RDB.");
        }
        ByteArray bytes = parser.rdbGenericLoadStringObject(Constants.RDB_LOAD_NONE);
        return Strings.toString(bytes.first());
    }

    /**
     * @param version param version of {@link ModuleParser#(RedisInputStream, int)}
     * @return string buffer
     * @throws IOException IOException
     * @since 2.3.0
     */
    public byte[] loadStringBuffer(int version) throws IOException {
        if (version == 2) {
            long opcode = parser.rdbLoadLen().len;
            if (opcode != RDB_MODULE_OPCODE_STRING)
                throw new UnsupportedOperationException("Error loading string from RDB.");
        }
        ByteArray bytes = parser.rdbGenericLoadStringObject(Constants.RDB_LOAD_PLAIN);
        return bytes.first();
    }

    /**
     * @param version param version of {@link ModuleParser#(RedisInputStream, int)}
     * @return double
     * @throws IOException IOException
     * @since 2.3.0
     */
    public double loadDouble(int version) throws IOException {
        if (version == 2) {
            long opcode = parser.rdbLoadLen().len;
            if (opcode != RDB_MODULE_OPCODE_DOUBLE)
                throw new UnsupportedOperationException("Error loading double from RDB.");
        }
        return parser.rdbLoadBinaryDoubleValue();
    }

    /**
     * @param version param version of {@link ModuleParser#(RedisInputStream, int)}
     * @return single precision float
     * @throws IOException io exception
     * @since 2.3.0
     */
    public float loadFloat(int version) throws IOException {
        if (version == 2) {
            long opcode = parser.rdbLoadLen().len;
            if (opcode != RDB_MODULE_OPCODE_FLOAT)
                throw new UnsupportedOperationException("Error loading float from RDB.");
        }
        return parser.rdbLoadBinaryFloatValue();
    }
}
