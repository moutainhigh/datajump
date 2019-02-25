
package com.ai.ds.dj.rdb.parse;


import com.ai.ds.dj.datatype.Module;
import com.ai.ds.dj.rdb.io.RedisInputStream;
import java.io.IOException;
public interface ModuleParser<T extends Module> {



    T parse(RedisInputStream in, int version) throws IOException;
}
