

package com.ai.ds.dj.rdb.datatype;

import java.io.Serializable;

public enum EvictType implements Serializable {

    /**
     * maxmemory-policy : volatile-lru, allkeys-lru. unit : second
     */
    LRU,

    /**
     * maxmemory-policy : volatile-lfu, allkeys-lfu.
     */
    LFU,

    /**
     * maxmemory-policy : noeviction, volatile-random, allkeys-random, volatile-ttl.
     */
    NONE
}
