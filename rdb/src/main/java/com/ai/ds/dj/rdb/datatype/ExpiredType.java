

package com.ai.ds.dj.rdb.datatype;

import java.io.Serializable;

/**
 *
 */
public enum ExpiredType implements Serializable {
    /**
     * not set
     */
    NONE,
    /**
     * expired by seconds
     */
    SECOND,
    /**
     * expired by millisecond
     */
    MS
}
