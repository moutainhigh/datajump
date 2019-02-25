package com.ai.ds.dj.util;

/**
 * Copyright asiainfo.com
 *
 * @author wuwh6
 */
public class CollectionsUtils {


    /**
     * 整形转化成数组
     * @param value
     * @return
     */
    public static final byte[] toByteArray(int value) {
        return SafeEncoder.encode(String.valueOf(value));
    }

    public static final byte[] toByteArray(long value) {
        return SafeEncoder.encode(String.valueOf(value));
    }

    public static final byte[] toByteArray(double value) {
        return Double.isInfinite(value) ? (value == 1.0D / 0.0 ? "+inf".getBytes() : "-inf".getBytes()) : SafeEncoder.encode(String.valueOf(value));
    }



}
