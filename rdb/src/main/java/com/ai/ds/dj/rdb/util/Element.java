package com.ai.ds.dj.rdb.util;


import java.io.Serializable;
import java.util.Arrays;

public  final class Element implements Serializable {
    private static final long serialVersionUID = 1L;

    final byte[] bytes;

    Element(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Element key = (Element) o;
        return Arrays.equals(bytes, key.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}