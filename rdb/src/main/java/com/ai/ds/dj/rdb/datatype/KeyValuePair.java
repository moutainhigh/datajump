

package com.ai.ds.dj.rdb.datatype;


import com.ai.ds.dj.rdb.event.Event;


public class KeyValuePair<K, V> implements Event {

    private static final long serialVersionUID = 1L;

    protected DB db;
    protected int valueRdbType;
    protected ExpiredType expiredType = ExpiredType.NONE;
    protected Long expiredValue;
    protected EvictType evictType = EvictType.NONE;
    protected Long evictValue;
    protected K key;
    protected V value;

    public int getValueRdbType() {
        return valueRdbType;
    }

    public void setValueRdbType(int valueRdbType) {
        this.valueRdbType = valueRdbType;
    }

    public ExpiredType getExpiredType() {
        return expiredType;
    }

    public void setExpiredType(ExpiredType expiredType) {
        this.expiredType = expiredType;
    }

    public Long getExpiredValue() {
        return expiredValue;
    }

    public void setExpiredValue(Long expiredValue) {
        this.expiredValue = expiredValue;
    }

    public EvictType getEvictType() {
        return evictType;
    }

    public void setEvictType(EvictType evictType) {
        this.evictType = evictType;
    }

    public Long getEvictValue() {
        return evictValue;
    }

    public void setEvictValue(Long evictValue) {
        this.evictValue = evictValue;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public DB getDb() {
        return db;
    }

    public void setDb(DB db) {
        this.db = db;
    }

    /**
     * @return expiredValue as Integer
     */
    public Integer getExpiredSeconds() {
        return expiredValue == null ? null : expiredValue.intValue();
    }

    /**
     * @return expiredValue as Long
     */
    public Long getExpiredMs() {
        return expiredValue;
    }
}
