
package com.ai.ds.dj.rdb.datatype;


public class ContextKeyValuePair extends KeyValuePair<Void, Void> {
    
    private static final long serialVersionUID = 1L;
    
    public <K, V> KeyValuePair<K, V> valueOf(KeyValuePair<K, V> kv) {
        kv.setDb(this.getDb());
        kv.setEvictType(this.getEvictType());
        kv.setEvictValue(this.getEvictValue());
        kv.setExpiredType(this.getExpiredType());
        kv.setExpiredValue(this.getExpiredValue());
        return kv;
    }
}
