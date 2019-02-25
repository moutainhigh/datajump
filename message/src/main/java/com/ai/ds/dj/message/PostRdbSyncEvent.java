

package com.ai.ds.dj.message;


public class PostRdbSyncEvent implements Event {

    private static final long serialVersionUID = 1L;

    private long checksum;
    
    public PostRdbSyncEvent() {
    }
    
    public PostRdbSyncEvent(long checksum) {
        this.checksum = checksum;
    }

    public long getChecksum() {
        return checksum;
    }

    public void setChecksum(long checksum) {
        this.checksum = checksum;
    }
}
