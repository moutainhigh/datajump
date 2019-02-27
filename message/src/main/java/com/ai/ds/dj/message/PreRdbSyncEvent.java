

package com.ai.ds.dj.message;


import com.ai.ds.dj.datatype.Event;

public class PreRdbSyncEvent implements Event {
    private static final long serialVersionUID = 1L;

    @Override
    public int getEventType() {
        return -1;
    }

    @Override
    public void setEventType(int type) {

    }
}
