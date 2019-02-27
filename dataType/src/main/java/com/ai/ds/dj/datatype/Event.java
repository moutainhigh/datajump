

package com.ai.ds.dj.datatype;

import java.io.Serializable;


public interface Event extends Serializable {

    /**
     * 获取事件类型
     * @return
     */
    public int getEventType();

    /**
     * 设置事件类型
     * @param type
     */
    public void setEventType(int type);
}
