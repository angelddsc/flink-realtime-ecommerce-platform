package com.grad.realtime.model;

import java.io.Serializable;

/** 行为转化计数：某个窗口内某种行为（pv/cart/fav/buy）的数量 */
public class BehaviorMetric implements Serializable {

    private static final long serialVersionUID = 1L;

    public String behavior;
    public long windowEnd;
    public long cnt;

    public BehaviorMetric() {
    }

    public BehaviorMetric(String behavior, long windowEnd, long cnt) {
        this.behavior = behavior;
        this.windowEnd = windowEnd;
        this.cnt = cnt;
    }

    @Override
    public String toString() {
        return String.format("{\"behavior\":\"%s\",\"windowEnd\":%d,\"cnt\":%d}",
                behavior, windowEnd, cnt);
    }
}
