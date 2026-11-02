package com.grad.realtime.model;

import java.io.Serializable;

/** 数据质量指标：某个时间窗口内接收、有效、无效记录数 */
public class DataQuality implements Serializable {

    private static final long serialVersionUID = 1L;

    public long windowEnd;
    public long total;
    public long valid;
    public long invalid;
    public double invalidRate;

    public DataQuality() {
    }

    public DataQuality(long windowEnd, long total, long valid, long invalid, double invalidRate) {
        this.windowEnd = windowEnd;
        this.total = total;
        this.valid = valid;
        this.invalid = invalid;
        this.invalidRate = invalidRate;
    }

    @Override
    public String toString() {
        return String.format("{\"windowEnd\":%d,\"total\":%d,\"valid\":%d,\"invalid\":%d,\"invalidRate\":%.2f}",
                windowEnd, total, valid, invalid, invalidRate);
    }
}
