package com.grad.realtime.model;

import java.io.Serializable;

/** 品类实时指标结果，对应 MySQL 表 category_metric */
public class CategoryMetric implements Serializable {

    private static final long serialVersionUID = 1L;

    public String category;
    public long windowEnd;
    public long paidOrders;
    public double gmv;
    public long latencyMs;

    public CategoryMetric() {
    }

    public CategoryMetric(String category, long windowEnd, long paidOrders, double gmv, long latencyMs) {
        this.category = category;
        this.windowEnd = windowEnd;
        this.paidOrders = paidOrders;
        this.gmv = gmv;
        this.latencyMs = latencyMs;
    }

    @Override
    public String toString() {
        return String.format("{\"category\":\"%s\",\"windowEnd\":%d,\"paidOrders\":%d,\"gmv\":%.2f,\"latencyMs\":%d}",
                category, windowEnd, paidOrders, gmv, latencyMs);
    }
}
