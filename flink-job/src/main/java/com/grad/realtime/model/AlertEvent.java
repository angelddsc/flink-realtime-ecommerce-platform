package com.grad.realtime.model;

import java.io.Serializable;

/** 实时告警事件，对应 MySQL 表 alert_event */
public class AlertEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    public String category;
    public long windowEnd;
    public String alertType;
    public String message;
    public double metricValue;
    public double thresholdValue;

    public AlertEvent() {
    }

    public AlertEvent(String category, long windowEnd, String alertType,
                      String message, double metricValue, double thresholdValue) {
        this.category = category;
        this.windowEnd = windowEnd;
        this.alertType = alertType;
        this.message = message;
        this.metricValue = metricValue;
        this.thresholdValue = thresholdValue;
    }

    @Override
    public String toString() {
        return String.format("{\"category\":\"%s\",\"windowEnd\":%d,\"alertType\":\"%s\",\"message\":\"%s\",\"value\":%.2f,\"threshold\":%.2f}",
                category, windowEnd, alertType, message, metricValue, thresholdValue);
    }
}
