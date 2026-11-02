package com.grad.realtime.model;

import java.io.Serializable;

/** 告警规则：从数据库 alert_rule 表加载 */
public class AlertRule implements Serializable {

    private static final long serialVersionUID = 1L;

    public long id;
    public String name;
    public String metric;    // gmv / orders
    public String operator;  // > / <
    public double threshold;
    public boolean enabled;

    public AlertRule() {
    }

    public AlertRule(long id, String name, String metric, String operator, double threshold, boolean enabled) {
        this.id = id;
        this.name = name;
        this.metric = metric;
        this.operator = operator;
        this.threshold = threshold;
        this.enabled = enabled;
    }
}
