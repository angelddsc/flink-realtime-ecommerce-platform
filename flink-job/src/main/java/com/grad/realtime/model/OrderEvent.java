package com.grad.realtime.model;

import java.io.Serializable;

/**
 * 订单事件 POJO，字段与 Python 模拟器输出保持一致。
 * Flink POJO 要求：public 类 + 无参构造 + public 字段或 getter/setter。
 */
public class OrderEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    public String orderId;
    public String userId;
    public String productId;
    public String category;
    public int quantity;
    public double amount;
    public String province;
    public String status;
    public String behavior;
    public long eventTime;

    public OrderEvent() {
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                ", category='" + category + '\'' +
                ", quantity=" + quantity +
                ", amount=" + amount +
                ", province='" + province + '\'' +
                ", status='" + status + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}
