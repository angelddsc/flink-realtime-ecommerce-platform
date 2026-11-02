package com.grad.realtime.model;

import java.io.Serializable;

/** CEP 检测到的复合事件：同一用户同一商品的「加购→购买」转化 */
public class CepEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    public String userId;
    public String productId;
    public String category;
    public long cartTime;
    public long buyTime;
    public long gapMs;
    public String pattern;

    public CepEvent() {
    }

    public CepEvent(String userId, String productId, String category,
                    long cartTime, long buyTime, String pattern) {
        this.userId = userId;
        this.productId = productId;
        this.category = category;
        this.cartTime = cartTime;
        this.buyTime = buyTime;
        this.gapMs = Math.max(0L, buyTime - cartTime);
        this.pattern = pattern;
    }
}
