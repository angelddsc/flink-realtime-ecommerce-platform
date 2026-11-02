-- 实时指标结果表（Flink 聚合结果落库）
CREATE TABLE IF NOT EXISTS category_metric (
    id          BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键',
    category    VARCHAR(64)  NOT NULL COMMENT '商品品类',
    window_end  BIGINT       NOT NULL COMMENT '窗口结束时间(epoch ms)',
    paid_orders BIGINT       NOT NULL DEFAULT 0 COMMENT '支付成功订单数',
    gmv         DECIMAL(18, 2) NOT NULL DEFAULT 0.00 COMMENT '成交总额',
    latency_ms  BIGINT       NOT NULL DEFAULT 0 COMMENT '窗口结束到结果落库的处理延迟(ms)',
    created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '写入时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_cat_we (category, window_end),
    KEY idx_window_end (window_end),
    KEY idx_category (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='品类实时指标';

-- 实时告警事件表
CREATE TABLE IF NOT EXISTS alert_event (
    id              BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键',
    category        VARCHAR(64)  NOT NULL COMMENT '商品品类',
    window_end      BIGINT       NOT NULL COMMENT '窗口结束时间(epoch ms)',
    alert_type      VARCHAR(32)  NOT NULL COMMENT '告警类型 GMV/ORDERS',
    message         VARCHAR(255) NOT NULL COMMENT '告警内容',
    metric_value    DECIMAL(18, 2) NOT NULL DEFAULT 0.00 COMMENT '实际值',
    threshold_value DECIMAL(18, 2) NOT NULL DEFAULT 0.00 COMMENT '阈值',
    created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '写入时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_alert (category, window_end, alert_type),
    KEY idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='实时告警事件';

-- 订单明细表（后续阶段写入）
CREATE TABLE IF NOT EXISTS order_detail (
    id          BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键',
    order_id    VARCHAR(64)  NOT NULL COMMENT '订单号',
    user_id     VARCHAR(32)  NOT NULL COMMENT '用户ID',
    product_id  VARCHAR(32)  NOT NULL COMMENT '商品ID',
    category    VARCHAR(64)  NOT NULL COMMENT '品类',
    quantity    INT          NOT NULL DEFAULT 1 COMMENT '数量',
    amount      DECIMAL(18, 2) NOT NULL DEFAULT 0.00 COMMENT '金额',
    province    VARCHAR(32)  COMMENT '省份',
    status      VARCHAR(16)  NOT NULL COMMENT '状态',
    event_time  BIGINT       NOT NULL COMMENT '事件时间(epoch ms)',
    created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '写入时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_order_id (order_id),
    KEY idx_event_time (event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单明细';

-- 行为转化漏斗计数表
CREATE TABLE IF NOT EXISTS behavior_funnel (
    window_end BIGINT       NOT NULL COMMENT '窗口结束时间(epoch ms)',
    behavior   VARCHAR(16)  NOT NULL COMMENT '行为类型 pv/cart/fav/buy',
    cnt        BIGINT       NOT NULL DEFAULT 0 COMMENT '窗口内数量',
    created_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '写入时间',
    PRIMARY KEY (window_end, behavior)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='行为转化漏斗计数';

-- 数据质量指标表
CREATE TABLE IF NOT EXISTS data_quality (
    window_end   BIGINT       NOT NULL COMMENT '窗口结束时间(epoch ms)',
    total        BIGINT       NOT NULL DEFAULT 0 COMMENT '接收记录总数',
    valid        BIGINT       NOT NULL DEFAULT 0 COMMENT '有效记录数',
    invalid      BIGINT       NOT NULL DEFAULT 0 COMMENT '无效/脏数据数',
    invalid_rate DECIMAL(6,2) NOT NULL DEFAULT 0.00 COMMENT '无效占比(%)',
    created_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '写入时间',
    PRIMARY KEY (window_end)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据质量指标';

-- CEP 复合事件表（加购→购买 转化路径）
CREATE TABLE IF NOT EXISTS cep_event (
    id          BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键',
    user_id     VARCHAR(32)  NOT NULL COMMENT '用户ID',
    product_id  VARCHAR(32)  NOT NULL COMMENT '商品ID',
    category    VARCHAR(64)  COMMENT '品类',
    cart_time   BIGINT       NOT NULL COMMENT '加购事件时间(ms)',
    buy_time    BIGINT       NOT NULL COMMENT '购买事件时间(ms)',
    gap_ms      BIGINT       NOT NULL DEFAULT 0 COMMENT '两次行为间隔(ms)',
    pattern     VARCHAR(32)  NOT NULL COMMENT '模式名',
    created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '写入时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_cep (user_id, product_id, cart_time, buy_time),
    KEY idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='CEP复合事件';

-- 告警规则表（可配置）
CREATE TABLE IF NOT EXISTS alert_rule (
    id         BIGINT        NOT NULL AUTO_INCREMENT COMMENT '主键',
    name       VARCHAR(32)   NOT NULL COMMENT '规则名称(同时作为告警类型)',
    metric     VARCHAR(16)   NOT NULL COMMENT '指标 gmv/orders',
    operator   VARCHAR(4)    NOT NULL COMMENT '比较符 >/<',
    threshold  DECIMAL(18,2) NOT NULL DEFAULT 0.00 COMMENT '阈值',
    enabled    TINYINT(1)    NOT NULL DEFAULT 1 COMMENT '是否启用',
    updated_at TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='告警规则';

INSERT INTO alert_rule (name, metric, operator, threshold, enabled) VALUES
    ('GMV_HIGH', 'gmv', '>', 30000.00, 1),
    ('ORDERS_HIGH', 'orders', '>', 8.00, 1)
ON DUPLICATE KEY UPDATE name = name;
