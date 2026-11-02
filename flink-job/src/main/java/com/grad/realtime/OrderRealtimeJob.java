package com.grad.realtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.grad.realtime.model.AlertEvent;
import com.grad.realtime.model.AlertRule;
import com.grad.realtime.model.BehaviorMetric;
import com.grad.realtime.model.CategoryMetric;
import com.grad.realtime.model.CepEvent;
import com.grad.realtime.model.DataQuality;
import com.grad.realtime.model.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 实时作业：
 * Kafka(订单流) -> JSON 解析 -> 过滤支付成功 -> 按品类 10 秒滚动窗口
 *   -> 统计 GMV 与订单量
 *   -> ① 打印  ② 写入 MySQL category_metric
 *   -> ③ 超过阈值的生成告警, 写入 MySQL alert_event
 */
public class OrderRealtimeJob {

    /** 10 秒窗口内 GMV 阈值 */
    private static final double GMV_ALERT_THRESHOLD = 30000.0;
    /** 10 秒窗口内支付订单数阈值 */
    private static final long ORDER_ALERT_THRESHOLD = 8L;

    public static void main(String[] args) throws Exception {
        final String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        final String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "order-events");
        final String jdbcUrl = System.getenv().getOrDefault("MYSQL_URL",
                "jdbc:mysql://127.0.0.1:3306/bigdata?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf8&allowPublicKeyRetrieval=true");
        final String jdbcUser = System.getenv().getOrDefault("MYSQL_USER", "root");
        final String jdbcPassword = System.getenv().getOrDefault("MYSQL_PASSWORD", "root123456");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000L);
        env.setParallelism(2);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId("order-realtime-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        final ObjectMapper mapper = new ObjectMapper();

        DataStream<String> raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-order-source");

        DataStream<OrderEvent> orders = raw
                .map(line -> {
                    try {
                        return mapper.readValue(line, OrderEvent.class);
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter(order -> order != null)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, ts) -> event.eventTime)
                                .withIdleness(Duration.ofSeconds(5))
                );

        // 数据质量：解析失败或关键字段缺失的统计（处理时间 10 秒窗口）
        DataStream<QualityFlag> qualityFlags = raw.map(line -> {
            try {
                OrderEvent o = mapper.readValue(line, OrderEvent.class);
                boolean ok = o != null && o.orderId != null && o.category != null
                        && o.status != null && o.eventTime > 0;
                return ok ? new QualityFlag(1, 0) : new QualityFlag(0, 1);
            } catch (Exception e) {
                return new QualityFlag(0, 1);
            }
        });

        DataStream<DataQuality> quality = qualityFlags
                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(new QualityAggregator(), new QualityWindowFunction());

        // 行为转化漏斗：按行为类型在窗口内计数（pv/cart/fav/buy）
        DataStream<BehaviorMetric> funnel = orders
                .map(OrderRealtimeJob::behaviorOf)
                .filter(b -> b != null)
                .keyBy(b -> b)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(new CountAggregator(), new BehaviorWindowFunction());

        funnel.print("behavior-funnel");

        DataStream<CategoryMetric> metrics = orders
                .filter(order -> "PAID".equals(order.status))
                .keyBy(order -> order.category)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(new AmountAggregator(), new MetricResultFunction());

        // ① 控制台打印
        metrics.print("realtime-metric");

        // ② 指标写入 MySQL
        JdbcConnectionOptions conn = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername(jdbcUser)
                .withPassword(jdbcPassword)
                .build();

        JdbcExecutionOptions exec = JdbcExecutionOptions.builder()
                .withBatchSize(20)
                .withBatchIntervalMs(2000)
                .withMaxRetries(3)
                .build();

        metrics.addSink(JdbcSink.sink(
                "INSERT INTO category_metric (category, window_end, paid_orders, gmv, latency_ms) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE paid_orders=VALUES(paid_orders), gmv=VALUES(gmv), latency_ms=VALUES(latency_ms)",
                (ps, m) -> {
                    ps.setString(1, m.category);
                    ps.setLong(2, m.windowEnd);
                    ps.setLong(3, m.paidOrders);
                    ps.setBigDecimal(4, BigDecimal.valueOf(m.gmv));
                    ps.setLong(5, m.latencyMs);
                },
                exec, conn
        )).name("mysql-category-metric");

        // 行为漏斗写入 MySQL（幂等 upsert）
        funnel.addSink(JdbcSink.sink(
                "INSERT INTO behavior_funnel (window_end, behavior, cnt) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE cnt = VALUES(cnt)",
                (ps, f) -> {
                    ps.setLong(1, f.windowEnd);
                    ps.setString(2, f.behavior);
                    ps.setLong(3, f.cnt);
                },
                exec, conn
        )).name("mysql-behavior-funnel");

        // 数据质量指标写入 MySQL（幂等 upsert）
        quality.addSink(JdbcSink.sink(
                "INSERT INTO data_quality (window_end, total, valid, invalid, invalid_rate) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE total=VALUES(total), valid=VALUES(valid), invalid=VALUES(invalid), invalid_rate=VALUES(invalid_rate)",
                (ps, q) -> {
                    ps.setLong(1, q.windowEnd);
                    ps.setLong(2, q.total);
                    ps.setLong(3, q.valid);
                    ps.setLong(4, q.invalid);
                    ps.setBigDecimal(5, BigDecimal.valueOf(q.invalidRate));
                },
                exec, conn
        )).name("mysql-data-quality");

        // ③ 阈值告警（规则来自数据库 alert_rule，每 30 秒自动刷新）
        DataStream<AlertEvent> alerts = metrics
                .flatMap(new RuleAlertFunction(jdbcUrl, jdbcUser, jdbcPassword));

        alerts.print("alert");

        alerts.addSink(JdbcSink.sink(
                "INSERT INTO alert_event (category, window_end, alert_type, message, metric_value, threshold_value) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE message=VALUES(message), metric_value=VALUES(metric_value), threshold_value=VALUES(threshold_value)",
                (ps, a) -> {
                    ps.setString(1, a.category);
                    ps.setLong(2, a.windowEnd);
                    ps.setString(3, a.alertType);
                    ps.setString(4, a.message);
                    ps.setBigDecimal(5, BigDecimal.valueOf(a.metricValue));
                    ps.setBigDecimal(6, BigDecimal.valueOf(a.thresholdValue));
                },
                exec, conn
        )).name("mysql-alert-event");

        // ⑤ CEP 复杂事件：同一用户同一商品「加购 → 购买」转化路径（30 分钟内）
        DataStream<OrderEvent> cepInput = orders.filter(o ->
                "CART".equalsIgnoreCase(o.status) || "PAID".equalsIgnoreCase(o.status));

        Pattern<OrderEvent, ?> cepPattern = Pattern.<OrderEvent>begin("cart")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent e) {
                        return "CART".equalsIgnoreCase(e.status);
                    }
                })
                .followedBy("buy")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent e) {
                        return "PAID".equalsIgnoreCase(e.status);
                    }
                })
                .within(Duration.ofMinutes(30));

        PatternStream<OrderEvent> patternStream = CEP.pattern(
                cepInput.keyBy(o -> o.userId + "|" + o.productId), cepPattern);

        DataStream<CepEvent> cepEvents = patternStream.process(
                new PatternProcessFunction<OrderEvent, CepEvent>() {
                    @Override
                    public void processMatch(Map<String, List<OrderEvent>> match,
                                             Context ctx,
                                             Collector<CepEvent> out) {
                        OrderEvent cart = match.get("cart").get(0);
                        OrderEvent buy = match.get("buy").get(0);
                        out.collect(new CepEvent(cart.userId, cart.productId, cart.category,
                                cart.eventTime, buy.eventTime, "CART_BUY"));
                    }
                });

        cepEvents.print("cep-event");

        cepEvents.addSink(JdbcSink.sink(
                "INSERT INTO cep_event (user_id, product_id, category, cart_time, buy_time, gap_ms, pattern) VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE gap_ms=VALUES(gap_ms)",
                (ps, e) -> {
                    ps.setString(1, e.userId);
                    ps.setString(2, e.productId);
                    ps.setString(3, e.category);
                    ps.setLong(4, e.cartTime);
                    ps.setLong(5, e.buyTime);
                    ps.setLong(6, e.gapMs);
                    ps.setString(7, e.pattern);
                },
                exec, conn
        )).name("mysql-cep-event");

        env.execute("Order Realtime Monitor");
    }

    /** 窗口累加器 */
    public static class AmountAccumulator {
        public double amount = 0.0;
        public long count = 0L;
    }

    /** 增量聚合：金额与订单数 */
    public static class AmountAggregator
            implements AggregateFunction<OrderEvent, AmountAccumulator, AmountAccumulator> {

        @Override
        public AmountAccumulator createAccumulator() {
            return new AmountAccumulator();
        }

        @Override
        public AmountAccumulator add(OrderEvent value, AmountAccumulator acc) {
            acc.amount += value.amount;
            acc.count += 1;
            return acc;
        }

        @Override
        public AmountAccumulator getResult(AmountAccumulator acc) {
            return acc;
        }

        @Override
        public AmountAccumulator merge(AmountAccumulator a, AmountAccumulator b) {
            a.amount += b.amount;
            a.count += b.count;
            return a;
        }
    }

    /** 给聚合结果补上窗口结束时间, 输出 CategoryMetric */
    public static class MetricResultFunction
            extends ProcessWindowFunction<AmountAccumulator, CategoryMetric, String, TimeWindow> {

        @Override
        public void process(String category,
                            Context context,
                            Iterable<AmountAccumulator> elements,
                            Collector<CategoryMetric> out) {
            AmountAccumulator acc = elements.iterator().next();
            long windowEnd = context.window().getEnd();
            long latency = Math.max(0L, System.currentTimeMillis() - windowEnd);
            out.collect(new CategoryMetric(category, windowEnd, acc.count, acc.amount, latency));
        }
    }

    /** 从订单中解析行为类型：优先 behavior 字段，其次根据 status 推导 */
    static String behaviorOf(OrderEvent o) {
        if (o.behavior != null && !o.behavior.isEmpty()) {
            String b = o.behavior.toLowerCase();
            if (b.equals("pv") || b.equals("cart") || b.equals("fav") || b.equals("buy")) {
                return b;
            }
        }
        if ("PAID".equalsIgnoreCase(o.status)) {
            return "buy";
        }
        if (o.status != null) {
            String s = o.status.toLowerCase();
            if (s.equals("pv") || s.equals("cart") || s.equals("fav")) {
                return s;
            }
        }
        return null;
    }

    /** 行为计数累加器 */
    public static class CountAggregator implements AggregateFunction<String, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(String value, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    /** 给行为计数补上窗口结束时间 */
    public static class BehaviorWindowFunction
            extends ProcessWindowFunction<Long, BehaviorMetric, String, TimeWindow> {
        @Override
        public void process(String behavior,
                            Context context,
                            Iterable<Long> elements,
                            Collector<BehaviorMetric> out) {
            out.collect(new BehaviorMetric(behavior, context.window().getEnd(), elements.iterator().next()));
        }
    }

    /** 数据质量累加器 */
    public static class QualityAccumulator {
        public long valid = 0L;
        public long invalid = 0L;
    }

    /** 单条记录的质量标记（POJO，便于 Flink 推断类型） */
    public static class QualityFlag {
        public int valid;
        public int invalid;

        public QualityFlag() {
        }

        public QualityFlag(int valid, int invalid) {
            this.valid = valid;
            this.invalid = invalid;
        }
    }

    /** 数据质量增量聚合：有效/无效记录数 */
    public static class QualityAggregator
            implements AggregateFunction<QualityFlag, QualityAccumulator, QualityAccumulator> {

        @Override
        public QualityAccumulator createAccumulator() {
            return new QualityAccumulator();
        }

        @Override
        public QualityAccumulator add(QualityFlag v, QualityAccumulator acc) {
            acc.valid += v.valid;
            acc.invalid += v.invalid;
            return acc;
        }

        @Override
        public QualityAccumulator getResult(QualityAccumulator acc) {
            return acc;
        }

        @Override
        public QualityAccumulator merge(QualityAccumulator a, QualityAccumulator b) {
            a.valid += b.valid;
            a.invalid += b.invalid;
            return a;
        }
    }

    /** 生成数据质量结果 */
    public static class QualityWindowFunction
            extends ProcessAllWindowFunction<QualityAccumulator, DataQuality, TimeWindow> {
        @Override
        public void process(Context context,
                            Iterable<QualityAccumulator> elements,
                            Collector<DataQuality> out) {
            QualityAccumulator acc = elements.iterator().next();
            long total = acc.valid + acc.invalid;
            double rate = total > 0 ? acc.invalid * 100.0 / total : 0.0;
            out.collect(new DataQuality(context.window().getEnd(), total, acc.valid, acc.invalid, rate));
        }
    }

    /** 可配置规则告警：从 alert_rule 表加载规则，每 30 秒刷新一次 */
    public static class RuleAlertFunction extends RichFlatMapFunction<CategoryMetric, AlertEvent> {

        private final String url;
        private final String user;
        private final String password;
        private transient volatile List<AlertRule> rules = new ArrayList<>();
        private transient ScheduledExecutorService scheduler;

        public RuleAlertFunction(String url, String user, String password) {
            this.url = url;
            this.user = user;
            this.password = password;
        }

        @Override
        public void open(Configuration parameters) {
            loadRules();
            scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleWithFixedDelay(this::loadRules, 30, 30, TimeUnit.SECONDS);
        }

        private void loadRules() {
            List<AlertRule> list = new ArrayList<>();
            try (Connection c = DriverManager.getConnection(url, user, password);
                 Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT id, name, metric, operator, threshold, enabled FROM alert_rule WHERE enabled = 1")) {
                while (rs.next()) {
                    list.add(new AlertRule(
                            rs.getLong("id"),
                            rs.getString("name"),
                            rs.getString("metric"),
                            rs.getString("operator"),
                            rs.getDouble("threshold"),
                            rs.getBoolean("enabled")));
                }
                this.rules = list;
                System.out.println("[rule] loaded " + list.size() + " alert rules");
            } catch (Exception e) {
                System.err.println("[rule] load failed, keep previous rules: " + e.getMessage());
            }
        }

        @Override
        public void flatMap(CategoryMetric m, Collector<AlertEvent> out) {
            for (AlertRule r : rules) {
                double value = "orders".equalsIgnoreCase(r.metric) ? m.paidOrders : m.gmv;
                boolean hit = ">".equals(r.operator) ? value > r.threshold : value < r.threshold;
                if (hit) {
                    String message = String.format("规则[%s] 品类[%s] 10秒内 %s=%.2f 触发(条件 %s %.2f)",
                            r.name, m.category, r.metric, value, r.operator, r.threshold);
                    out.collect(new AlertEvent(m.category, m.windowEnd, r.name, message, value, r.threshold));
                }
            }
        }

        @Override
        public void close() throws Exception {
            if (scheduler != null) {
                scheduler.shutdownNow();
            }
        }
    }
}
