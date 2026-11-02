# -*- coding: utf-8 -*-
"""
淘宝用户行为数据集 (UserBehavior.csv) 按时间戳回放器

数据集原始列(无表头): user_id, item_id, category_id, behavior_type, timestamp
  behavior_type: pv / buy / cart / fav
  timestamp: Unix 秒

本脚本把每条记录映射成现有订单 JSON (order-events)，字段与 Flink 作业保持一致，
从而下游 Flink / MySQL / 大屏无需任何修改。

时间处理:
  - 按原始时间戳顺序回放，并按倍速 SPEED 加速；
  - 事件时间依据 SPEED 压缩并重定基到当前时间，使窗口落在“现在”，
    从而延迟指标仍然有意义。

环境变量:
  DATA_FILE       数据集路径, 默认同目录 UserBehavior.csv
  KAFKA_BOOTSTRAP 默认 localhost:9092
  KAFKA_TOPIC     默认 order-events
  SPEED           回放倍速, 默认 100
  MAX_ROWS        最多回放行数, 0=全部
  ONLY_BUY        1=只回放 buy 行为(成交量), 0=全部行为
  LOOP            1=回放到结尾后循环重放
  EPS_CAP         最大发送速率(条/秒), 0=不限
"""
import csv
import json
import os
import signal
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.getenv("DATA_FILE", os.path.join(HERE, "UserBehavior.csv"))
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "order-events")
SPEED = float(os.getenv("SPEED", "100"))
MAX_ROWS = int(os.getenv("MAX_ROWS", "0"))
ONLY_BUY = os.getenv("ONLY_BUY", "0") == "1"
LOOP = os.getenv("LOOP", "0") == "1"
EPS_CAP = float(os.getenv("EPS_CAP", "20000"))

running = True


def handle_signal(signum, frame):
    global running
    running = False


def create_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks=1,
                linger_ms=20,
                batch_size=131072,
            )
        except KafkaError:
            print("[replay] Kafka 未就绪, 3 秒后重试: %s" % BOOTSTRAP)
            time.sleep(3)


def pseudo_amount(item_id, category_id):
    """原始数据无金额, 由 item/category 派生一个稳定的价格, 保证 GMV 有区分度"""
    return round(10.0 + (item_id % 900) + (category_id % 100) * 0.5, 2)


def iter_rows(path):
    """流式读取, 自动跳过表头/空行/坏行"""
    with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 5:
                continue
            try:
                user_id = int(row[0])
                item_id = int(row[1])
                category_id = int(row[2])
                behavior = row[3].strip()
                ts = int(row[4])
            except ValueError:
                continue  # 跳过表头或坏行
            yield user_id, item_id, category_id, behavior, ts


def replay_once(producer, base_ms):
    seq = 0
    t0 = None
    wall0 = None
    last_log = time.time()
    sent_since = 0

    for user_id, item_id, category_id, behavior, ts in iter_rows(DATA_FILE):
        if not running:
            break
        if MAX_ROWS and seq >= MAX_ROWS:
            break
        if ONLY_BUY and behavior != "buy":
            continue

        if t0 is None:
            t0 = ts
            wall0 = time.time()

        # 目标发送时刻(按倍速压缩)
        target = wall0 + (ts - t0) / SPEED
        now = time.time()
        if target > now:
            time.sleep(min(target - now, 5.0))

        # 事件时间: 压缩并重定基到当前
        event_ms = base_ms + int((ts - t0) * 1000 / SPEED)

        order = {
            "orderId": "TB%09d" % seq,
            "userId": "U%d" % user_id,
            "productId": "P%d" % item_id,
            "category": "C%d" % category_id,
            "quantity": 1,
            "amount": pseudo_amount(item_id, category_id),
            "province": "",
            "status": "PAID" if behavior == "buy" else behavior.upper(),
            "behavior": behavior,
            "eventTime": event_ms,
        }
        producer.send(TOPIC, order)

        seq += 1
        sent_since += 1
        if sent_since >= 1000:
            producer.flush()
            now2 = time.time()
            rate = sent_since / (now2 - last_log) if now2 > last_log else 0
            print("[replay] 已发送 %d 条, 速率 %.0f 条/秒, 数据集时间戳 %d"
                  % (seq, rate, ts))
            last_log = now2
            sent_since = 0

    producer.flush()
    return seq


def main():
    signal.signal(signal.SIGINT, handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_signal)

    if not os.path.exists(DATA_FILE):
        print("[replay] 未找到数据集: %s" % DATA_FILE)
        print("         请下载 UserBehavior.csv 放到该路径, 或设置环境变量 DATA_FILE")
        sys.exit(1)

    print("[replay] 数据集: %s" % DATA_FILE)
    print("[replay] 倍速 SPEED=%s, ONLY_BUY=%s, LOOP=%s, MAX_ROWS=%s"
          % (SPEED, ONLY_BUY, LOOP, MAX_ROWS))

    producer = create_producer()
    total = 0
    while running:
        total += replay_once(producer, int(time.time() * 1000))
        if not LOOP or not running:
            break
        print("[replay] 到达文件末尾, 循环重放...")

    producer.close()
    print("[replay] 结束, 本次共发送 %d 条" % total)


if __name__ == "__main__":
    main()
