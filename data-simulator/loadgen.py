# -*- coding: utf-8 -*-
"""
高吞吐压测数据生成器
多线程向 Kafka 打满订单数据, 用于测量 Flink 处理能力。

环境变量:
  KAFKA_BOOTSTRAP  默认 localhost:9092
  KAFKA_TOPIC      默认 order-events
  TOTAL            总发送条数, 默认 200000
  THREADS          并发线程数, 默认 4
"""
import json
import os
import random
import threading
import time
import uuid

from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "order-events")
TOTAL = int(os.getenv("TOTAL", "200000"))
THREADS = int(os.getenv("THREADS", "4"))

CATEGORIES = ["手机数码", "家用电器", "服饰鞋包", "食品生鲜", "图书文娱", "运动户外"]
PROVINCES = ["北京", "上海", "广东", "浙江", "江苏", "四川", "湖北", "山东", "陕西", "福建"]
STATUS_LIST = ["PAID", "PAID", "PAID", "PAID", "FAILED", "REFUND"]

_lock = threading.Lock()
_counter = {"n": 0}


def build_order():
    quantity = random.randint(1, 3)
    unit_price = round(random.uniform(20, 6000), 2)
    return {
        "orderId": uuid.uuid4().hex[:16],
        "userId": "U%05d" % random.randint(10000, 99999),
        "productId": "P%04d" % random.randint(1000, 1049),
        "category": random.choice(CATEGORIES),
        "quantity": quantity,
        "amount": round(unit_price * quantity, 2),
        "province": random.choice(PROVINCES),
        "status": random.choice(STATUS_LIST),
        "eventTime": int(time.time() * 1000),
    }


def worker(producer, quota):
    sent = 0
    builder = build_order
    send = producer.send
    while sent < quota:
        send(TOPIC, builder())
        sent += 1
        if sent % 1000 == 0:
            with _lock:
                _counter["n"] += 1000
    remainder = sent % 1000
    if remainder:
        with _lock:
            _counter["n"] += remainder


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks=1,
        linger_ms=20,
        batch_size=131072,
    )

    base = TOTAL // THREADS
    quotas = [base] * THREADS
    quotas[-1] += TOTAL - base * THREADS

    print("[loadgen] 开始压测: TOTAL=%d THREADS=%d" % (TOTAL, THREADS))
    start = time.time()
    threads = [threading.Thread(target=worker, args=(producer, q)) for q in quotas]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    producer.flush()
    producer.close()
    elapsed = time.time() - start
    rate = TOTAL / elapsed if elapsed > 0 else 0
    print("[loadgen] 完成: %d 条, 用时 %.2f s, 生产速率 %.0f 条/秒" % (TOTAL, elapsed, rate))


if __name__ == "__main__":
    main()
