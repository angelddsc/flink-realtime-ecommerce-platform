# -*- coding: utf-8 -*-
"""生成 UserBehavior 格式的“会话型”样本（仅用于联调）
   每个会话内同一用户对同一商品依次产生 pv / cart / fav / buy 序列，
   便于 CEP 检测“加购→购买”转化路径。字段与真实数据集一致。"""
import os
import random

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "UserBehavior_sample.csv")
SESSIONS = 12000
SPAN = 1800
BASE_TS = 1511539200

random.seed(2026)
rows = []
for _ in range(SESSIONS):
    user = random.randint(1, 20000)
    item = random.randint(1, 500000)
    cat = random.randint(1, 2000)
    t = random.randint(0, SPAN - 120)

    for _ in range(random.randint(1, 4)):
        rows.append((user, item, cat, "pv", BASE_TS + t))
        t += random.randint(1, 20)

    r = random.random()
    if r < 0.25:
        rows.append((user, item, cat, "cart", BASE_TS + t)); t += random.randint(1, 30)
        if random.random() < 0.60:
            rows.append((user, item, cat, "buy", BASE_TS + t)); t += random.randint(1, 30)
    elif r < 0.40:
        rows.append((user, item, cat, "fav", BASE_TS + t)); t += random.randint(1, 30)
        if random.random() < 0.50:
            rows.append((user, item, cat, "buy", BASE_TS + t)); t += random.randint(1, 30)

rows.sort(key=lambda x: x[4])
with open(OUT, "w", encoding="utf-8", newline="") as f:
    for r in rows:
        f.write("%d,%d,%d,%s,%d\n" % r)

print("已生成会话型样本:", OUT, "共", len(rows), "行")
