# -*- coding: utf-8 -*-
"""实时监控大屏后端：从 MySQL 读取 Flink 落库的指标与告警"""
import json
import os
import time
from datetime import datetime
from decimal import Decimal

import pymysql
from flask import Flask, jsonify, render_template, request

DB = dict(
    host=os.getenv("MYSQL_HOST", "127.0.0.1"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "root"),
    password=os.getenv("MYSQL_PASSWORD", "root123456"),
    database=os.getenv("MYSQL_DB", "bigdata"),
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

app = Flask(__name__)

# 可选的品类ID→名称映射表：放在 dashboard/category_map.json，形如 {"C123":"手机数码"}
CATEGORY_MAP = {}
_map_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "category_map.json")
if os.path.exists(_map_path):
    try:
        with open(_map_path, encoding="utf-8") as f:
            CATEGORY_MAP = json.load(f)
    except Exception:
        CATEGORY_MAP = {}


def query(sql, args=None):
    conn = pymysql.connect(**DB)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args or ())
            return cur.fetchall()
    finally:
        conn.close()


def clean(rows):
    """把 Decimal / datetime 转成可 JSON 序列化的类型"""
    out = []
    for row in rows:
        item = {}
        for k, v in row.items():
            if isinstance(v, Decimal):
                item[k] = float(v)
            elif isinstance(v, datetime):
                item[k] = v.strftime("%Y-%m-%d %H:%M:%S")
            else:
                item[k] = v
        out.append(item)
    return out


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/summary")
def summary():
    rows = query(
        """SELECT COUNT(*) AS metric_rows,
                  (SELECT COUNT(*) FROM alert_event) AS alert_rows,
                  (SELECT IFNULL(ROUND(SUM(gmv), 2), 0) FROM category_metric) AS total_gmv,
                  (SELECT IFNULL(ROUND(AVG(latency_ms), 0), 0)
                     FROM (SELECT latency_ms FROM category_metric ORDER BY id DESC LIMIT 200) t) AS avg_latency
           FROM category_metric"""
    )
    return jsonify(clean(rows)[0] if rows else {})


@app.route("/api/latest")
def latest():
    """每个品类最新一个窗口的指标；limit>0 时按成交额返回前 N 个品类"""
    limit = int(request.args.get("limit", "0"))
    sql = """SELECT m.category, m.window_end, m.paid_orders, m.gmv
             FROM category_metric m
             JOIN (SELECT category, MAX(window_end) AS we
                   FROM category_metric GROUP BY category) t
               ON m.category = t.category AND m.window_end = t.we
             ORDER BY m.gmv DESC"""
    if limit > 0:
        rows = query(sql + " LIMIT %s", (limit,))
    else:
        rows = query(sql)
    for r in rows:
        cid = str(r.get("category") or "")
        key = cid[1:] if cid.startswith("C") else cid
        r["category_name"] = CATEGORY_MAP.get(cid) or CATEGORY_MAP.get(key)
    return jsonify(clean(rows))


@app.route("/api/trend")
def trend():
    """指定品类最近 N 个窗口的趋势；minutes>0 时只取最近 N 分钟"""
    category = request.args.get("category")
    limit = int(request.args.get("limit", "30"))
    minutes = int(request.args.get("minutes", "0"))
    sql = "SELECT window_end, gmv, paid_orders FROM category_metric WHERE category = %s"
    args = [category]
    if minutes > 0:
        sql += " AND window_end >= %s"
        args.append(int(time.time() * 1000) - minutes * 60000)
    sql += " ORDER BY window_end DESC LIMIT %s"
    args.append(limit)
    rows = query(sql, tuple(args))
    rows.reverse()
    return jsonify(clean(rows))


@app.route("/api/alerts")
def alerts():
    limit = int(request.args.get("limit", "20"))
    minutes = int(request.args.get("minutes", "0"))
    sql = ("SELECT category, alert_type, message, metric_value, threshold_value, created_at "
           "FROM alert_event")
    args = []
    if minutes > 0:
        sql += " WHERE window_end >= %s"
        args.append(int(time.time() * 1000) - minutes * 60000)
    sql += " ORDER BY id DESC LIMIT %s"
    args.append(limit)
    return jsonify(clean(query(sql, tuple(args))))


@app.route("/api/metrics")
def metrics():
    """最近的指标明细，用于明细页；minutes>0 时只取最近 N 分钟"""
    limit = int(request.args.get("limit", "50"))
    minutes = int(request.args.get("minutes", "0"))
    sql = ("SELECT category, window_end, paid_orders, gmv, latency_ms, created_at "
           "FROM category_metric")
    args = []
    if minutes > 0:
        sql += " WHERE window_end >= %s"
        args.append(int(time.time() * 1000) - minutes * 60000)
    sql += " ORDER BY id DESC LIMIT %s"
    args.append(limit)
    return jsonify(clean(query(sql, tuple(args))))


@app.route("/api/funnel")
def funnel():
    """行为转化漏斗：按时窗汇总 pv/cart/fav/buy 数量；minutes>0 时只取最近 N 分钟"""
    limit = int(request.args.get("limit", "1"))
    minutes = int(request.args.get("minutes", "0"))
    sql = """SELECT window_end,
                    MAX(CASE WHEN behavior='pv'   THEN cnt ELSE 0 END) AS pv,
                    MAX(CASE WHEN behavior='cart' THEN cnt ELSE 0 END) AS cart,
                    MAX(CASE WHEN behavior='fav'  THEN cnt ELSE 0 END) AS fav,
                    MAX(CASE WHEN behavior='buy'  THEN cnt ELSE 0 END) AS buy
             FROM behavior_funnel"""
    args = []
    if minutes > 0:
        sql += " WHERE window_end >= %s"
        args.append(int(time.time() * 1000) - minutes * 60000)
    sql += " GROUP BY window_end ORDER BY window_end DESC LIMIT %s"
    args.append(limit)
    rows = query(sql, tuple(args))
    return jsonify(clean(rows))


@app.route("/api/quality")
def quality():
    """数据质量指标：最近 N 个窗口"""
    limit = int(request.args.get("limit", "1"))
    rows = query(
        """SELECT window_end, total, valid, invalid, invalid_rate, created_at
           FROM data_quality
           ORDER BY window_end DESC
           LIMIT %s""",
        (limit,),
    )
    return jsonify(clean(rows))


@app.route("/api/cep")
def cep():
    """CEP 检测到的复合事件"""
    limit = int(request.args.get("limit", "20"))
    rows = query(
        """SELECT user_id, product_id, category, cart_time, buy_time, gap_ms, pattern, created_at
           FROM cep_event
           ORDER BY id DESC
           LIMIT %s""",
        (limit,),
    )
    return jsonify(clean(rows))


@app.route("/api/rules", methods=["GET"])
def list_rules():
    rows = query("SELECT id, name, metric, operator, threshold, enabled, updated_at FROM alert_rule ORDER BY id")
    return jsonify(clean(rows))


@app.route("/api/rules/<int:rid>", methods=["PUT"])
def update_rule(rid):
    data = request.get_json(force=True) or {}
    threshold = float(data.get("threshold", 0))
    enabled = 1 if data.get("enabled", True) else 0
    conn = pymysql.connect(**DB)
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE alert_rule SET threshold=%s, enabled=%s WHERE id=%s",
                        (threshold, enabled, rid))
        conn.commit()
    finally:
        conn.close()
    return list_rules()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
