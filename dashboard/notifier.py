# -*- coding: utf-8 -*-
"""告警通知推送器：轮询 alert_event 新告警并推送到 Webhook（钉钉/企业微信/通用）
环境变量:
  WEBHOOK_URL    推送地址（不配置则只记录、不推送）
  WEBHOOK_TYPE   dingtalk / wecom / generic（默认 dingtalk）
  POLL_SECONDS   轮询间隔秒（默认 5）
  MYSQL_HOST / MYSQL_PORT / MYSQL_USER / MYSQL_PASSWORD / MYSQL_DB
"""
import json
import os
import time
import urllib.request

import pymysql

DB = dict(
    host=os.getenv("MYSQL_HOST", "127.0.0.1"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "root"),
    password=os.getenv("MYSQL_PASSWORD", "root123456"),
    database=os.getenv("MYSQL_DB", "bigdata"),
    charset="utf8mb4",
)

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
WEBHOOK_TYPE = os.getenv("WEBHOOK_TYPE", "dingtalk").strip().lower()
POLL = float(os.getenv("POLL_SECONDS", "5"))
STATE = os.getenv("STATE_FILE", os.path.join(os.path.dirname(os.path.abspath(__file__)), "notifier.state"))


def load_last():
    try:
        with open(STATE, "r", encoding="utf-8") as f:
            return int(f.read().strip() or "0")
    except Exception:
        return 0


def save_last(v):
    try:
        with open(STATE, "w", encoding="utf-8") as f:
            f.write(str(v))
    except Exception:
        pass


def push(text):
    if not WEBHOOK_URL:
        return None
    if WEBHOOK_TYPE in ("dingtalk", "wecom"):
        payload = {"msgtype": "text", "text": {"content": text}}
    else:
        payload = {"text": text}
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(WEBHOOK_URL, data=data,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except Exception as e:
        print("[notifier] push failed:", e)
        return False


def main():
    last = load_last()
    if not WEBHOOK_URL:
        print("[notifier] 未配置 WEBHOOK_URL，仅记录不推送（设置后可启用钉钉/企业微信推送）")
    print("[notifier] 启动，轮询周期 %.0f 秒，起始 id=%d" % (POLL, last))
    while True:
        rows = []
        try:
            conn = pymysql.connect(**DB)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, category, alert_type, message, created_at "
                    "FROM alert_event WHERE id > %s ORDER BY id LIMIT 20", (last,))
                rows = cur.fetchall()
            conn.close()
        except Exception as e:
            print("[notifier] query failed:", e)
            time.sleep(POLL)
            continue

        for rid, category, alert_type, message, created in rows:
            text = "[%s] %s 告警：%s" % (created, alert_type, message)
            ok = push(text)
            if WEBHOOK_URL:
                print("[notifier] push id=%s -> %s" % (rid, "ok" if ok else "fail"))
            last = rid
            save_last(last)

        time.sleep(POLL)


if __name__ == "__main__":
    main()
