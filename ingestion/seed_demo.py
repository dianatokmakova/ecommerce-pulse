"""
Офлайн-сид данных живого домена (пока Fake API не задеплоен на Vercel):
  1) генерирует историю цен той же логикой, что fake-marketplace-api -> bronze.pricing_raw -> silver.pricing
  2) тянет каталог из OLTP (Postgres) -> silver.catalog -> gold.product_margin
  3) гоняет реальный детектор аномалий проекта -> gold.pricing_anomalies

Запуск в окружении airflow-образа (есть clickhouse-connect, pandas, sklearn, доступ к сети):
  docker compose run --rm airflow-scheduler python /opt/airflow/ingestion/seed_demo.py
"""
from __future__ import annotations
import hashlib
import math
import os
import random
import sys
from datetime import datetime, timedelta, timezone

import clickhouse_connect

CH = dict(host=os.environ.get("CH_HOST", "clickhouse"),
          port=int(os.environ.get("CH_PORT", "8123")),
          username=os.environ.get("CH_USER", "analyst"),
          password=os.environ.get("CH_PASSWORD", "change_me_strong_password"))

DAYS = int(os.environ.get("SEED_DAYS", "14"))
N_PRODUCTS = 50
CATEGORIES = ["electronics", "home", "toys", "fashion", "beauty",
              "sports", "garden", "auto", "books", "grocery"]
ANOMALY_RATE = 0.04
FX = 0.19  # BRL->USD (как fallback в fetch_pricing)


def _seed(pid: str) -> int:
    return int(hashlib.md5(pid.encode()).hexdigest(), 16)


def _base_price(pid: str) -> float:
    return round(random.Random(_seed(pid)).uniform(20, 500), 2)


def snapshot(ts: datetime) -> list[tuple]:
    """Снимок цен на момент ts — повторяет логику fake-marketplace-api/api/prices.py."""
    hour_f = math.sin(ts.hour / 24 * 2 * math.pi)
    dow_f = 0.05 if ts.weekday() >= 5 else 0.0
    out = []
    for i in range(N_PRODUCTS):
        pid = f"SKU-{i:04d}"
        price = _base_price(pid) * (1 + 0.08 * hour_f + dow_f + random.uniform(-0.03, 0.03))
        stock = random.randint(0, 200)
        if random.random() < ANOMALY_RATE:                 # подмешиваем аномалию
            if random.random() < 0.6:
                price *= 1.40
            else:
                stock = 0
        out.append((pid, CATEGORIES[_seed(pid) % len(CATEGORIES)], round(price, 2), stock))
    return out


def main():
    c = clickhouse_connect.get_client(**CH)

    # 1) история цен -> bronze.pricing_raw
    c.command("TRUNCATE TABLE bronze.pricing_raw")
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0, tzinfo=None)
    rows = []
    for h in range(DAYS * 24, 0, -1):
        ts = now - timedelta(hours=h)
        for pid, cat, price, stock in snapshot(ts):
            rows.append([pid, cat, float(price), int(stock), "BRL", ts,
                         "fake_marketplace", round(price * FX, 4)])
    c.insert("bronze.pricing_raw", rows,
             column_names=["product_id", "product_category", "price", "stock",
                           "currency", "captured_at", "source", "price_usd"])
    print(f"bronze.pricing_raw: {len(rows)} строк ({DAYS}д × {N_PRODUCTS} SKU)")

    # 2) silver.pricing
    c.command("TRUNCATE TABLE silver.pricing")
    c.command("""
        INSERT INTO silver.pricing
            (product_id, product_category, price_brl, price_usd, stock, captured_at, source)
        SELECT product_id, product_category, price, price_usd, stock, captured_at, source
        FROM bronze.pricing_raw
    """)

    # 3) OLTP каталог -> silver.catalog -> gold.product_margin
    pg = "postgresql('{h}:{p}', '{db}', '{{t}}', '{u}', '{pw}', '{s}')".format(
        h=os.environ.get("OLTP_HOST", "oltp"), p=os.environ.get("OLTP_PORT", "5432"),
        db=os.environ.get("OLTP_DB", "marketplace"), u=os.environ.get("OLTP_USER", "shop"),
        pw=os.environ["OLTP_PASSWORD"], s=os.environ.get("OLTP_SCHEMA", "shop"))
    c.command("TRUNCATE TABLE bronze.products_oltp")
    c.command(f"""INSERT INTO bronze.products_oltp
            (product_id, title, category, cost_price, is_active, updated_at)
        SELECT product_id, title, category,
               toFloat64(cost_price), toUInt8(is_active), toDateTime(updated_at)
        FROM {pg.format(t='products')}""")
    c.command("TRUNCATE TABLE silver.catalog")
    c.command("INSERT INTO silver.catalog "
              "SELECT product_id, category, cost_price FROM bronze.products_oltp WHERE is_active = 1")
    c.command("TRUNCATE TABLE gold.product_margin")
    c.command("""INSERT INTO gold.product_margin SELECT * FROM (
        WITH latest AS (
            SELECT product_id, argMax(price_brl, captured_at) AS price_brl, max(captured_at) AS last_seen
            FROM silver.pricing GROUP BY product_id)
        SELECT l.product_id, ct.category, round(l.price_brl, 2), ct.cost_price,
               round(l.price_brl - ct.cost_price, 2),
               round((l.price_brl - ct.cost_price) / l.price_brl, 4), l.last_seen
        FROM latest AS l INNER JOIN silver.catalog AS ct USING (product_id))""")
    print(f"silver.catalog + gold.product_margin: {c.query('SELECT count() FROM gold.product_margin').result_rows[0][0]} товаров")

    # 4) аномалии — реальный модуль проекта
    c.command("TRUNCATE TABLE gold.pricing_anomalies")
    sys.path.append("/opt/airflow/analytics/anomaly")
    from detect_anomalies import main as anomaly_main
    print(f"gold.pricing_anomalies: {anomaly_main()} аномалий")


if __name__ == "__main__":
    main()
