"""
Dashboard API — тонкий read-only слой над gold-витринами ClickHouse.
Фронт (Vercel) дёргает эти эндпоинты, в проде перед ними стоит Caddy (HTTPS).

Безопасность: read-only юзер bi_reader, все запросы фиксированные
(параметров с SQL нет) → инъекция невозможна.
"""
from __future__ import annotations
import os

import clickhouse_connect
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="E-Commerce Pulse · Dashboard API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("FRONTEND_ORIGIN", "*").split(","),
    allow_methods=["GET"],
    allow_headers=["*"],
)


def ch():
    return clickhouse_connect.get_client(
        host=os.environ.get("CH_HOST", "clickhouse"),
        port=int(os.environ.get("CH_PORT", "8123")),
        username=os.environ.get("CH_USER", "bi_reader"),
        password=os.environ.get("CH_PASSWORD", "change_me_bi_password"),
    )


def rows(sql: str) -> list[dict]:
    # клиент на запрос (низкая нагрузка)
    return list(ch().query(sql).named_results())


@app.get("/api/health")
def health():
    try:
        ch().query("SELECT 1")
        return {"status": "ok"}
    except Exception as e:
        return {"status": "down", "error": str(e)}


@app.get("/api/kpis")
def kpis():
    # SummingMergeTree → всегда sum()+GROUP BY, не полагаемся на фоновый мердж
    r = rows("""
        SELECT sum(revenue_brl) AS revenue, sum(orders) AS orders, sum(items) AS items
        FROM gold.daily_revenue
    """)[0]
    customers = rows("SELECT count() AS c FROM gold.rfm FINAL")[0]["c"]
    anomalies = rows("""
        SELECT count() AS c FROM gold.pricing_anomalies
        WHERE captured_at >= now() - INTERVAL 7 DAY
    """)[0]["c"]
    aov = round(r["revenue"] / r["orders"], 2) if r["orders"] else 0
    return {**r, "aov": aov, "customers": customers, "anomalies_7d": anomalies}


@app.get("/api/revenue")
def revenue():
    return {
        "by_day": rows("""
            SELECT order_date, round(sum(revenue_brl)) AS revenue, sum(orders) AS orders
            FROM gold.daily_revenue GROUP BY order_date ORDER BY order_date
        """),
        "by_category": rows("""
            SELECT category, round(sum(revenue_brl)) AS revenue
            FROM gold.daily_revenue GROUP BY category ORDER BY revenue DESC LIMIT 10
        """),
        "by_state": rows("""
            SELECT state, round(sum(revenue_brl)) AS revenue
            FROM gold.daily_revenue GROUP BY state ORDER BY revenue DESC LIMIT 12
        """),
    }


@app.get("/api/rfm")
def rfm():
    return rows("""
        SELECT segment, count() AS customers, round(avg(monetary)) AS avg_monetary
        FROM gold.rfm FINAL GROUP BY segment ORDER BY customers DESC
    """)


@app.get("/api/cohorts")
def cohorts():
    return rows("""
        SELECT toString(cohort_month) AS cohort, month_number, retention_rate
        FROM gold.cohort_retention
        WHERE month_number <= 11
        ORDER BY cohort_month, month_number
    """)


@app.get("/api/sla")
def sla():
    # алиас НЕ называем как столбец orders — иначе sum(orders) прочитается как
    # sum(<алиас>) → вложенный агрегат, ClickHouse code 184 (ILLEGAL_AGGREGATION)
    return {
        "by_day": rows("""
            SELECT order_date,
                   sum(orders)                              AS orders_total,
                   round(sum(late_orders) / sum(orders), 4) AS late_share
            FROM gold.delivery_sla GROUP BY order_date ORDER BY order_date
        """),
        "worst_states": rows("""
            SELECT state,
                   sum(orders)                              AS orders_total,
                   round(sum(late_orders) / sum(orders), 4) AS late_share
            FROM gold.delivery_sla GROUP BY state
            HAVING orders_total > 200 ORDER BY late_share DESC LIMIT 10
        """),
    }


@app.get("/api/margin")
def margin():
    # маржа по категориям: метрика из OLTP-источника (cost_price живёт только в Postgres)
    return rows("""
        SELECT category,
               round(avg(margin_pct), 4) AS margin_pct,
               round(avg(price_brl), 2)  AS avg_price,
               round(avg(cost_price), 2) AS avg_cost,
               count() AS products
        FROM gold.product_margin FINAL
        GROUP BY category ORDER BY margin_pct DESC
    """)


@app.get("/api/anomalies")
def anomalies():
    return rows("""
        SELECT captured_at, product_id, product_category, price_brl, zscore,
               iqr_flag, isoforest_flag
        FROM gold.pricing_anomalies
        ORDER BY captured_at DESC, abs(zscore) DESC LIMIT 200
    """)
