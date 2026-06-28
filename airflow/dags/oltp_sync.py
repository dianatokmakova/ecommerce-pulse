"""
DAG: oltp_sync  ·  расписание @hourly
ELT из операционной БД (PostgreSQL) в ClickHouse: продуктовый каталог с себестоимостью.

Загрузка напрямую табличной функцией ClickHouse postgresql() — без промежуточного pandas
(ELT, а не ETL: трансформация уже внутри склада). Каталог мелкий → полный рефреш,
watermark-инкремент тут не нужен (он реализован в live_pricing_ingest по captured_at).

Итог: gold.product_margin = живая цена (из потока) − себестоимость (из OLTP).
"""
from __future__ import annotations
import os
from datetime import datetime

from airflow.sdk import dag, task

CH = dict(host=os.environ["CH_HOST"], port=int(os.environ["CH_PORT"]),
          username=os.environ["CH_USER"], password=os.environ["CH_PASSWORD"])

# postgresql('host:port', 'db', 'table', 'user', 'pass', 'schema') — {t} подставляется на месте
PG = "postgresql('{h}:{p}', '{db}', '{{t}}', '{u}', '{pw}', '{s}')".format(
    h=os.environ["OLTP_HOST"], p=os.environ["OLTP_PORT"], db=os.environ["OLTP_DB"],
    u=os.environ["OLTP_USER"], pw=os.environ["OLTP_PASSWORD"], s=os.environ["OLTP_SCHEMA"])


def ch_client():
    import clickhouse_connect
    return clickhouse_connect.get_client(**CH)


@dag(
    dag_id="oltp_sync",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce-pulse", "oltp", "elt"],
    default_args={"retries": 2},
)
def oltp_sync():

    @task
    def load_products() -> int:
        c = ch_client()
        c.command("TRUNCATE TABLE bronze.products_oltp")
        c.command(f"""
            INSERT INTO bronze.products_oltp
                (product_id, title, category, cost_price, is_active, updated_at)
            SELECT product_id, title, category,
                   toFloat64(cost_price), toUInt8(is_active), toDateTime(updated_at)
            FROM {PG.format(t='products')}
        """)
        return int(c.query("SELECT count() FROM bronze.products_oltp").result_rows[0][0])

    @task
    def build_catalog() -> None:
        c = ch_client()
        c.command("TRUNCATE TABLE silver.catalog")
        c.command("""
            INSERT INTO silver.catalog
            SELECT product_id, category, cost_price
            FROM bronze.products_oltp WHERE is_active = 1
        """)

    @task
    def build_margin() -> int:
        c = ch_client()
        c.command("TRUNCATE TABLE gold.product_margin")
        # CTE завёрнут в подзапрос ради совместимости INSERT...SELECT в ClickHouse
        c.command("""
            INSERT INTO gold.product_margin
            SELECT * FROM (
                WITH latest AS (
                    SELECT product_id,
                           argMax(price_brl, captured_at) AS price_brl,
                           max(captured_at)               AS last_seen
                    FROM silver.pricing GROUP BY product_id
                )
                SELECT
                    l.product_id,
                    c.category,
                    round(l.price_brl, 2)                              AS price_brl,
                    c.cost_price,
                    round(l.price_brl - c.cost_price, 2)              AS margin_brl,
                    round((l.price_brl - c.cost_price) / l.price_brl, 4) AS margin_pct,
                    l.last_seen
                FROM latest AS l
                INNER JOIN silver.catalog AS c USING (product_id)
            )
        """)
        return int(c.query("SELECT count() FROM gold.product_margin").result_rows[0][0])

    load_products() >> build_catalog() >> build_margin()


oltp_sync()
