"""
DAG: olist_batch_load  ·  расписание @daily
Грузит Olist CSV (MinIO raw/olist/*.csv) -> bronze -> silver -> gold-витрины,
затем прогоняет data-quality проверки.

Источник: Olist CSV в MinIO raw/olist/ (имена файлов в FILE_MAP).
Authoring: Airflow 3.x (TaskFlow API).
"""
from __future__ import annotations
import os
from datetime import datetime

from airflow.sdk import dag, task

CH = dict(host=os.environ["CH_HOST"], port=int(os.environ["CH_PORT"]),
          username=os.environ["CH_USER"], password=os.environ["CH_PASSWORD"])
MINIO_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET = os.environ["MINIO_SECRET_KEY"]

# bronze-таблица  ->  файл в raw/olist/
FILE_MAP = {
    "olist_orders":                 "olist_orders_dataset.csv",
    "olist_order_items":            "olist_order_items_dataset.csv",
    "olist_order_reviews":          "olist_order_reviews_dataset.csv",
    "olist_customers":              "olist_customers_dataset.csv",
    "olist_products":               "olist_products_dataset.csv",
    "olist_sellers":                "olist_sellers_dataset.csv",
    "product_category_translation": "product_category_name_translation.csv",
}


def ch_client():
    import clickhouse_connect
    return clickhouse_connect.get_client(**CH)


def run_sql_file(client, path: str) -> None:
    """Прогоняет .sql c несколькими запросами (разделитель ';')."""
    with open(path) as f:
        sql = f.read()
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        client.command(stmt)


@dag(
    dag_id="olist_batch_load",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce-pulse", "batch", "olist"],
    default_args={"retries": 1},
)
def olist_batch_load():

    @task
    def load_bronze() -> dict:
        client = ch_client()
        counts = {}
        for table, fname in FILE_MAP.items():
            client.command(f"TRUNCATE TABLE IF EXISTS bronze.{table}")
            # CSVWithNames: ClickHouse сам выведет схему по структуре таблицы
            client.command(f"""
                INSERT INTO bronze.{table}
                SELECT * FROM s3('http://minio:9000/raw/olist/{fname}',
                                 '{MINIO_KEY}', '{MINIO_SECRET}', 'CSVWithNames')
            """)
            counts[table] = int(
                client.query(f"SELECT count() FROM bronze.{table}").result_rows[0][0]
            )
        print("bronze counts:", counts)
        return counts

    @task
    def build_silver() -> None:
        client = ch_client()
        # orders: парсим даты, считаем дни доставки и просрочку
        client.command("TRUNCATE TABLE silver.orders")
        client.command("""
            INSERT INTO silver.orders
            SELECT
                order_id, customer_id, order_status,
                parseDateTimeBestEffort(order_purchase_timestamp)              AS purchase_ts,
                parseDateTimeBestEffortOrNull(order_approved_at)               AS approved_ts,
                parseDateTimeBestEffortOrNull(order_delivered_customer_date)   AS delivered_customer_ts,
                parseDateTimeBestEffortOrNull(order_estimated_delivery_date)   AS estimated_delivery_ts,
                order_status = 'delivered'                                     AS is_delivered,
                dateDiff('day', purchase_ts, delivered_customer_ts)           AS delivery_days,
                dateDiff('day', estimated_delivery_ts, delivered_customer_ts) AS delay_days,
                toStartOfMonth(purchase_ts)                                   AS order_month
            FROM bronze.olist_orders
            WHERE order_purchase_timestamp IS NOT NULL
        """)
        client.command("TRUNCATE TABLE silver.order_items")
        client.command("""
            INSERT INTO silver.order_items
            SELECT order_id, toUInt16(order_item_id), product_id, seller_id,
                   toFloat64OrZero(price), toFloat64OrZero(freight_value)
            FROM bronze.olist_order_items
        """)
        client.command("TRUNCATE TABLE silver.customers")
        client.command("""
            INSERT INTO silver.customers
            SELECT customer_id, customer_unique_id, ifNull(customer_zip_code_prefix,''),
                   ifNull(customer_city,''), ifNull(customer_state,'')
            FROM bronze.olist_customers
        """)
        client.command("TRUNCATE TABLE silver.products")
        client.command("""
            INSERT INTO silver.products
            SELECT p.product_id,
                   ifNull(t.product_category_name_english, ifNull(p.product_category_name,'unknown')),
                   toInt32OrNull(p.product_weight_g), toInt16OrNull(p.product_photos_qty)
            FROM bronze.olist_products p
            LEFT JOIN bronze.product_category_translation t USING (product_category_name)
        """)
        client.command("TRUNCATE TABLE silver.sellers")
        client.command("""
            INSERT INTO silver.sellers
            SELECT seller_id, ifNull(seller_zip_code_prefix,''),
                   ifNull(seller_city,''), ifNull(seller_state,'')
            FROM bronze.olist_sellers
        """)
        client.command("TRUNCATE TABLE silver.reviews")
        client.command("""
            INSERT INTO silver.reviews
            SELECT review_id, order_id, toUInt8OrZero(review_score),
                   parseDateTimeBestEffortOrNull(review_creation_date)
            FROM bronze.olist_order_reviews
        """)

    @task
    def build_gold() -> None:
        client = ch_client()
        for f in ["revenue_kpis.sql", "rfm.sql", "cohort_retention.sql", "delivery_sla.sql"]:
            run_sql_file(client, f"/opt/airflow/analytics/sql/{f}")

    @task
    def data_quality_checks() -> None:
        """Простые DQ-проверки. Падают (assert) -> таск красный -> Airflow шлёт алерт."""
        client = ch_client()

        def scalar(q): return client.query(q).result_rows[0][0]

        # 1. в заказах не должно быть пустых order_id
        empty_ids = scalar("SELECT count() FROM silver.orders WHERE order_id = ''")
        assert empty_ids == 0, f"DQ: пустые order_id = {empty_ids}"

        # 2. витрина выручки не пустая
        rev_rows = scalar("SELECT count() FROM gold.daily_revenue")
        assert rev_rows > 0, "DQ: gold.daily_revenue пустая"

        # 3. RFM покрывает разумную долю клиентов
        rfm_rows = scalar("SELECT count() FROM gold.rfm")
        cust = scalar("SELECT uniqExact(customer_unique_id) FROM silver.customers")
        assert rfm_rows > 0.5 * cust, f"DQ: RFM покрывает мало клиентов ({rfm_rows}/{cust})"

        print(f"DQ OK · revenue_rows={rev_rows} · rfm={rfm_rows}/{cust}")

    load_bronze() >> build_silver() >> build_gold() >> data_quality_checks()


olist_batch_load()
