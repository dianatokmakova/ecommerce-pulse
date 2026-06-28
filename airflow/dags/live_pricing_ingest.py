"""
DAG: live_pricing_ingest  ·  расписание @hourly
Поток: Fake API + FX -> Parquet в MinIO -> bronze.pricing_raw -> silver.pricing -> детекция аномалий.

Authoring: Airflow 3.x (TaskFlow API).
"""
from __future__ import annotations
import os
import sys
from datetime import datetime

from airflow.sdk import dag, task

# модули проекта примонтированы в docker-compose — добавляются в sys.path
sys.path.append("/opt/airflow/ingestion")
sys.path.append("/opt/airflow/analytics/anomaly")

CH = dict(host=os.environ["CH_HOST"], port=int(os.environ["CH_PORT"]),
          username=os.environ["CH_USER"], password=os.environ["CH_PASSWORD"])
MINIO_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET = os.environ["MINIO_SECRET_KEY"]


def ch_client():
    import clickhouse_connect
    return clickhouse_connect.get_client(**CH)


@dag(
    dag_id="live_pricing_ingest",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce-pulse", "streaming", "pricing"],
    default_args={"retries": 2},
)
def live_pricing_ingest():

    @task
    def fetch_to_minio() -> str:
        from fetch_pricing import main as fetch_main   # ingestion/fetch_pricing.py
        return fetch_main()                             # -> s3://raw/pricing/dt=.../hour=.../data.parquet

    @task
    def load_bronze(s3_uri: str) -> int:
        # s3://raw/<key>  ->  http://minio:9000/raw/<key>
        key = s3_uri.replace("s3://raw/", "")
        client = ch_client()
        client.command(f"""
            INSERT INTO bronze.pricing_raw
                (product_id, product_category, price, stock, currency, captured_at, source, price_usd)
            SELECT product_id, product_category, price, stock, currency, captured_at, source, price_usd
            FROM s3('http://minio:9000/raw/{key}',
                    '{MINIO_KEY}', '{MINIO_SECRET}', 'Parquet')
        """)
        return int(client.query("SELECT count() FROM bronze.pricing_raw").result_rows[0][0])

    @task
    def build_silver() -> None:
        # переносим новые строки в silver.pricing (ReplacingMergeTree сам схлопнет дубли)
        ch_client().command("""
            INSERT INTO silver.pricing
                (product_id, product_category, price_brl, price_usd, stock, captured_at, source)
            SELECT product_id, product_category, price AS price_brl, price_usd, stock, captured_at, source
            FROM bronze.pricing_raw
            WHERE captured_at >= now() - INTERVAL 2 HOUR
        """)

    @task
    def run_anomaly_detection() -> int:
        from detect_anomalies import main as anomaly_main   # analytics/anomaly/detect_anomalies.py
        return anomaly_main()

    uri = fetch_to_minio()
    rows = load_bronze(uri)
    silver = build_silver()
    anomaly = run_anomaly_detection()
    # порядок: bronze -> silver -> аномалии (XComArg поддерживает >>)
    rows >> silver >> anomaly


live_pricing_ingest()
