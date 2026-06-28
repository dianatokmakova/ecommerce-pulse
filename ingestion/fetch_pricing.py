"""
Снимок цен из Fake Marketplace API (+ курс BRL→USD из Frankfurter),
Parquet в MinIO по партициям dt=YYYY-MM-DD/hour=HH.

Используется Airflow-таском live_pricing_ingest (airflow/dags/live_pricing_ingest.py).
"""
from __future__ import annotations
import io
import os
from datetime import datetime, timezone

import boto3
import httpx
import pandas as pd

FAKE_API_URL   = os.environ.get("FAKE_API_URL", "https://ecommerce-pulse-api.vercel.app/api/prices")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY      = os.environ.get("MINIO_ACCESS_KEY", "ecp_minio")
MINIO_SECRET   = os.environ.get("MINIO_SECRET_KEY", "change_me_strong_password")
RAW_BUCKET     = os.environ.get("RAW_BUCKET", "raw")


def fetch_fx_brl_usd() -> float:
    """Курс BRL→USD из Frankfurter (без ключа). При сбое — запасное значение."""
    try:
        r = httpx.get("https://api.frankfurter.app/latest",
                      params={"from": "BRL", "to": "USD"}, timeout=15)
        r.raise_for_status()
        return float(r.json()["rates"]["USD"])
    except Exception as e:                     # сбой внешнего API не должен ронять пайплайн
        print(f"[fx] fallback, reason: {e}")
        return 0.19


def fetch_prices() -> pd.DataFrame:
    """Снимок корзины товаров из Fake API."""
    r = httpx.get(FAKE_API_URL, timeout=30)
    r.raise_for_status()
    df = pd.DataFrame(r.json()["items"])       # формат: {"items": [{product_id, category, price, stock}, ...]}
    captured = datetime.now(timezone.utc).replace(microsecond=0)
    fx = fetch_fx_brl_usd()
    df = df.rename(columns={"category": "product_category"})
    df["currency"]    = "BRL"
    df["captured_at"] = captured.replace(tzinfo=None)
    df["source"]      = "fake_marketplace"
    df["price_usd"]   = (df["price"] * fx).round(4)
    return df[["product_id", "product_category", "price", "stock",
               "currency", "captured_at", "source", "price_usd"]]


def upload_parquet(df: pd.DataFrame) -> str:
    """Пишет Parquet в MinIO: raw/pricing/dt=YYYY-MM-DD/hour=HH/data.parquet"""
    now = datetime.now(timezone.utc)
    key = f"pricing/dt={now:%Y-%m-%d}/hour={now:%H}/data.parquet"
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    s3 = boto3.client("s3", endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=MINIO_KEY, aws_secret_access_key=MINIO_SECRET)
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=buf.getvalue())
    uri = f"s3://{RAW_BUCKET}/{key}"
    print(f"[minio] uploaded {len(df)} rows -> {uri}")
    return uri


def main() -> str:
    df = fetch_prices()
    return upload_parquet(df)


if __name__ == "__main__":
    main()
