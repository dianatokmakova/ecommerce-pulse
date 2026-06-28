"""
Детекция аномалий цен. Берёт последние N дней из silver.pricing,
считает аномалии ДВУМЯ способами и сравнивает их:
  1) статистика: z-score по истории товара + правило IQR (межквартильный размах);
  2) ML: IsolationForest (scikit-learn).
Финальный флаг is_anomaly = (zscore-аномалия) OR (iqr) OR (isoforest).
Результат пишется в gold.pricing_anomalies.
"""
from __future__ import annotations
import os

import clickhouse_connect
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

CH_HOST = os.environ.get("CH_HOST", "clickhouse")
CH_PORT = int(os.environ.get("CH_PORT", "8123"))
CH_USER = os.environ.get("CH_USER", "analyst")
CH_PASSWORD = os.environ.get("CH_PASSWORD", "change_me_strong_password")

LOOKBACK_DAYS = int(os.environ.get("ANOMALY_LOOKBACK_DAYS", "14"))
Z_THRESHOLD = 3.0          # |z| > 3 -> аномалия
CONTAMINATION = 0.02       # ожидаемая доля аномалий для IsolationForest


def get_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD
    )


def load_prices(client) -> pd.DataFrame:
    q = f"""
        SELECT product_id, product_category, price_brl, captured_at
        FROM silver.pricing
        WHERE captured_at >= now() - INTERVAL {LOOKBACK_DAYS} DAY
        ORDER BY product_id, captured_at
    """
    return client.query_df(q)


def zscore_iqr(g: pd.DataFrame) -> pd.DataFrame:
    """Статистические флаги по истории одного товара."""
    p = g["price_brl"].astype(float)
    mu, sd = p.mean(), p.std(ddof=0)
    g["zscore"] = 0.0 if sd == 0 else (p - mu) / sd
    q1, q3 = p.quantile(0.25), p.quantile(0.75)
    iqr = q3 - q1
    lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    g["iqr_flag"] = (((p < lo) | (p > hi)) & (iqr > 0)).astype("uint8")
    return g


def detect(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    # 1) статистика по каждому товару
    df = df.groupby("product_id", group_keys=False).apply(zscore_iqr)

    # 2) ML — IsolationForest на «фичах» (цена + |z|)
    feats = df[["price_brl", "zscore"]].fillna(0.0).to_numpy()
    iso = IsolationForest(contamination=CONTAMINATION, random_state=42)
    df["isoforest_flag"] = (iso.fit_predict(feats) == -1).astype("uint8")

    df["zscore"] = df["zscore"].round(4)
    z_flag = (df["zscore"].abs() > Z_THRESHOLD).astype("uint8")
    df["is_anomaly"] = ((z_flag == 1) | (df["iqr_flag"] == 1) | (df["isoforest_flag"] == 1)).astype("uint8")
    return df


def write_anomalies(client, df: pd.DataFrame) -> int:
    out = df[["product_id", "product_category", "captured_at", "price_brl",
              "zscore", "iqr_flag", "isoforest_flag", "is_anomaly"]].copy()
    out = out[out["is_anomaly"] == 1]                 # пишем только аномалии
    if out.empty:
        print("[anomaly] аномалий не найдено")
        return 0
    client.insert_df("gold.pricing_anomalies", out)
    print(f"[anomaly] записано {len(out)} аномалий "
          f"(z>{Z_THRESHOLD}: {(df['zscore'].abs() > Z_THRESHOLD).sum()}, "
          f"iqr: {df['iqr_flag'].sum()}, iso: {df['isoforest_flag'].sum()})")
    return len(out)


def main() -> int:
    client = get_client()
    df = load_prices(client)
    df = detect(df)
    return write_anomalies(client, df)


if __name__ == "__main__":
    main()
