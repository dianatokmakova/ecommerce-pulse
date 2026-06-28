-- GOLD: витрины под дашборды. BI читает ТОЛЬКО отсюда. Заполняются из Airflow.

-- Выручка по дням × категория × штат
CREATE TABLE IF NOT EXISTS gold.daily_revenue (
    order_date   Date,
    category     LowCardinality(String),
    state        LowCardinality(String),
    orders       UInt32,
    revenue_brl  Float64,
    items        UInt32,
    aov          Float64           -- средний чек
) ENGINE = SummingMergeTree
ORDER BY (order_date, category, state);

-- RFM-сегментация клиентов
CREATE TABLE IF NOT EXISTS gold.rfm (
    customer_unique_id String,
    recency_days       UInt32,
    frequency          UInt32,
    monetary           Float64,
    r_score            UInt8,
    f_score            UInt8,
    m_score            UInt8,
    segment            LowCardinality(String),   -- Champions / At Risk / ...
    calc_date          Date
) ENGINE = ReplacingMergeTree(calc_date)
ORDER BY customer_unique_id;

-- Когортный retention: когорта = месяц первой покупки
CREATE TABLE IF NOT EXISTS gold.cohort_retention (
    cohort_month   Date,
    month_number   UInt8,            -- 0,1,2,... месяцев с первой покупки
    customers      UInt32,
    retained       UInt32,
    retention_rate Float64
) ENGINE = MergeTree
ORDER BY (cohort_month, month_number);

-- SLA доставки
CREATE TABLE IF NOT EXISTS gold.delivery_sla (
    order_date     Date,
    state          LowCardinality(String),
    orders         UInt32,
    avg_delivery_days Float64,
    late_orders    UInt32,
    late_share     Float64
) ENGINE = SummingMergeTree
ORDER BY (order_date, state);

-- Маржа: живая цена (поток) − себестоимость (OLTP/Postgres). Пишет DAG oltp_sync.
CREATE TABLE IF NOT EXISTS gold.product_margin (
    product_id String,
    category   LowCardinality(String),
    price_brl  Float64,
    cost_price Float64,
    margin_brl Float64,
    margin_pct Float64,
    last_seen  DateTime
) ENGINE = ReplacingMergeTree(last_seen)
ORDER BY product_id;

-- Аномалии цен (пишет detect_anomalies.py)
CREATE TABLE IF NOT EXISTS gold.pricing_anomalies (
    product_id       String,
    product_category LowCardinality(String),
    captured_at      DateTime,
    price_brl        Float64,
    zscore           Float64,
    iqr_flag         UInt8,
    isoforest_flag   UInt8,
    is_anomaly       UInt8,            -- финальный флаг (any метод)
    detected_at      DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (product_id, captured_at);
