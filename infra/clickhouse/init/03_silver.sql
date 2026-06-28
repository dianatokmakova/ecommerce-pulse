-- SILVER: очищенные и типизированные таблицы. Заполняются SQL-задачей из Airflow
-- (INSERT INTO silver.* SELECT ... FROM bronze.*), здесь только DDL.

CREATE TABLE IF NOT EXISTS silver.orders (
    order_id              String,
    customer_id           String,
    order_status          LowCardinality(String),
    purchase_ts           DateTime,
    approved_ts           Nullable(DateTime),
    delivered_customer_ts Nullable(DateTime),
    estimated_delivery_ts Nullable(DateTime),
    is_delivered          UInt8,
    delivery_days         Nullable(Int32),   -- факт. дни доставки
    delay_days            Nullable(Int32),   -- (+) просрочка, (-) раньше срока
    order_month           Date                -- первый день месяца покупки
) ENGINE = MergeTree
PARTITION BY toYYYYMM(purchase_ts)
ORDER BY (order_id);

CREATE TABLE IF NOT EXISTS silver.order_items (
    order_id      String,
    order_item_id UInt16,
    product_id    String,
    seller_id     String,
    price         Float64,
    freight_value Float64
) ENGINE = MergeTree ORDER BY (order_id, order_item_id);

CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id        String,
    customer_unique_id String,
    zip_prefix         String,
    city               String,
    state              LowCardinality(String)
) ENGINE = MergeTree ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS silver.products (
    product_id    String,
    category      LowCardinality(String),     -- уже на английском (через справочник)
    weight_g      Nullable(Int32),
    photos_qty    Nullable(Int16)
) ENGINE = MergeTree ORDER BY product_id;

CREATE TABLE IF NOT EXISTS silver.sellers (
    seller_id  String,
    zip_prefix String,
    city       String,
    state      LowCardinality(String)
) ENGINE = MergeTree ORDER BY seller_id;

CREATE TABLE IF NOT EXISTS silver.reviews (
    review_id     String,
    order_id      String,
    review_score  UInt8,
    created_date  Nullable(DateTime)
) ENGINE = MergeTree ORDER BY order_id;

-- Каталог из OLTP: product_id → категория + себестоимость (для расчёта маржи)
CREATE TABLE IF NOT EXISTS silver.catalog (
    product_id String,
    category   LowCardinality(String),
    cost_price Float64
) ENGINE = MergeTree ORDER BY product_id;

-- Цены: дедуплицируем по (product_id, captured_at). ReplacingMergeTree схлопывает дубли.
CREATE TABLE IF NOT EXISTS silver.pricing (
    product_id       String,
    product_category LowCardinality(String),
    price_brl        Float64,
    price_usd        Float64,           -- через курс Frankfurter
    stock            Int32,
    captured_at      DateTime,
    source           LowCardinality(String)
) ENGINE = ReplacingMergeTree
ORDER BY (product_id, captured_at);
