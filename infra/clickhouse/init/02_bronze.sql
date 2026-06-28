-- BRONZE: данные «как пришли». Минимум типизации (String/Nullable), без бизнес-логики.
-- Olist (7 таблиц, что реально используются в silver/gold) + сырые снимки цен из live-потока.

CREATE TABLE IF NOT EXISTS bronze.olist_orders (
    order_id                       String,
    customer_id                    String,
    order_status                   String,
    order_purchase_timestamp       Nullable(String),
    order_approved_at              Nullable(String),
    order_delivered_carrier_date   Nullable(String),
    order_delivered_customer_date  Nullable(String),
    order_estimated_delivery_date  Nullable(String),
    _loaded_at                     DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY order_id;

CREATE TABLE IF NOT EXISTS bronze.olist_order_items (
    order_id            String,
    order_item_id       String,
    product_id          String,
    seller_id           String,
    shipping_limit_date Nullable(String),
    price               Nullable(String),
    freight_value       Nullable(String),
    _loaded_at          DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY (order_id, order_item_id);

CREATE TABLE IF NOT EXISTS bronze.olist_order_reviews (
    review_id               String,
    order_id                String,
    review_score            Nullable(String),
    review_comment_title    Nullable(String),
    review_comment_message  Nullable(String),
    review_creation_date    Nullable(String),
    review_answer_timestamp Nullable(String),
    _loaded_at              DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY order_id;

CREATE TABLE IF NOT EXISTS bronze.olist_customers (
    customer_id              String,
    customer_unique_id       String,
    customer_zip_code_prefix Nullable(String),
    customer_city            Nullable(String),
    customer_state           Nullable(String),
    _loaded_at               DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS bronze.olist_products (
    product_id                 String,
    product_category_name      Nullable(String),
    product_name_lenght        Nullable(String),
    product_description_lenght Nullable(String),
    product_photos_qty         Nullable(String),
    product_weight_g           Nullable(String),
    product_length_cm          Nullable(String),
    product_height_cm          Nullable(String),
    product_width_cm           Nullable(String),
    _loaded_at                 DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY product_id;

CREATE TABLE IF NOT EXISTS bronze.olist_sellers (
    seller_id              String,
    seller_zip_code_prefix Nullable(String),
    seller_city            Nullable(String),
    seller_state           Nullable(String),
    _loaded_at             DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY seller_id;

CREATE TABLE IF NOT EXISTS bronze.product_category_translation (
    product_category_name         String,
    product_category_name_english Nullable(String),
    _loaded_at                    DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY product_category_name;

-- Live-поток цен: сырые снимки (по одному на товар на запуск DAG)
CREATE TABLE IF NOT EXISTS bronze.pricing_raw (
    product_id        String,
    product_category  String,
    price             Float64,
    stock             Int32,
    currency          String,
    captured_at       DateTime,
    source            String,
    price_usd         Float64,
    _loaded_at        DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (product_id, captured_at);

-- Каталог из OLTP (PostgreSQL) — тянется ELT-таском oltp_sync через postgresql()
CREATE TABLE IF NOT EXISTS bronze.products_oltp (
    product_id String,
    title      String,
    category   String,
    cost_price Float64,
    is_active  UInt8,
    updated_at DateTime,
    _loaded_at DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY product_id;
