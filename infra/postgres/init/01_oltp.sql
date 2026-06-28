-- OLTP: операционная БД маркетплейса (PostgreSQL). Product master с себестоимостью
-- (cost_price); источник для ELT в ClickHouse.
CREATE SCHEMA IF NOT EXISTS shop;

CREATE TABLE IF NOT EXISTS shop.products (
    product_id  text PRIMARY KEY,
    title       text NOT NULL,
    category    text NOT NULL,
    cost_price  numeric(10,2) NOT NULL,          -- закупочная цена, BRL
    is_active   boolean NOT NULL DEFAULT true,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- сид: те же 50 SKU, что отдаёт Fake API (SKU-0000..SKU-0049), чтобы join по product_id сошёлся
INSERT INTO shop.products (product_id, title, category, cost_price)
SELECT
    format('SKU-%s', lpad(g::text, 4, '0')),
    format('Product %s', g),
    (ARRAY['electronics','home','toys','fashion','beauty',
           'sports','garden','auto','books','grocery'])[1 + (g % 10)],
    round((30 + random() * 200)::numeric, 2)
FROM generate_series(0, 49) AS g
ON CONFLICT (product_id) DO NOTHING;

-- индекс под watermark-инкремент по updated_at (каталог мелкий → полный рефреш)
CREATE INDEX IF NOT EXISTS ix_products_updated ON shop.products(updated_at);
