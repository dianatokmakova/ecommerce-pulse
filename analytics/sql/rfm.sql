-- gold.rfm: RFM-сегментация. Скоры 1..5 через квантильные пороги.
-- (CTE завёрнут в подзапрос ради совместимости INSERT...SELECT в ClickHouse)
TRUNCATE TABLE gold.rfm;

INSERT INTO gold.rfm
SELECT * FROM
(
    WITH
    base AS (
        SELECT
            c.customer_unique_id AS customer_unique_id,
            -- assumeNotNull: скалярный подзапрос ClickHouse типизирует как Nullable,
            -- иначе recency_days тянет Nullable в arraySum-лямбды ниже (code 43)
            dateDiff('day', max(o.purchase_ts),
                     assumeNotNull((SELECT max(purchase_ts) FROM silver.orders))) AS recency_days,
            uniqExact(o.order_id)                                  AS frequency,
            round(sum(oi.price), 2)                                AS monetary
        FROM silver.orders     AS o
        INNER JOIN silver.customers   AS c ON o.customer_id = c.customer_id
        INNER JOIN silver.order_items AS oi ON oi.order_id = o.order_id
        WHERE o.order_status != 'canceled'
        GROUP BY customer_unique_id
    ),
    q AS (
        SELECT
            quantilesExact(0.2, 0.4, 0.6, 0.8)(recency_days) AS r,
            quantilesExact(0.2, 0.4, 0.6, 0.8)(frequency)    AS f,
            quantilesExact(0.2, 0.4, 0.6, 0.8)(monetary)     AS m
        FROM base
    )
    SELECT
        base.customer_unique_id,
        base.recency_days,
        base.frequency,
        base.monetary,
        toUInt8(5 - arraySum(x -> base.recency_days > x, q.r)) AS r_score,  -- меньше recency = выше скор
        toUInt8(1 + arraySum(x -> base.frequency   > x, q.f)) AS f_score,
        toUInt8(1 + arraySum(x -> base.monetary    > x, q.m)) AS m_score,
        multiIf(
            r_score >= 4 AND f_score >= 4, 'Champions',
            f_score >= 4,                  'Loyal',
            m_score >= 4,                  'Big Spenders',
            r_score >= 4,                  'New / Promising',
            r_score <= 2 AND f_score >= 3, 'At Risk',
            r_score <= 2,                  'Hibernating',
                                           'Regular'
        ) AS segment,
        today() AS calc_date
    FROM base
    CROSS JOIN q
);
