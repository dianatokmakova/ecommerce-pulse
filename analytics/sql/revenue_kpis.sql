-- gold.daily_revenue: выручка / заказы / AOV по дню × категория × штат
TRUNCATE TABLE gold.daily_revenue;

INSERT INTO gold.daily_revenue
SELECT
    toDate(o.purchase_ts)                       AS order_date,
    ifNull(p.category, 'unknown')               AS category,
    ifNull(c.state, 'NA')                        AS state,
    uniqExact(o.order_id)                        AS orders,
    round(sum(oi.price), 2)                      AS revenue_brl,
    count()                                       AS items,
    round(sum(oi.price) / uniqExact(o.order_id), 2) AS aov
FROM silver.order_items AS oi
INNER JOIN silver.orders   AS o ON oi.order_id = o.order_id
LEFT  JOIN silver.products AS p ON oi.product_id = p.product_id
LEFT  JOIN silver.customers AS c ON o.customer_id = c.customer_id
WHERE o.order_status != 'canceled'
GROUP BY order_date, category, state;
