-- gold.delivery_sla: качество доставки по дню × штату (план vs факт, доля просрочек)
TRUNCATE TABLE gold.delivery_sla;

INSERT INTO gold.delivery_sla
SELECT
    toDate(o.purchase_ts)                                   AS order_date,
    ifNull(c.state, 'NA')                                   AS state,
    uniqExact(o.order_id)                                   AS orders,
    round(avg(o.delivery_days), 2)                          AS avg_delivery_days,
    countIf(o.delay_days > 0)                               AS late_orders,
    round(countIf(o.delay_days > 0) / uniqExact(o.order_id), 4) AS late_share
FROM silver.orders AS o
LEFT JOIN silver.customers AS c ON o.customer_id = c.customer_id
WHERE o.is_delivered = 1
  AND o.delivered_customer_ts IS NOT NULL
GROUP BY order_date, state;
