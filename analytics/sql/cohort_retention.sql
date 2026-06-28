-- gold.cohort_retention: когорта = месяц первой покупки; retention по месяцам жизни.
TRUNCATE TABLE gold.cohort_retention;

INSERT INTO gold.cohort_retention
SELECT * FROM
(
    WITH
    first_purchase AS (
        SELECT c.customer_unique_id AS cuid,
               toStartOfMonth(min(o.purchase_ts)) AS cohort_month
        FROM silver.orders   AS o
        INNER JOIN silver.customers AS c ON o.customer_id = c.customer_id
        WHERE o.order_status != 'canceled'
        GROUP BY cuid
    ),
    activity AS (
        SELECT c.customer_unique_id AS cuid,
               toStartOfMonth(o.purchase_ts) AS active_month
        FROM silver.orders   AS o
        INNER JOIN silver.customers AS c ON o.customer_id = c.customer_id
        WHERE o.order_status != 'canceled'
        GROUP BY cuid, active_month
    ),
    joined AS (
        SELECT f.cohort_month AS cohort_month,
               toUInt8(dateDiff('month', f.cohort_month, a.active_month)) AS month_number,
               a.cuid AS cuid
        FROM first_purchase AS f
        INNER JOIN activity AS a ON f.cuid = a.cuid
    ),
    sizes AS (
        SELECT cohort_month, uniqExact(cuid) AS cohort_size
        FROM first_purchase
        GROUP BY cohort_month
    )
    SELECT
        j.cohort_month,
        j.month_number,
        s.cohort_size                                  AS customers,
        uniqExact(j.cuid)                              AS retained,
        round(uniqExact(j.cuid) / s.cohort_size, 4)    AS retention_rate
    FROM joined AS j
    INNER JOIN sizes AS s ON j.cohort_month = s.cohort_month
    GROUP BY j.cohort_month, j.month_number, s.cohort_size
    ORDER BY j.cohort_month, j.month_number
);
