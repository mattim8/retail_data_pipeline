WITH daily_metrics AS (
    SELECT
        DATE(order_ts) AS order_date,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(price) AS total_revenue,
        COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(price) / NULLIF(COUNT(DISTINCT order_id), 0) AS aov
    FROM core.order_items
    WHERE status IN ('delivered', 'shipped')
    GROUP BY 1
)
SELECT
    order_date,
    total_orders,
    total_revenue,
    aov,
    unique_customers
FROM daily_metrics
ORDER BY order_date;


