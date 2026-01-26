CREATE TABLE IF NOT EXISTS mart.daily_sales (
    order_date DATE PRIMARY KEY,
    total_orders INT,
    total_revenue NUMERIC(18,2),
    aov NUMERIC(18,2),
    unique_customers INT
);

TRUNCATE TABLE mart.daily_sales;
INSERT INTO mart.daily_sales(order_date, total_orders, total_revenue, aov, unique_customers)
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


