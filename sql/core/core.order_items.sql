CREATE TABLE IF NOT EXISTS core.order_items (
    id SERIAL PRIMARY KEY,
    order_id TEXT NOT NULL,
    order_item_id INT NOT NULL,
    order_ts TIMESTAMP,
    status TEXT,
    customer_id TEXT,
    product_id TEXT,
    price NUMERIC,
    freight NUMERIC,
    seller_id TEXT,
    UNIQUE(order_id, order_item_id)
);

TRUNCATE TABLE core.order_items;
INSERT INTO core.order_items(order_id, order_item_id, order_ts, status, customer_id, product_id, price, freight, seller_id)
SELECT
    o.order_id,
    o.order_purchase_timestamp::TIMESTAMP AS order_ts,
    o.order_status AS status,
    c.customer_id,
    oi.product_id,
    oi.price::NUMERIC AS price,
    oi.freight_value::NUMERIC AS freight,
    oi.seller_id,
    oi.order_item_id AS order_item_id
FROM stg.orders o
JOIN stg.customers c ON o.customer_id = c.customer_id
JOIN stg.order_items oi ON o.order_id = oi.order_id
WHERE o.order_id IS NOT NULL
  AND c.customer_id IS NOT NULL
  AND oi.product_id IS NOT NULL;