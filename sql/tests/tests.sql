-- Success condition for each test: failed_rows = 0.

SELECT 'duplicate_order_item_key' AS test_name, COUNT(*) AS failed_rows
FROM (
    SELECT order_id, order_item_id
    FROM core.order_items
    GROUP BY order_id, order_item_id
    HAVING COUNT(*) > 1
) t

UNION ALL

SELECT 'null_required_fields' AS test_name, COUNT(*) AS failed_rows
FROM core.order_items
WHERE order_id IS NULL
   OR order_item_id IS NULL
   OR customer_id IS NULL
   OR product_id IS NULL

UNION ALL

SELECT 'negative_price' AS test_name, COUNT(*) AS failed_rows
FROM core.order_items
WHERE price < 0

UNION ALL

SELECT 'negative_freight' AS test_name, COUNT(*) AS failed_rows
FROM core.order_items
WHERE freight < 0

UNION ALL

SELECT 'invalid_status' AS test_name, COUNT(*) AS failed_rows
FROM core.order_items
WHERE status IS NULL
   OR status NOT IN (
       'created',
       'approved',
       'invoiced',
       'processing',
       'shipped',
       'delivered',
       'unavailable',
       'canceled'
   )

UNION ALL

SELECT 'orphan_customer_fk' AS test_name, COUNT(*) AS failed_rows
FROM core.order_items oi
LEFT JOIN core.customers c
  ON oi.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
