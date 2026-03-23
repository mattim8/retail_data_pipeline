DO $$
DECLARE
    total_failed BIGINT;
BEGIN
    SELECT COALESCE(SUM(failed_rows), 0) INTO total_failed
    FROM (
        SELECT COUNT(*) AS failed_rows
        FROM (
            SELECT order_id, order_item_id
            FROM core.order_items
            GROUP BY order_id, order_item_id
            HAVING COUNT(*) > 1
        ) t

        UNION ALL

        SELECT COUNT(*) AS failed_rows
        FROM core.order_items
        WHERE order_id IS NULL
           OR order_item_id IS NULL
           OR customer_id IS NULL
           OR product_id IS NULL

        UNION ALL

        SELECT COUNT(*) AS failed_rows
        FROM core.order_items
        WHERE price < 0

        UNION ALL

        SELECT COUNT(*) AS failed_rows
        FROM core.order_items
        WHERE freight < 0

        UNION ALL

        SELECT COUNT(*) AS failed_rows
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

        SELECT COUNT(*) AS failed_rows
        FROM core.order_items oi
        LEFT JOIN core.customers c
          ON oi.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    ) checks;

    IF total_failed > 0 THEN
        RAISE EXCEPTION 'Data quality tests failed: % violations found', total_failed;
    END IF;
END $$;
