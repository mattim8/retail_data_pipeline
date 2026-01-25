CREATE OR REPLACE VIEW stg.order_items AS
SELECT
    NULLIF(TRIM(order_id), '') as order_id,
    NULLIF(TRIM(order_item_id), '')::int as order_item_id,
    NULLIF(TRIM(product_id), '') as product_id,
    NULLIF(TRIM(seller_id), '') as seller_id,
    NULLIF(TRIM(shipping_limit_date),'')::timestamp as shipping_limit_date,
    NULLIF(TRIM(price), '')::numeric as price,
    NULLIF(TRIM(freight_value), '')::numeric as freight_value
FROM raw.order_items;