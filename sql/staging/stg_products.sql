CREATE OR REPLACE VIEW stg.products AS
SELECT
    NULLIF(TRIM(product_id), '') AS product_id,
    NULLIF(TRIM(product_category_name), '') AS product_category_name,
    NULLIF(TRIM(product_name_lenght), '')::integer AS product_name_lenght,
    NULLIF(TRIM(product_description_lenght), '')::integer AS product_description_lenght,
    NULLIF(TRIM(product_photos_qty), '')::integer AS product_photos_qty,
    NULLIF(TRIM(product_weight_g), '')::integer AS product_weight_g,
    NULLIF(TRIM(product_length_cm), '')::integer AS product_length_cm,
    NULLIF(TRIM(product_height_cm), '')::integer AS product_height_cm,
    NULLIF(TRIM(product_width_cm), '')::integer AS product_width_cm
FROM raw.products;