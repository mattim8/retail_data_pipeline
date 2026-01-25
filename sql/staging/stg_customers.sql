CREATE OR REPLACE VIEW stg.customers AS
SELECT
    NULLIF(TRIM(customer_id), '') as customer_id,
    NULLIF(TRIM(customer_unique_id), '') as customer_unique_id,
    NULLIF(TRIM(customer_zip_code_prefix), '')::int as customer_zip_code_prefix,
    NULLIF(TRIM(customer_city), '') as customer_city,
    NULLIF(TRIM(customer_state), '') as customer_state
FROM raw.customers;
