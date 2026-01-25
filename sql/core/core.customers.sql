CREATE TABLE IF NOT EXISTS core.customers (
    id SERIAL PRIMARY KEY,
    customer_id TEXT UNIQUE,
    customer_unique_id TEXT,
    customer_zip_code_prefix INT,
    customer_city TEXT,
    customer_state TEXT
);

TRUNCATE TABLE core.customers;
INSERT INTO core.customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM stg.customers
WHERE customer_id IS NOT NULL;




