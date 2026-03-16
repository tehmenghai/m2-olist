WITH source AS (SELECT * FROM {{ source('olist_raw', 'customers') }}),
masked AS (
    SELECT
        customer_id,
        customer_unique_id,
        TO_HEX(MD5(CAST(customer_zip_code_prefix AS STRING))) AS customer_zip_hash,
        CAST(customer_city AS STRING)                         AS customer_city,
        CAST(customer_state AS STRING)                        AS customer_state
    FROM source WHERE customer_id IS NOT NULL
)
SELECT * FROM masked
