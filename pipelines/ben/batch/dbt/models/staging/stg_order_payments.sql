WITH source AS (SELECT * FROM {{ source('olist_raw', 'olist_order_payments') }}),
renamed AS (
    SELECT
        CONCAT(order_id, '_', payment_sequential) AS payment_key,
        order_id,
        CAST(payment_sequential AS INT64)    AS payment_sequential,
        CAST(payment_type AS STRING)         AS payment_type,
        CAST(payment_installments AS INT64)  AS payment_installments,
        CAST(payment_value AS FLOAT64)       AS payment_value
    FROM source WHERE order_id IS NOT NULL
)
SELECT * FROM renamed
