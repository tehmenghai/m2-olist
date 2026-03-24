WITH source AS (SELECT * FROM {{ source('olist_raw', 'olist_order_items') }}),
renamed AS (
    SELECT
        CONCAT(order_id, '_', CAST(order_item_id AS STRING)) AS order_item_key,
        order_id,
        CAST(order_item_id AS INT64)       AS order_item_id,
        product_id,
        seller_id,
        CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
        CAST(price AS FLOAT64)             AS price,
        CAST(freight_value AS FLOAT64)     AS freight_value
    FROM source WHERE order_id IS NOT NULL
)
SELECT * FROM renamed
