WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'olist_orders') }}
),
renamed AS (
    SELECT
        order_id,
        customer_id,
        CAST(order_status AS STRING)                        AS order_status,
        CAST(order_purchase_timestamp AS TIMESTAMP)         AS order_purchase_timestamp,
        CAST(order_approved_at AS TIMESTAMP)                AS order_approved_at,
        CAST(order_delivered_carrier_date AS TIMESTAMP)     AS order_delivered_carrier_date,
        CAST(order_delivered_customer_date AS TIMESTAMP)    AS order_delivered_customer_date,
        CAST(order_estimated_delivery_date AS TIMESTAMP)    AS order_estimated_delivery_date
    FROM source
    WHERE order_id IS NOT NULL
)
SELECT * FROM renamed
