WITH orders AS (SELECT * FROM {{ ref('stg_orders') }}),
items AS (
    SELECT
        order_id,
        COUNT(*)                  AS item_count,
        SUM(price)               AS items_total,
        SUM(freight_value)       AS freight_total,
        SUM(price + freight_value) AS order_value,
        STRING_AGG(DISTINCT seller_id, ',') AS seller_ids
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
),
payments AS (
    SELECT
        order_id,
        SUM(payment_value)        AS payment_value,
        MAX(payment_type)         AS payment_type,
        MAX(payment_installments) AS payment_installments
    FROM {{ ref('stg_order_payments') }}
    GROUP BY order_id
),
reviews AS (
    SELECT
        order_id,
        MAX(review_score)          AS review_score,
        MAX(review_comment_message) AS review_comment
    FROM {{ ref('stg_order_reviews') }}
    GROUP BY order_id
),
products AS (
    SELECT
        oi.order_id,
        MAX(p.product_category_name) AS product_category_name,
        -- map category to English (most common categories)
        MAX(CASE p.product_category_name
            WHEN 'cama_mesa_banho' THEN 'Bed Bath Table'
            WHEN 'beleza_saude' THEN 'Health Beauty'
            WHEN 'esporte_lazer' THEN 'Sports Leisure'
            WHEN 'moveis_decoracao' THEN 'Furniture Decor'
            WHEN 'informatica_acessorios' THEN 'Computers Accessories'
            WHEN 'utilidades_domesticas' THEN 'Housewares'
            WHEN 'relogios_presentes' THEN 'Watches Gifts'
            WHEN 'telefonia' THEN 'Telephony'
            WHEN 'ferramentas_jardim' THEN 'Garden Tools'
            WHEN 'automotivo' THEN 'Auto'
            ELSE COALESCE(p.product_category_name, 'Other')
        END) AS product_category_name_english
    FROM {{ ref('stg_order_items') }} oi
    LEFT JOIN {{ ref('stg_products') }} p USING (product_id)
    GROUP BY oi.order_id
),
sellers AS (
    SELECT
        oi.order_id,
        MAX(s.seller_state) AS seller_state
    FROM {{ ref('stg_order_items') }} oi
    LEFT JOIN {{ ref('stg_sellers') }} s USING (seller_id)
    GROUP BY oi.order_id
)
SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    DATE_DIFF(
        DATE(o.order_delivered_customer_date),
        DATE(o.order_estimated_delivery_date),
        DAY
    )                                                     AS delivery_delay_days,
    CASE
        WHEN o.order_delivered_customer_date IS NULL THEN NULL
        WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date THEN TRUE
        ELSE FALSE
    END                                                   AS on_time_delivery,
    COALESCE(i.item_count, 0)                             AS item_count,
    COALESCE(i.items_total, 0)                            AS items_total,
    COALESCE(i.freight_total, 0)                          AS freight_total,
    COALESCE(p.payment_value, i.order_value, 0)           AS payment_value,
    p.payment_type,
    COALESCE(p.payment_installments, 1)                   AS payment_installments,
    r.review_score,
    r.review_comment,
    pr.product_category_name,
    pr.product_category_name_english,
    sl.seller_state
FROM orders o
LEFT JOIN items   i  USING (order_id)
LEFT JOIN payments p USING (order_id)
LEFT JOIN reviews  r USING (order_id)
LEFT JOIN products pr USING (order_id)
LEFT JOIN sellers  sl USING (order_id)
