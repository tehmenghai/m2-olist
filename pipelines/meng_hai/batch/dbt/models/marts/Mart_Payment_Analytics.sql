-- Mart_Payment_Analytics: one row per payment line
-- Grain: payment_key (order_id + payment_sequential)
-- Purpose: supports payment slicing by geography, product category, and price band

WITH payments AS (
    SELECT
        payment_key,
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value
    FROM {{ ref('stg_order_payments') }}
),
orders AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        FORMAT_DATE('%Y-%m', DATE(order_purchase_timestamp)) AS order_month
    FROM {{ ref('stg_orders') }}
),
customers AS (
    SELECT customer_id, customer_city, customer_state
    FROM {{ ref('stg_customers') }}
),
order_items_agg AS (
    SELECT
        order_id,
        SUM(price)                AS items_total,
        SUM(price + freight_value) AS order_total
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
),
order_product AS (
    SELECT
        oi.order_id,
        MAX(p.product_category_name) AS product_category_name
    FROM {{ ref('stg_order_items') }} oi
    LEFT JOIN {{ ref('stg_products') }} p USING (product_id)
    GROUP BY oi.order_id
)
SELECT
    pay.payment_key,
    pay.order_id,
    pay.payment_sequential,
    pay.payment_type,
    pay.payment_installments,
    pay.payment_value,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_month,
    c.customer_city,
    c.customer_state,
    op.product_category_name,
    CASE op.product_category_name
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
        WHEN 'brinquedos' THEN 'Toys'
        WHEN 'cool_stuff' THEN 'Cool Stuff'
        WHEN 'perfumaria' THEN 'Perfumery'
        WHEN 'bebes' THEN 'Baby'
        WHEN 'eletronicos' THEN 'Electronics'
        ELSE COALESCE(op.product_category_name, 'Other')
    END AS product_category_english,
    COALESCE(ia.items_total, 0) AS items_total,
    COALESCE(ia.order_total, 0) AS order_total,
    CASE
        WHEN pay.payment_value <= 50 THEN '0-50'
        WHEN pay.payment_value <= 100 THEN '50-100'
        WHEN pay.payment_value <= 200 THEN '100-200'
        WHEN pay.payment_value <= 500 THEN '200-500'
        WHEN pay.payment_value <= 1000 THEN '500-1000'
        ELSE '1000+'
    END AS price_band,
    CASE
        WHEN pay.payment_value <= 50 THEN 1
        WHEN pay.payment_value <= 100 THEN 2
        WHEN pay.payment_value <= 200 THEN 3
        WHEN pay.payment_value <= 500 THEN 4
        WHEN pay.payment_value <= 1000 THEN 5
        ELSE 6
    END AS price_band_order
FROM payments pay
JOIN orders o USING (order_id)
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN order_items_agg ia USING (order_id)
LEFT JOIN order_product op USING (order_id)
WHERE o.order_status NOT IN ('canceled', 'unavailable')
