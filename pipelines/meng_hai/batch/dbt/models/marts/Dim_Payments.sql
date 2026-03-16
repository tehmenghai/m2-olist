SELECT
    p.payment_key,
    p.order_id,
    p.payment_sequential,
    p.payment_type,
    p.payment_installments,
    p.payment_value,
    o.order_purchase_timestamp,
    o.customer_id
FROM {{ ref('stg_order_payments') }} p
LEFT JOIN {{ ref('stg_orders') }} o USING (order_id)
