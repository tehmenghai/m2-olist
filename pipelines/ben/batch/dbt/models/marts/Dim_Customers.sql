SELECT
    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    g.latitude,
    g.longitude,
    COUNT(DISTINCT f.order_id)                               AS total_orders,
    ROUND(SUM(f.payment_value), 2)                           AS total_spend,
    ROUND(AVG(f.payment_value), 2)                           AS avg_order_value,
    MAX(DATE(f.order_purchase_timestamp))                    AS last_order_date,
    DATE_DIFF(CURRENT_DATE(), MAX(DATE(f.order_purchase_timestamp)), DAY) AS days_since_last_order,
    ROUND(AVG(f.review_score), 2)                            AS avg_review_score
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('Fact_Orders') }} f USING (customer_id)
LEFT JOIN {{ ref('stg_geolocation') }} g ON c.customer_zip_hash = TO_HEX(MD5(g.zip_code_prefix))
GROUP BY 1, 2, 3, 4, 5, 6
