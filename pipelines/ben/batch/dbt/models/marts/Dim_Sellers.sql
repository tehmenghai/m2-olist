SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    g.latitude,
    g.longitude,
    COUNT(DISTINCT oi.order_id)            AS total_orders,
    ROUND(SUM(oi.price), 2)               AS total_revenue,
    ROUND(AVG(oi.price), 2)               AS avg_item_price,
    ROUND(AVG(r.review_score), 2)         AS avg_review_score,
    COUNT(DISTINCT oi.product_id)         AS unique_products
FROM {{ ref('stg_sellers') }} s
LEFT JOIN {{ ref('stg_order_items') }} oi USING (seller_id)
LEFT JOIN {{ ref('stg_order_reviews') }} r USING (order_id)
LEFT JOIN {{ ref('stg_geolocation') }} g ON s.seller_zip_hash = TO_HEX(MD5(g.zip_code_prefix))
GROUP BY 1, 2, 3, 4, 5
