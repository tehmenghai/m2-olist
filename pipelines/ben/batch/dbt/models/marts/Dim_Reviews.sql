SELECT
    r.review_id,
    r.order_id,
    r.review_score,
    r.review_comment_title,
    r.review_comment_message,
    r.review_creation_date,
    r.review_answer_timestamp,
    DATE_DIFF(r.review_answer_timestamp, r.review_creation_date, HOUR) AS response_hours,
    o.customer_id,
    o.order_purchase_timestamp
FROM {{ ref('stg_order_reviews') }} r
LEFT JOIN {{ ref('stg_orders') }} o USING (order_id)
