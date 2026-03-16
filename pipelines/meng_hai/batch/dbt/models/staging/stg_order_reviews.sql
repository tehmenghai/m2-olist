WITH source AS (SELECT * FROM {{ source('olist_raw', 'order_reviews') }}),
renamed AS (
    SELECT
        review_id,
        order_id,
        CAST(review_score AS INT64)              AS review_score,
        CAST(review_comment_title AS STRING)     AS review_comment_title,
        CAST(review_comment_message AS STRING)   AS review_comment_message,
        CAST(review_creation_date AS TIMESTAMP)  AS review_creation_date,
        CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp
    FROM source WHERE review_id IS NOT NULL
)
SELECT * FROM renamed
