WITH source AS (SELECT * FROM {{ source('olist_raw', 'sellers') }}),
renamed AS (
    SELECT
        seller_id,
        TO_HEX(MD5(CAST(seller_zip_code_prefix AS STRING))) AS seller_zip_hash,
        CAST(seller_city AS STRING)   AS seller_city,
        CAST(seller_state AS STRING)  AS seller_state
    FROM source WHERE seller_id IS NOT NULL
)
SELECT * FROM renamed
