WITH source AS (SELECT * FROM {{ source('olist_raw', 'olist_geolocation') }}),
deduped AS (
    SELECT
        CAST(geolocation_zip_code_prefix AS STRING) AS zip_code_prefix,
        AVG(CAST(geolocation_lat AS FLOAT64))       AS latitude,
        AVG(CAST(geolocation_lng AS FLOAT64))       AS longitude,
        MAX(CAST(geolocation_city AS STRING))       AS city,
        MAX(CAST(geolocation_state AS STRING))      AS state
    FROM source
    GROUP BY 1
)
SELECT * FROM deduped
