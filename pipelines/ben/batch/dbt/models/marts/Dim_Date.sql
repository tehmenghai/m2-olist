WITH date_range AS (
    SELECT CAST(date_col AS DATE) AS date_key
    FROM UNNEST(GENERATE_DATE_ARRAY('2016-01-01', CURRENT_DATE())) AS date_col
)
SELECT
    date_key,
    EXTRACT(YEAR FROM date_key) AS year,
    EXTRACT(QUARTER FROM date_key) AS quarter,
    EXTRACT(MONTH FROM date_key) AS month,
    EXTRACT(WEEK FROM date_key) AS week,
    EXTRACT(DAYOFWEEK FROM date_key) AS day,
    EXTRACT(DAYOFWEEK FROM date_key) IN (1, 7) AS is_weekend
FROM date_range
