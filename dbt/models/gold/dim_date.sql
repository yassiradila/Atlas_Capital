{{
    config(
        materialized='table'
    )
}}

WITH date_spine AS (
    SELECT DISTINCT
        trade_date AS date_actual
    FROM {{ ref('stg_le_boursier_quotes') }}
)

SELECT
    -- Universal Kimball date key: YYYYMMDD as an integer
    TO_CHAR(date_actual, 'YYYYMMDD')::INT AS date_key,
    date_actual AS date,
    EXTRACT(YEAR FROM date_actual)::INT AS year,
    EXTRACT(MONTH FROM date_actual)::INT AS month,
    TO_CHAR(date_actual, 'Month') AS month_name,
    EXTRACT(QUARTER FROM date_actual)::INT AS quarter,
    EXTRACT(WEEK FROM date_actual)::INT AS iso_week,
    EXTRACT(ISODOW FROM date_actual)::INT AS day_of_week,
    TO_CHAR(date_actual, 'Day') AS day_name,
    CASE WHEN EXTRACT(ISODOW FROM date_actual) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    TRUE AS is_trading_day
FROM date_spine
ORDER BY date_actual ASC
