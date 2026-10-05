{{
    config(
        materialized='table'
    )
}}

WITH silver_quotes AS (
    SELECT *
    FROM {{ ref('stg_le_boursier_quotes') }}
),

dim_sec AS (
    SELECT *
    FROM {{ ref('dim_security') }}
),

dim_dt AS (
    SELECT *
    FROM {{ ref('dim_date') }}
)

SELECT
    s.security_key,
    d.date_key,
    q.trade_date,
    q.close_price,
    q.high_price,
    q.low_price,
    q.volume,
    q.variation_pct AS daily_return_pct,
    LAG(q.close_price, 1) OVER (
        PARTITION BY s.security_key
        ORDER BY q.trade_date ASC
    ) AS previous_close_price,
    q._ingested_at
FROM silver_quotes q
JOIN dim_sec s
  ON q.ticker = s.ticker
JOIN dim_dt d
  ON q.trade_date = d.date
