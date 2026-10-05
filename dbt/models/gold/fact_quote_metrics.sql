{{
    config(
        materialized='view'
    )
}}

SELECT
    security_key,
    date_key,
    trade_date,
    close_price,
    volume,
    daily_return_pct,

    -- 20-day Simple Moving Average (SMA)
    ROUND(
        AVG(close_price) OVER (
            PARTITION BY security_key
            ORDER BY trade_date ASC
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS ma_20,

    -- 50-day Simple Moving Average (SMA)
    ROUND(
        AVG(close_price) OVER (
            PARTITION BY security_key
            ORDER BY trade_date ASC
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS ma_50,

    -- 20-day Rolling Average Volume
    ROUND(
        AVG(volume) OVER (
            PARTITION BY security_key
            ORDER BY trade_date ASC
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ),
        0
    )::BIGINT AS avg_volume_20

FROM {{ ref('fact_daily_quote') }}