{{
    config(
        materialized='incremental',
        unique_key=['ticker', 'trade_date'],
        incremental_strategy='delete+insert'
    )
}}

WITH source_data AS (
    SELECT *
    FROM {{ source('bronze', 'le_boursier_quotes') }}
    {% if is_incremental() %}
        -- Only process incoming partitions
        WHERE trade_date >= (SELECT COALESCE(MAX(trade_date), '1970-01-01') FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        TRIM(LOWER(ticker)) AS ticker,
        TRIM(company_name) AS company_name,
        trade_date,
        -- Clean French numeric strings by stripping all non-numeric characters
        NULLIF(
            REGEXP_REPLACE(REPLACE(close_price, ',', '.'), '[^0-9.]', '', 'g'),
            ''
        )::NUMERIC(12, 2) AS close_price,

        NULLIF(
            REGEXP_REPLACE(REPLACE(variation_pct, ',', '.'), '[^0-9.-]', '', 'g'),
            ''
        )::NUMERIC(6, 4) AS variation_pct,

        NULLIF(
            REGEXP_REPLACE(REPLACE(high_price, ',', '.'), '[^0-9.]', '', 'g'),
            ''
        )::NUMERIC(12, 2) AS high_price,

        NULLIF(
            REGEXP_REPLACE(REPLACE(low_price, ',', '.'), '[^0-9.]', '', 'g'),
            ''
        )::NUMERIC(12, 2) AS low_price,

        NULLIF(
            REGEXP_REPLACE(volume, '[^0-9]', '', 'g'),
            ''
        )::BIGINT AS volume,

        _source_file,
        _ingested_at,
        _run_id,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(LOWER(ticker)), trade_date
            ORDER BY _ingested_at DESC
        ) AS row_num
    FROM source_data
    WHERE ticker IS NOT NULL
      AND TRIM(ticker) != ''
)

SELECT
    ticker,
    company_name,
    trade_date,
    close_price,
    variation_pct,
    high_price,
    low_price,
    volume,
    _source_file,
    _ingested_at,
    _run_id
FROM cleaned
WHERE row_num = 1
  AND close_price IS NOT NULL
  AND close_price > 0