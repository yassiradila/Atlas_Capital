{{
    config(
        materialized='table'
    )
}}

WITH distinct_securities AS (
    SELECT DISTINCT
        ticker,
        company_name
    FROM {{ ref('stg_le_boursier_quotes') }}
)

SELECT
    DENSE_RANK() OVER (ORDER BY ticker)::INT AS security_key,
    ticker,
    company_name
FROM distinct_securities
