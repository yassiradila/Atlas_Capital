-- We created 2 DBs (airflow & warehouse), with 3 schemas (bronze, silver, gold) inside warehouse db.

-- Create airflow metadata database if it does not exist
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

-- Create warehouse database if it does not exist
SELECT 'CREATE DATABASE warehouse'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'warehouse')\gexec

-- Connect to the warehouse database to set up our medallion schemas
\c warehouse;

-- Medallion architecture schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Grant permissions to default user
GRANT ALL ON SCHEMA bronze TO postgres;
GRANT ALL ON SCHEMA silver TO postgres;
GRANT ALL ON SCHEMA gold TO postgres;







-- Bronze table: raw text plus audit columns
CREATE TABLE IF NOT EXISTS bronze.le_boursier_quotes (
    ticker         TEXT,
    company_name   TEXT,
    raw_date       TEXT,
    trade_date     DATE NOT NULL,
    close_price    TEXT,
    variation_pct  TEXT,
    high_price     TEXT,
    low_price      TEXT,
    volume         TEXT,
    _source_file   TEXT,
    _ingested_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    _run_id        TEXT
);

-- Index on trade_date for fast partition deletes and incremental downstream reads
CREATE INDEX IF NOT EXISTS idx_bronze_quotes_trade_date ON bronze.le_boursier_quotes (trade_date);