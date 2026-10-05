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