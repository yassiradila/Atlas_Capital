"""
Market Daily Pipeline DAG.
Orchestrates end-to-end extraction, loading, validation, and dbt transformations
for the Casablanca Stock Exchange on every trading day.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from atlas.alerts.telegram import airflow_failure_callback
from atlas.loading.bronze_loader import load_raw_partition_to_bronze
from atlas.scraping.scraper import load_companies, save_raw_files, scrape_quotes
from atlas.utils.calendar import verify_market_open
from atlas.utils.db import get_warehouse_connection

default_args = {
    "owner": "atlas_capital",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": airflow_failure_callback,
}


def _task_check_market_open(ds: str, **context):
    """Verifies if logical date (ds) is an active trading session."""
    verify_market_open(ds)


def _task_extract_to_raw(ds: str, **context):
    """Scrapes quotes for logical date (ds) and writes partitioned raw files."""
    companies = load_companies()
    quotes = scrape_quotes(start_date=ds, end_date=ds, companies=companies)
    created_files = save_raw_files(quotes)
    return created_files


def _task_load_bronze(ds: str, run_id: str, **context):
    """Loads the raw partition for logical date (ds) idempotently into Bronze."""
    partition_csv = Path(f"/opt/airflow/data/raw/le_boursier/trade_date={ds}/quotes.csv")
    if not partition_csv.exists():
        raise FileNotFoundError(f"Expected partition not found: {partition_csv}")

    rows_loaded = load_raw_partition_to_bronze(str(partition_csv), run_id=run_id)
    return rows_loaded


def _task_validate_bronze(ds: str, **context):
    """Pipeline-level validation: asserts that rows loaded for trading date > 0."""
    with get_warehouse_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM bronze.le_boursier_quotes WHERE trade_date = %s;",
                (ds,),
            )
            count = cur.fetchone()[0]

    if count == 0:
        raise ValueError(
            f"Bronze validation failed: 0 rows found in bronze.le_boursier_quotes for trade_date={ds}!"
        )
    return count


with DAG(
    dag_id="market_daily_pipeline",
    default_args=default_args,
    description="Daily automated ELT pipeline for Casablanca Stock Exchange data",
    schedule="0 18 * * 1-5",  # Monday-Friday at 18:00 (Casablanca time)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stock_market", "daily", "medallion"],
) as dag:

    check_market_open = PythonOperator(
        task_id="check_market_open",
        python_callable=_task_check_market_open,
    )

    extract_to_raw = PythonOperator(
        task_id="extract_to_raw",
        python_callable=_task_extract_to_raw,
    )

    load_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=_task_load_bronze,
    )

    validate_bronze = PythonOperator(
        task_id="validate_bronze",
        python_callable=_task_validate_bronze,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test",
    )

    # Linear medallion dependency flow
    check_market_open >> extract_to_raw >> load_bronze >> validate_bronze >> dbt_run >> dbt_test
