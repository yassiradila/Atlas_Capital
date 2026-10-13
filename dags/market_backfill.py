"""
Market Backfill DAG.
Manually triggered pipeline to backfill historical data for a specified date range.
Reuses the exact same scraping, loading, and transformation logic.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from atlas.loading.bronze_loader import load_raw_partition_to_bronze
from atlas.scraping.scraper import load_companies, save_raw_files, scrape_quotes

default_args = {
    "owner": "atlas_capital",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _task_extract_historical(params: dict, **context):
    """Extracts quotes for all companies across the parameterized date range."""
    start_date = params["start_date"]
    end_date = params["end_date"]
    companies = load_companies()

    print(f"[+] Starting historical extraction from {start_date} to {end_date} for {len(companies)} companies.")
    quotes = scrape_quotes(start_date=start_date, end_date=end_date, companies=companies)
    created_files = save_raw_files(quotes)
    print(f"[+] Extracted {len(quotes)} quotes into {len(created_files)} raw partitions.")
    return created_files


def _task_load_historical_bronze(run_id: str, **context):
    """Iterates through all raw partitions on disk and idempotently loads them into Bronze."""
    raw_dir = Path("/opt/airflow/data/raw/le_boursier")
    partition_files = list(raw_dir.glob("trade_date=*/quotes.csv"))
    print(f"[+] Loading {len(partition_files)} raw partitions into bronze...")

    total_rows = 0
    for p_file in partition_files:
        total_rows += load_raw_partition_to_bronze(str(p_file), run_id=run_id)

    print(f"[+] Successfully loaded {total_rows} total rows into bronze.")
    return total_rows


with DAG(
    dag_id="market_backfill",
    default_args=default_args,
    description="Manual backfill pipeline for historical Casablanca Stock Exchange quotes",
    schedule=None,  # Triggered manually only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "start_date": Param(
            default="2024-01-01",
            type="string",
            description="Start date (YYYY-MM-DD)",
        ),
        "end_date": Param(
            default="2024-05-15",
            type="string",
            description="End date (YYYY-MM-DD)",
        ),
    },
    tags=["stock_market", "backfill", "medallion"],
) as dag:

    extract_historical_raw = PythonOperator(
        task_id="extract_historical_raw",
        python_callable=_task_extract_historical,
    )

    load_historical_bronze = PythonOperator(
        task_id="load_historical_bronze",
        python_callable=_task_load_historical_bronze,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test",
    )

    extract_historical_raw >> load_historical_bronze >> dbt_run >> dbt_test
