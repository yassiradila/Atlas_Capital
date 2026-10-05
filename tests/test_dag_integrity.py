"""
DAG Integrity Unit Test.
Verifies that all DAGs in the dags folder import without syntax or configuration errors.
Run with: pytest tests/test_dag_integrity.py
"""

from airflow.models import DagBag


def test_dagbag_import_errors():
    """Ensures no DAG in dags/ has syntax or import errors."""
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    assert len(dagbag.import_errors) == 0, f"DAG import errors found: {dagbag.import_errors}"


def test_market_daily_pipeline_structure():
    """Asserts that market_daily_pipeline has the expected tasks and settings."""
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag("market_daily_pipeline")
    assert dag is not None, "DAG market_daily_pipeline not found in DagBag!"

    expected_tasks = [
        "check_market_open",
        "extract_to_raw",
        "load_bronze",
        "validate_bronze",
        "dbt_run",
        "dbt_test",
    ]
    actual_tasks = [t.task_id for t in dag.tasks]
    assert set(expected_tasks) == set(actual_tasks)

    # Verify retries and timezone
    assert dag.default_args.get("retries") == 2
    assert dag.timezone.name == "Africa/Casablanca"


def test_market_backfill_structure():
    """Asserts that market_backfill has the expected tasks, params, and settings."""
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag("market_backfill")
    assert dag is not None, "DAG market_backfill not found in DagBag!"

    expected_tasks = [
        "extract_historical_raw",
        "load_historical_bronze",
        "dbt_run",
        "dbt_test",
    ]
    actual_tasks = [t.task_id for t in dag.tasks]
    assert set(expected_tasks) == set(actual_tasks)

    # Verify manual trigger only and parameters
    assert dag.schedule_interval is None
    assert "start_date" in dag.params
    assert "end_date" in dag.params
