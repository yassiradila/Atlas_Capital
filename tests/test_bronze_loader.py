"""
Tests the Bronze loader and verifies the idempotency contract:
loading the same partition multiple times must never create duplicates.
"""

from pathlib import Path
from atlas.loading.bronze_loader import load_raw_partition_to_bronze
from atlas.utils.db import get_warehouse_connection


def test_idempotent_load():
    # Pick the partition created during our extraction test
    raw_dir = Path("/opt/airflow/data/raw/le_boursier")
    partitions = list(raw_dir.glob("trade_date=*/quotes.csv"))
    assert len(partitions) > 0, "No raw partition files found to test!"

    test_file = str(partitions[0])
    partition_date = partitions[0].parent.name.split("=")[1]
    print(f"\n[+] Testing bronze load for file: {test_file} (date: {partition_date})")

    # First load
    count_1 = load_raw_partition_to_bronze(test_file, run_id="test_run_1")
    print(f"[+] Run 1 inserted {count_1} rows.")

    with get_warehouse_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM bronze.le_boursier_quotes WHERE trade_date = %s;",
                (partition_date,),
            )
            db_count_1 = cur.fetchone()[0]

    print(f"[+] Database count after Run 1: {db_count_1}")
    assert db_count_1 == count_1

    # Second load (simulate re-running the Airflow task)
    print("\n[+] Re-running load for the same partition (Idempotency test)...")
    count_2 = load_raw_partition_to_bronze(test_file, run_id="test_run_2")
    print(f"[+] Run 2 reported {count_2} rows.")

    with get_warehouse_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM bronze.le_boursier_quotes WHERE trade_date = %s;",
                (partition_date,),
            )
            db_count_2 = cur.fetchone()[0]

    print(f"[+] Database count after Run 2: {db_count_2}")

    # The fundamental test: count must be identical
    assert db_count_1 == db_count_2, (
        f"Idempotency failed! Count changed from {db_count_1} to {db_count_2}"
    )
    print("\n[✓] IDEMPOTENCY TEST PASSED! No duplicates created.")


if __name__ == "__main__":
    test_idempotent_load()
