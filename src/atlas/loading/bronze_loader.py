"""
Bronze Layer Loader.
Loads partitioned raw CSV files into bronze.le_boursier_quotes using COPY.
Guarantees idempotency via atomic delete+insert per trade_date.
"""

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path

from atlas.utils.db import get_warehouse_connection

logger = logging.getLogger(__name__)


def load_raw_partition_to_bronze(
    partition_csv_path: str,
    run_id: str | None = None,
) -> int:
    """
    Idempotently loads a single raw quotes.csv partition into bronze.le_boursier_quotes.
    In one single transaction:
      1. Deletes any existing rows for this partition's trade_date.
      2. Inserts new rows using PostgreSQL COPY protocol.
    Returns the number of rows inserted.
    """
    path = Path(partition_csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Partition file does not exist: {partition_csv_path}")

    # Read rows from raw CSV
    rows_to_insert = []
    trade_dates = set()

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            t_date = r.get("trade_date")
            if t_date:
                trade_dates.add(t_date)
            rows_to_insert.append(r)

    if not rows_to_insert:
        logger.warning("No rows found in %s, nothing to load.", partition_csv_path)
        return 0

    if len(trade_dates) != 1:
        raise ValueError(
            f"Expected exactly 1 trade_date per partition, found: {trade_dates} in {partition_csv_path}"
        )

    partition_trade_date = trade_dates.pop()
    now_utc = datetime.now(timezone.utc).isoformat()
    actual_run_id = run_id or f"manual_{int(datetime.now().timestamp())}"

    # Atomic transaction: delete then COPY
    with get_warehouse_connection() as conn, conn.cursor() as cur:
        # 1. Idempotency: purge existing records for this partition
        cur.execute(
            "DELETE FROM bronze.le_boursier_quotes WHERE trade_date = %s;",
            (partition_trade_date,),
        )
        deleted_count = cur.rowcount
        if deleted_count > 0:
            logger.info(
                "Purged %d existing records in bronze for trade_date=%s",
                deleted_count,
                partition_trade_date,
            )

        # 2. Fast ingestion via PostgreSQL COPY
        copy_sql = """
                COPY bronze.le_boursier_quotes (
                    ticker, company_name, raw_date, trade_date,
                    close_price, variation_pct, high_price, low_price, volume,
                    _source_file, _ingested_at, _run_id
                ) FROM STDIN
            """
        with cur.copy(copy_sql) as copy:
            for row in rows_to_insert:
                record = (
                    row.get("ticker", ""),
                    row.get("company_name", ""),
                    row.get("raw_date", ""),
                    row.get("trade_date", ""),
                    row.get("close_price", ""),
                    row.get("variation_pct", ""),
                    row.get("high_price", ""),
                    row.get("low_price", ""),
                    row.get("volume", ""),
                    str(path.as_posix()),
                    now_utc,
                    actual_run_id,
                )
                copy.write_row(record)

    logger.info(
        "Successfully loaded %d rows into bronze.le_boursier_quotes for trade_date=%s",
        len(rows_to_insert),
        partition_trade_date,
    )
    return len(rows_to_insert)
