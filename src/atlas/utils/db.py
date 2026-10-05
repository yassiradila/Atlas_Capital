"""
PostgreSQL database connection utilities using psycopg 3.
Reads connection parameters from environment variables.
"""

import os
from collections.abc import Generator
from contextlib import contextmanager

import psycopg


def get_connection_uri() -> str:
    """Builds the PostgreSQL connection URI from environment variables."""
    host = os.environ.get("WAREHOUSE_HOST", "postgres")
    port = os.environ.get("WAREHOUSE_PORT", "5432")
    dbname = os.environ.get("WAREHOUSE_DB", "warehouse")
    user = os.environ.get("WAREHOUSE_USER", "postgres")
    password = os.environ.get("WAREHOUSE_PASSWORD", "postgres")

    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


@contextmanager
def get_warehouse_connection() -> Generator[psycopg.Connection, None, None]:
    """
    Context manager providing a psycopg connection to the warehouse database.
    Automatically handles commit and rollback on error.
    """
    conn_uri = get_connection_uri()
    with psycopg.connect(conn_uri) as conn:
        yield conn
