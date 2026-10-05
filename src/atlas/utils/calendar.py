"""
Calendar and Market Session Verification.
Checks if a given logical date is a valid trading session for the Casablanca Stock Exchange.
"""

import logging
from datetime import datetime

from airflow.exceptions import AirflowSkipException

logger = logging.getLogger(__name__)

# Known Moroccan public holidays (Month-Day format)
MOROCCAN_FIXED_HOLIDAYS = {
    "01-01",  # New Year's Day
    "01-11",  # Independence Manifesto Day
    "05-01",  # Labour Day
    "07-30",  # Throne Day
    "08-14",  # Oued Ed-Dahab Day
    "08-20",  # Revolution of the King and the People
    "08-21",  # Youth Day
    "11-06",  # Green March Day
    "11-18",  # Independence Day
}


def is_weekend(date_str: str) -> bool:
    """Returns True if the date (YYYY-MM-DD) falls on Saturday or Sunday."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.weekday() in (5, 6)


def is_holiday(date_str: str) -> bool:
    """Returns True if the date matches known fixed holidays."""
    mm_dd = date_str[5:]
    return mm_dd in MOROCCAN_FIXED_HOLIDAYS


def verify_market_open(logical_date_str: str) -> bool:
    """
    Evaluates whether the Casablanca Stock Exchange was open on logical_date_str.
    Raises AirflowSkipException if the market was closed (weekend or holiday),
    gracefully skipping all downstream tasks without failing the DAG run.
    """
    logger.info("Checking market session status for date: %s", logical_date_str)

    if is_weekend(logical_date_str):
        msg = f"Casablanca Stock Exchange is closed on weekends (date: {logical_date_str}). Skipping run."
        logger.info(msg)
        raise AirflowSkipException(msg)

    if is_holiday(logical_date_str):
        msg = f"Casablanca Stock Exchange is closed for holiday (date: {logical_date_str}). Skipping run."
        logger.info(msg)
        raise AirflowSkipException(msg)

    logger.info("Confirmed: %s is a regular trading session.", logical_date_str)
    return True
