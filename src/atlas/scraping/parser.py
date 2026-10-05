"""
Pure parsing utilities for Le Boursier stock market data.
No Selenium or network dependencies — completely unit-testable.
"""

import re
from datetime import datetime


def clean_slug(raw_name: str) -> str:
    """
    Transforms a company name into the URL slug format expected by Le Boursier.
    Example: 'Attijariwafa Bank' -> 'attijariwafa-bank'
    """
    if not raw_name:
        return ""
    # Replace multiple whitespace characters with single dash
    words = raw_name.strip().split()
    return "-".join(words).lower()


def parse_french_number(value_str: str | None) -> float | None:
    """
    Parses French-formatted numbers into standard floats.
    Handles:
    - Decimal comma: '123,45' -> 123.45
    - Thousands separators (space, non-breaking space '\xa0', narrow no-break space '\u202f'):
      '1 250,50' -> 1250.50
    - Percentage signs: '+1,25%' -> 1.25
    - Empty, dash, or invalid values: '-' or '' -> None
    """
    if not value_str:
        return None

    cleaned = value_str.strip()
    if cleaned in ("-", "", "N/A", "null", "None"):
        return None

    # Remove percentage signs and '+'
    cleaned = cleaned.replace("%", "").replace("+", "").strip()

    # Remove all spaces and Unicode non-breaking spaces
    cleaned = re.sub(r"[\s\xa0\u202f]", "", cleaned)

    # Convert decimal comma to standard dot
    cleaned = cleaned.replace(",", ".")

    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_french_date(date_str: str | None) -> str | None:
    """
    Parses date strings into ISO format (YYYY-MM-DD).
    Supports:
    - 'DD/MM/YYYY' (standard Le Boursier format, e.g., '15/05/2024' -> '2024-05-15')
    - 'YYYY-MM-DD' (already ISO, passes through)
    """
    if not date_str:
        return None

    cleaned = date_str.strip()
    if not cleaned:
        return None

    # Already ISO format
    if re.match(r"^\d{4}-\d{2}-\d{2}$", cleaned):
        return cleaned

    # DD/MM/YYYY format
    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            parsed = datetime.strptime(cleaned, fmt)
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None
