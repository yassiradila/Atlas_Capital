"""
Core extraction module for Le Boursier stock market data.
Scrapes quotes for all registered companies within a given date range
and writes partitioned raw CSV files.
"""

import csv
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from atlas.scraping.driver import get_driver
from atlas.scraping.parser import clean_slug, parse_french_date

logger = logging.getLogger(__name__)

BASE_URL = "https://medias24.com/leboursier/fiche-action"
DEFAULT_COMPANIES_FILE = Path(__file__).parent / "companies.txt"

DATE_INPUT_LOCATORS = (
    (By.CSS_SELECTOR, "input[type='date']"),
    (By.XPATH, "//input[@type='date']"),
)
CHERCHER_LOCATORS = (
    (By.XPATH, "//input[@value='Chercher']"),
    (By.XPATH, "//button[contains(normalize-space(), 'Chercher')]"),
    (By.CSS_SELECTOR, "input[value='Chercher']"),
)
TABLE_ROW_XPATHS = (
    "//table//tbody/tr[td]",
    "//table//tr[td]",
    "//tr[td]",
)


def load_companies(filepath: Optional[Path] = None) -> List[str]:
    """Reads non-empty company names from the companies configuration file."""
    path = filepath or DEFAULT_COMPANIES_FILE
    if not os.path.exists(path):
        logger.error("Companies file not found at: %s", path)
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def _wait_for_page_ready(driver, timeout: int = 15) -> None:
    """Waits until document.readyState is complete."""
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )


def _apply_date_filter_and_search(driver, start_date: str, end_date: str) -> bool:
    """Injects start and end dates via JavaScript and clicks Chercher. Returns True on success."""
    try:
        _wait_for_page_ready(driver)
        wait = WebDriverWait(driver, 10)
        date_inputs = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//input[@type='date']")))
        if len(date_inputs) >= 2:
            driver.execute_script("arguments[0].value = arguments[1];", date_inputs[0], start_date)
            driver.execute_script("arguments[0].value = arguments[1];", date_inputs[1], end_date)

        chercher_btn = None
        for by, selector in CHERCHER_LOCATORS:
            elements = driver.find_elements(by, selector)
            if elements:
                chercher_btn = elements[0]
                break

        if chercher_btn:
            driver.execute_script("arguments[0].click();", chercher_btn)
            time.sleep(1.5)  # Wait for AJAX data grid to refresh
            return True
        return False
    except TimeoutException:
        return False


def _extract_table_rows(driver) -> List[List[str]]:
    """Extracts raw cell text from the displayed historical table."""
    for xpath in TABLE_ROW_XPATHS:
        rows = driver.find_elements(By.XPATH, xpath)
        data = []
        for row in rows:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) >= 2:
                data.append([c.text.strip().replace("\n", " ") for c in cells])
        if data:
            return data
    return []


def scrape_quotes(
    start_date: str,
    end_date: str,
    companies: Optional[List[str]] = None,
    delay_between_requests: float = 1.0,
) -> List[Dict[str, str]]:
    """
    Extracts quotes for all companies between start_date and end_date.
    Returns a list of dictionaries with raw string values.
    """
    if companies is None:
        companies = load_companies()

    driver = get_driver()
    results: List[Dict[str, str]] = []

    try:
        for idx, raw_name in enumerate(companies, start=1):
            slug = clean_slug(raw_name)
            display_name = raw_name.strip().title()
            logger.info("[%d/%d] Scraping quotes for: %s (slug: %s)", idx, len(companies), display_name, slug)

            try:
                target_url = f"{BASE_URL}?action={slug}&valeur=historiques"
                driver.get(target_url)
                time.sleep(1.0)

                success = _apply_date_filter_and_search(driver, start_date, end_date)
                rows = _extract_table_rows(driver) if success else []

                # Fallback slug with '-p' if 0 rows or inputs not found
                if not rows and not slug.endswith("-p"):
                    fallback_slug = f"{slug}-p"
                    logger.info("Retrying with fallback slug: %s", fallback_slug)
                    driver.get(f"{BASE_URL}?action={fallback_slug}&valeur=historiques")
                    time.sleep(1.0)
                    if _apply_date_filter_and_search(driver, start_date, end_date):
                        rows = _extract_table_rows(driver)

                logger.info("Found %d rows for %s", len(rows), display_name)

                for row in rows:
                    if len(row) < 6:
                        continue
                    # Expected row: Date, Cours, Variation%, Plus Haut, Plus Bas, Volume
                    raw_date = row[0]
                    iso_date = parse_french_date(raw_date) or raw_date

                    results.append({
                        "company_name": display_name,
                        "ticker": slug.replace("-p", ""),
                        "raw_date": raw_date,
                        "trade_date": iso_date,
                        "close_price": row[1] if len(row) > 1 else "",
                        "variation_pct": row[2] if len(row) > 2 else "",
                        "high_price": row[3] if len(row) > 3 else "",
                        "low_price": row[4] if len(row) > 4 else "",
                        "volume": row[5] if len(row) > 5 else "",
                    })

            except Exception as e:
                logger.warning("Error scraping company '%s': %s (skipping)", display_name, e)
                continue

            time.sleep(delay_between_requests)

    finally:
        driver.quit()

    return results


def save_raw_files(
    quotes: List[Dict[str, str]],
    base_output_dir: str = "/opt/airflow/data/raw/le_boursier",
) -> List[str]:
    """
    Saves extracted quotes partitioned by trade_date:
    data/raw/le_boursier/trade_date=YYYY-MM-DD/quotes.csv
    Returns the list of created file paths.
    """
    if not quotes:
        logger.warning("No quotes to save.")
        return []

    # Group records by trade_date partition
    partitions: Dict[str, List[Dict[str, str]]] = {}
    for q in quotes:
        d = q.get("trade_date", "unknown")
        partitions.setdefault(d, []).append(q)

    created_files: List[str] = []
    fieldnames = [
        "company_name", "ticker", "raw_date", "trade_date",
        "close_price", "variation_pct", "high_price", "low_price", "volume",
        "extracted_at",
    ]
    now_iso = datetime.now(timezone.utc).isoformat()

    for trade_date, records in partitions.items():
        partition_dir = Path(base_output_dir) / f"trade_date={trade_date}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        filepath = partition_dir / "quotes.csv"
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in records:
                row_copy = dict(r)
                row_copy["extracted_at"] = now_iso
                writer.writerow(row_copy)

        logger.info("Wrote %d records to %s", len(records), filepath)
        created_files.append(str(filepath))

    return created_files
