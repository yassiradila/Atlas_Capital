#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medias24 incremental daily scraper — production-ready for Airflow + Docker.
Collects today's data and appends it to Google Sheets (preserves historical data).
"""

import logging
import os
import time
from datetime import datetime, timedelta
from typing import List, Optional

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

CHROME_BIN = os.environ.get("CHROME_BIN", "/usr/bin/chromium")
CHROMEDRIVER_PATH = os.environ.get("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")
DEFAULT_SCRIPTS_DIR = os.environ.get("SCRAPING_SCRIPTS_DIR", "/opt/airflow/scripts")
DEFAULT_OUTPUT_DIR = os.environ.get("SCRAPING_OUTPUT_DIR", "/opt/airflow/data/scraping")
DEFAULT_BASE_URL = "https://medias24.com/leboursier/fiche-action"

TABLE_ROW_XPATHS = (
    "//table//tbody/tr[td]",
    "//table//tr[td]",
    "//tr[td]",
)
DATE_INPUT_LOCATORS = (
    (By.CSS_SELECTOR, "input[type='date']"),
    (By.XPATH, "//input[@type='date']"),
)
CHERCHER_LOCATORS = (
    (By.XPATH, "//input[@value='Chercher']"),
    (By.XPATH, "//button[contains(normalize-space(), 'Chercher')]"),
    (By.CSS_SELECTOR, "input[value='Chercher']"),
)
TABLE_LOCATORS = (
    (By.CSS_SELECTOR, "table tbody"),
    (By.CSS_SELECTOR, "table"),
    (By.XPATH, "//table"),
)


def _env_flag(name: str, default: bool = True) -> bool:
    return os.environ.get(name, str(default)).lower() in ("1", "true", "yes")


def upload_to_google_sheet(df: pd.DataFrame) -> None:
    """
    Append only genuinely new rows to Google Sheets using service account credentials.
    Upstream deduplication strategy: compares incoming data against existing Sheet
    content BEFORE insertion, so no clear()/update() cycle is ever needed.
    This keeps API usage minimal regardless of historical sheet size.

    Args:
        df: pandas DataFrame to upload (scraped data for the current window)

    Raises:
        gspread.exceptions.SpreadsheetNotFound: If the specified Google Sheet is not found
        FileNotFoundError: If the service account credentials file is missing
        Exception: For any other errors during upload
    """
    google_sheet_name = "Moroccan_data_airflow"
    credentials_path = "/opt/airflow/config/google_keys.json"

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    try:
        logger.info("Initialisation de la connexion à Google Sheets...")

        creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
        client = gspread.authorize(creds)

        try:
            spreadsheet = client.open(google_sheet_name)
            logger.info(f"Google Sheet '{google_sheet_name}' trouvé avec succès.")
        except gspread.exceptions.SpreadsheetNotFound:
            error_msg = (
                f"Google Sheet '{google_sheet_name}' introuvable. "
                f"Vérifiez que le classeur existe et que le service account "
                f"a les droits d'accès nécessaires."
            )
            logger.error(error_msg)
            raise gspread.exceptions.SpreadsheetNotFound(error_msg)

        worksheet = spreadsheet.get_worksheet(0)
        logger.info(f"Feuille de travail récupérée: {worksheet.title}")

        # --- Étape 1 : Chargement des clés existantes pour déduplication en amont ---
        existing_records = worksheet.get_all_records()
        if existing_records:
            df_existing = pd.DataFrame(existing_records)
            # Set de couples (Nom_Entreprise, Date) pour une recherche O(1)
            existing_keys = set(
                zip(df_existing["Nom_Entreprise"], df_existing["Date"].astype(str))
            )
            logger.info(f"{len(existing_keys)} couples (Nom_Entreprise, Date) déjà présents dans le Sheet.")
        else:
            existing_keys = set()
            logger.info("Feuille vide détectée — toutes les lignes seront insérées.")

        # --- Étape 2 : Filtrage — on ne garde que les lignes véritablement nouvelles ---
        df_clean = df.fillna("")
        df_new_rows = df_clean[
            ~df_clean.apply(
                lambda row: (row["Nom_Entreprise"], str(row["Date"])), axis=1
            ).isin(existing_keys)
        ]

        # --- Étape 3 : Insertion conditionnelle ---
        if df_new_rows.empty:
            logger.info(
                "Aucune nouvelle donnée à ajouter, le Google Sheet est déjà à jour."
            )
        else:
            worksheet.append_rows(
                df_new_rows.values.tolist(),
                value_input_option="USER_ENTERED",
            )
            logger.info(
                f"✓ Upload réussi : {len(df_new_rows)} nouvelles lignes insérées "
                f"sur {len(df_clean)} collectées "
                f"({len(df_clean) - len(df_new_rows)} doublon(s) ignoré(s))."
            )

    except gspread.exceptions.SpreadsheetNotFound:
        raise
    except FileNotFoundError:
        error_msg = f"Fichier de credentials introuvable: {credentials_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    except Exception as e:
        error_msg = f"Erreur lors de l'upload vers Google Sheets: {type(e).__name__}: {e}"
        logger.error(error_msg)
        raise Exception(error_msg) from e


def run_pipeline(
    base_url: str = DEFAULT_BASE_URL,
    txt_filepath: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    output_csv: Optional[str] = None,
    headless: Optional[bool] = None,
) -> None:
    """
    Entry point for Airflow PythonOperator and CLI runs.
    Incremental mode: collects the last 5 days by default to ensure
    data is captured even after weekends or public holidays.
    """
    if start_date is None or end_date is None:
        today = datetime.now()
        five_days_ago = today - timedelta(days=5)
        if start_date is None:
            start_date = five_days_ago.strftime("%Y-%m-%d")
        if end_date is None:
            end_date = today.strftime("%Y-%m-%d")
        logger.info(f"Mode incrémental élargi : collecte du {start_date} au {end_date}")

    if headless is None:
        headless = _env_flag("SCRAPING_HEADLESS", default=True)
    if txt_filepath is None:
        txt_filepath = os.path.join(DEFAULT_SCRIPTS_DIR, "companies.txt")
    if output_csv is None:
        os.makedirs(DEFAULT_OUTPUT_DIR, exist_ok=True)
        output_csv = os.path.join(DEFAULT_OUTPUT_DIR, "all_companies_historical.csv")

    scraper = Medias24FallbackScraper(headless=headless)
    try:
        scraper.start_driver()
        scraper.scrape_companies(
            base_url=base_url,
            txt_filepath=txt_filepath,
            start_date=start_date,
            end_date=end_date,
        )
        scraper.export_to_csv(output_csv)
        if not scraper.master_buffer:
            raise RuntimeError("Scraping finished with no data in buffer.")

        logger.info("Début de l'upload incrémental vers Google Sheets...")
        df = pd.read_csv(output_csv, encoding="utf-8-sig")
        upload_to_google_sheet(df)
        logger.info("Pipeline incrémental terminé avec succès (scraping + append + déduplication Google Sheets).")

    finally:
        scraper.shutdown()


class Medias24FallbackScraper:
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.options = Options()
        if headless:
            self.options.add_argument("--headless=new")
        self.options.add_argument("--disable-blink-features=AutomationControlled")
        self.options.add_experimental_option("excludeSwitches", ["enable-automation"])
        self.options.add_experimental_option("useAutomationExtension", False)
        self.options.add_argument("--window-size=1920,1080")
        self.options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        if os.path.isfile(CHROME_BIN):
            self.options.binary_location = CHROME_BIN

        self.driver = None
        self.wait: Optional[WebDriverWait] = None
        self.master_buffer: List[list] = []

    def start_driver(self):
        if not os.path.isfile(CHROMEDRIVER_PATH):
            raise FileNotFoundError(f"chromedriver not found at {CHROMEDRIVER_PATH}")
        if not os.path.isfile(CHROME_BIN):
            raise FileNotFoundError(f"chromium not found at {CHROME_BIN}")

        service = Service(executable_path=CHROMEDRIVER_PATH)
        logger.info(
            "Starting Chrome (headless=%s, binary=%s, driver=%s)",
            self.headless,
            CHROME_BIN,
            CHROMEDRIVER_PATH,
        )
        self.driver = webdriver.Chrome(service=service, options=self.options)
        self.wait = WebDriverWait(self.driver, 30)
        self.driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
        )
        self.driver.implicitly_wait(0)
        self.driver.set_page_load_timeout(120)

    def _find_elements_with_fallback(self, locators, condition=EC.presence_of_all_elements_located):
        assert self.wait is not None
        last_error: Optional[Exception] = None
        for locator in locators:
            try:
                return self.wait.until(condition(locator))
            except TimeoutException as exc:
                last_error = exc
        raise TimeoutException(f"None of the locators matched: {locators}") from last_error

    def _find_element_with_fallback(self, locators, condition=EC.presence_of_element_located):
        assert self.wait is not None
        last_error: Optional[Exception] = None
        for locator in locators:
            try:
                return self.wait.until(condition(locator))
            except TimeoutException as exc:
                last_error = exc
        raise TimeoutException(f"None of the locators matched: {locators}") from last_error

    def _wait_for_page_ready(self) -> None:
        assert self.wait is not None
        self.wait.until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )

    def _wait_for_historical_page(self) -> None:
        self._wait_for_page_ready()
        self._find_elements_with_fallback(DATE_INPUT_LOCATORS)

    def _wait_for_table_refresh(self) -> None:
        assert self.wait is not None
        self._find_element_with_fallback(TABLE_LOCATORS)
        try:
            self.wait.until(
                lambda d: len(self.extract_table_rows()) > 0,
                message="Waiting for data rows after search",
            )
        except TimeoutException:
            logger.info("No data rows detected after search (may be valid for this ticker).")

    def load_companies_from_txt(self, filepath: str = "companies.txt") -> List[str]:
        if not os.path.exists(filepath):
            logger.error("'%s' not found. Create it with one company name per line.", filepath)
            return []
        with open(filepath, "r", encoding="utf-8") as file:
            return [line.strip() for line in file if line.strip()]

    def configure_filters_and_search(self, start_date_iso: str, end_date_iso: str):
        """Injects calendar dates via JavaScript execution and clicks Search."""
        try:
            date_inputs = self._find_elements_with_fallback(DATE_INPUT_LOCATORS)
            if len(date_inputs) >= 2:
                self.driver.execute_script(
                    "arguments[0].value = arguments[1];", date_inputs[0], start_date_iso
                )
                self.driver.execute_script(
                    "arguments[0].value = arguments[1];", date_inputs[1], end_date_iso
                )

            chercher_btn = self._find_element_with_fallback(
                CHERCHER_LOCATORS,
                condition=EC.element_to_be_clickable,
            )
            self.driver.execute_script("arguments[0].click();", chercher_btn)

            time.sleep(1)

            self._wait_for_table_refresh()
        except TimeoutException as exc:
            logger.warning("Filter application delayed or incomplete: %s", exc)
        except Exception as exc:
            logger.warning("Filter application delayed: %s", exc)

    def extract_table_rows(self):
        """Helper to safely fetch non-empty data rows from the historical table."""
        for xpath in TABLE_ROW_XPATHS:
            rows = self.driver.find_elements(By.XPATH, xpath)
            data_rows = [
                row
                for row in rows
                if len(row.find_elements(By.TAG_NAME, "td")) >= 2
            ]
            if data_rows:
                return data_rows
        return []

    def scrape_companies(
        self,
        base_url: str,
        txt_filepath: str = "companies.txt",
        start_date: str = "2022-01-01",
        end_date: str = "2026-05-17",
    ):
        company_names = self.load_companies_from_txt(txt_filepath)
        if not company_names:
            return

        for index, raw_name in enumerate(company_names, start=1):
            clean_slug = " ".join(raw_name.split()).lower().replace(" ", "-")
            display_name = raw_name.strip().title()

            logger.info(
                "Progress: %d/%d | Ticker: %s",
                index,
                len(company_names),
                display_name,
            )

            target_url = f"{base_url}?action={clean_slug}&valeur=historiques"
            logger.info("Trying URL: %s", target_url)

            try:
                self.driver.get(target_url)
                self._wait_for_historical_page()
                self.configure_filters_and_search(start_date, end_date)

                rows = self.extract_table_rows()
                row_count = len(rows)

                if row_count == 0 and not clean_slug.endswith("-p"):
                    fallback_slug = f"{clean_slug}-p"
                    fallback_url = f"{base_url}?action={fallback_slug}&valeur=historiques"

                    logger.info(
                        "0 rows found. Retrying with fallback URL: %s", fallback_url
                    )
                    self.driver.get(fallback_url)
                    self._wait_for_historical_page()
                    self.configure_filters_and_search(start_date, end_date)

                    rows = self.extract_table_rows()
                    row_count = len(rows)

                logger.info("Successfully extracted rows: %d", row_count)

                for row in rows:
                    try:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if not cells or len(cells) < 2:
                            continue
                        row_data = [cell.text.strip().replace("\n", " ") for cell in cells]
                        self.master_buffer.append([display_name] + row_data)
                    except StaleElementReferenceException:
                        logger.debug("StaleElementReferenceException détectée, nouvelle tentative...")
                        time.sleep(0.5)
                        try:
                            cells = row.find_elements(By.TAG_NAME, "td")
                            if cells and len(cells) >= 2:
                                row_data = [cell.text.strip().replace("\n", " ") for cell in cells]
                                self.master_buffer.append([display_name] + row_data)
                        except (StaleElementReferenceException, Exception) as retry_error:
                            logger.warning(
                                "Impossible de récupérer la ligne après StaleElement: %s",
                                retry_error
                            )
                            continue

            except Exception as loop_error:
                logger.warning(
                    "Skipping block for '%s' due to error: %s", display_name, loop_error
                )
                continue

    def export_to_csv(self, filename: str = "all_77_companies_historical.csv"):
        if not self.master_buffer:
            logger.error("Consolidated matrix buffer is empty. Process aborted.")
            return

        df = pd.DataFrame(self.master_buffer)
        headers = [
            "Nom_Entreprise",
            "Date",
            "Cours",
            "Variation_Pourcentage",
            "Plus_Haut",
            "Plus_Bas",
            "Volume",
        ]

        if df.shape[1] <= len(headers):
            df.columns = headers[: df.shape[1]]
        else:
            df.columns = ["Nom_Entreprise"] + [f"Col_{i}" for i in range(df.shape[1] - 1)]

        df.drop_duplicates(subset=["Nom_Entreprise", "Date"], keep="first", inplace=True)
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        logger.info("Master pipeline finished. CSV written: '%s'", filename)

    def shutdown(self):
        if self.driver:
            self.driver.quit()
            logger.info("Session cleanup complete.")


if __name__ == "__main__":
    run_pipeline()
