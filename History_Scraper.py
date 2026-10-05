import os
import csv
import time
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

class Medias24FallbackScraper:
    def __init__(self, headless=False):
        self.options = Options()
        if headless:
            self.options.add_argument("--headless=new")
            
        # --- Advanced Anti-Detection Setup ---
        self.options.add_argument("--disable-blink-features=AutomationControlled")
        self.options.exclude_switches = ["enable-automation"]
        self.options.add_experimental_option("useAutomationExtension", False)
        self.options.add_argument("window-size=1920,1080")
        self.options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        
        self.driver = None
        self.master_buffer = []

    def start_driver(self):
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=self.options)
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })

    def load_companies_from_txt(self, filepath="companies.txt"):
        if not os.path.exists(filepath):
            print(f"[-] Error: '{filepath}' not found. Please create it with one company name per line.")
            return []
        with open(filepath, "r", encoding="utf-8") as file:
            return [line.strip() for line in file if line.strip()]

    def configure_filters_and_search(self, start_date_iso, end_date_iso):
        """Injects calendar dates via JavaScript execution and clicks Search."""
        try:
            date_inputs = self.driver.find_elements(By.XPATH, "//input[@type='date']")
            if len(date_inputs) >= 2:
                self.driver.execute_script("arguments[0].value = arguments[1];", date_inputs[0], start_date_iso)
                self.driver.execute_script("arguments[0].value = arguments[1];", date_inputs[1], end_date_iso)
            
            chercher_btn = self.driver.find_element(By.XPATH, "//input[@value='Chercher'] | //button[contains(text(), 'Chercher')]")
            self.driver.execute_script("arguments[0].click();", chercher_btn)
            time.sleep(3.5)  # Wait for AJAX data grid table to refresh
        except Exception as e:
            print(f"    [-] Warning: Filter application delayed: {e}")

    def extract_table_rows(self):
        """Helper to safely fetch non-empty data rows from the historical table."""
        return self.driver.find_elements(By.XPATH, "//table//tbody/tr | //tr[td]")

    def run_pipeline(self, base_url, txt_filepath="companies.txt", start_date="2022-01-01", end_date="2026-05-17"):
        company_names = self.load_companies_from_txt(txt_filepath)
        if not company_names:
            return

        self.start_driver()

        for index, raw_name in enumerate(company_names, start=1):
            # Clean name format to default URL hyphenated layout
            clean_slug = " ".join(raw_name.split()).lower().replace(" ", "-")
            display_name = raw_name.strip().title()
            
            print(f"\n[***] Progress: {index}/{len(company_names)} | Ticker: {display_name}")
            
            # 1. Try standard base URL first
            target_url = f"{base_url}?action={clean_slug}&valeur=historiques"
            print(f"    [>] Trying URL: {target_url}")
            
            try:
                self.driver.get(target_url)
                time.sleep(2.0)
                self.configure_filters_and_search(start_date, end_date)
                
                rows = self.extract_table_rows()
                row_count = len(rows)
                
                # 2. Suffix Fallback Logic: If 0 rows are found, try appending '-p'
                if row_count == 0 and not clean_slug.endswith("-p"):
                    fallback_slug = f"{clean_slug}-p"
                    fallback_url = f"{base_url}?action={fallback_slug}&valeur=historiques"
                    
                    print(f"    [!] 0 rows found. Retrying with fallback URL: {fallback_url}")
                    self.driver.get(fallback_url)
                    time.sleep(2.0)
                    self.configure_filters_and_search(start_date, end_date)
                    
                    rows = self.extract_table_rows()
                    row_count = len(rows)
                
                print(f"    [+] Successfully extracted rows: {row_count}")
                
                # 3. Parse collected data into master sheet array
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if not cells or len(cells) < 2:
                        continue
                    row_data = [cell.text.strip().replace('\n', ' ') for cell in cells]
                    
                    # Store data prepended with the clean company name
                    self.master_buffer.append([display_name] + row_data)
                    
            except Exception as loop_error:
                print(f"    [!] Skipping block for '{display_name}' due to error: {loop_error}")
                continue

    def export_to_csv(self, filename="all_77_companies_historical.csv"):
        if not self.master_buffer:
            print("[-] Consolidated matrix buffer is empty. Process aborted.")
            return
            
        df = pd.DataFrame(self.master_buffer)
        headers = ["Nom_Entreprise", "Date", "Cours", "Variation_Pourcentage", "Plus_Haut", "Plus_Bas", "Volume"]
        
        if df.shape[1] <= len(headers):
            df.columns = headers[:df.shape[1]]
        else:
            df.columns = ["Nom_Entreprise"] + [f"Col_{i}" for i in range(df.shape[1] - 1)]
            
        df.drop_duplicates(subset=["Nom_Entreprise", "Date"], keep='first', inplace=True)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"\n[+++++] Master Pipeline Finished! Consolidated CSV updated: '{filename}'")

    def shutdown(self):
        if self.driver:
            self.driver.quit()
            print("[+] Session cleanup complete.")

if __name__ == "__main__":
    base_portal_url = "https://medias24.com/leboursier/fiche-action"
    
    scraper = Medias24FallbackScraper(headless=False)
    try:
        scraper.run_pipeline(
            base_url=base_portal_url, 
            txt_filepath="companies.txt", 
            start_date="2024-01-01", 
            end_date="2026-05-17"
        )
        scraper.export_to_csv("all_companies_historical.csv")
    finally:
        scraper.shutdown()