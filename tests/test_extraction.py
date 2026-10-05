"""
End-to-end integration test for the scraper module.
Extracts a small window of quotes for 2 liquid companies.
"""

import logging

from atlas.scraping.scraper import save_raw_files, scrape_quotes

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def test_scrape_and_save():
    test_companies = ["attijariwafa bank", "maroc telecom"]
    print(f"\n[+] Starting test scraping for: {test_companies}")

    quotes = scrape_quotes(
        start_date="2024-05-01",
        end_date="2024-05-15",
        companies=test_companies,
    )

    print(f"\n[+] Total quotes extracted: {len(quotes)}")
    assert len(quotes) > 0, "Expected at least 1 quote to be extracted!"

    created_files = save_raw_files(quotes)
    print(f"[+] Total raw partition files created: {len(created_files)}")
    assert len(created_files) > 0, "Expected at least 1 partition file created!"

    for f in created_files:
        print(f"  -> {f}")

if __name__ == "__main__":
    test_scrape_and_save()
