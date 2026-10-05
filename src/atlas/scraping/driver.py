"""
Selenium Remote WebDriver Factory.
Connects to the standalone Chrome container over the Docker network.
"""

import os

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def get_driver(
    remote_url: str = None,
    headless: bool = True,
    window_size: str = "1920,1080",
) -> webdriver.Remote:
    """
    Creates and returns a remote Chrome WebDriver connected to the Selenium container.
    """
    if remote_url is None:
        remote_url = os.environ.get("SELENIUM_REMOTE_URL", "http://selenium:4444/wd/hub")

    options = Options()
    if headless:
        options.add_argument("--headless=new")

    # Anti-bot and stability flags
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(f"--window-size={window_size}")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Remote(command_executor=remote_url, options=options)

    # Remove navigator.webdriver flag dynamically
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
    )

    driver.set_page_load_timeout(120)
    driver.set_script_timeout(60)

    return driver
