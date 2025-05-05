import logging
import requests
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from datetime import datetime


# Fetch cookies from NSE using Selenium
def fetch_cookies_with_selenium():
    logging.info("Fetching cookies with Selenium...")
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")

    driver = webdriver.Chrome(options=chrome_options)
    try:
        driver.get("https://www.nseindia.com/market-data/exchange-traded-funds-etf")
        driver.maximize_window()
        time.sleep(5)  # Wait for the page to load
        cookies = {cookie['name']: cookie['value'] for cookie in driver.get_cookies()}
        logging.info("Cookies fetched successfully.")
        return cookies
    except Exception as e:
        logging.error(f"Error fetching cookies: {e}")
        return None
    finally:
        driver.quit()


# Download ETF data
def download_csv_with_cookies(cookies):
    logging.info("Downloading ETF data CSV...")
    csv_url = "https://www.nseindia.com/api/etf?csv=true&selectValFormat=crores"
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.nseindia.com/market-data/exchange-traded-funds-etf'}

    try:
        session = requests.Session()
        session.headers.update(headers)
        session.cookies.update(cookies)
        response = session.get(csv_url)
        response.raise_for_status()
        file_name = f"ETF_Data_{datetime.now().strftime('%Y-%m-%d')}.csv"
        with open(file_name, 'wb') as file:
            file.write(response.content)
        logging.info(f"ETF data saved as {file_name}")
        return file_name
    except Exception as e:
        logging.error(f"Error downloading ETF data: {e}")
        return None


# Main function to fetch ETF data
def fetch_etf_data():
    """
    Fetch ETF data by using cookies fetched via Selenium and saving the CSV.
    """
    print("Fetching ETF data...")
    cookies = fetch_cookies_with_selenium()
    if not cookies:
        print("Failed to fetch cookies. Exiting.")
        return None

    file_name = download_csv_with_cookies(cookies)
    if not file_name:
        print("Failed to download ETF CSV. Exiting.")
        return None

    print(f"ETF data successfully downloaded: {file_name}")
    return file_name
