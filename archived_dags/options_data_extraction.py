from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, time
from pytz import timezone
import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Set timezone
local_tz = timezone("Asia/Kolkata")

# ChromeDriver Path inside Docker
CHROMEDRIVER_PATH = "/usr/local/bin/chromedriver"

# Set the base download directory
DOWNLOAD_BASE_DIR = "/opt/airflow/extracted_data/nifty_option_chain"

# Function to get next 3 expiry Thursdays
def get_next_thursdays():
    today = datetime.now(local_tz).date()
    thursdays = []
    
    while len(thursdays) < 3:
        if today.weekday() == 3:  # Thursday
            thursdays.append(today.strftime('%d-%b-%Y'))
        today += timedelta(days=1)

    print(f"✅ Next 3 expiry dates: {thursdays}")
    return thursdays  # Returning a valid list


def extract_options_data():
    """
    Gets expiry dates, visits Upstox, and downloads CSV for each expiry.
    """
    expiry_dates = get_next_thursdays()

    for expiry in expiry_dates:
        expiry_dir = os.path.join(DOWNLOAD_BASE_DIR, expiry.replace(" ", "_"))
        os.makedirs(expiry_dir, exist_ok=True)

        # Chrome options
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.binary_location = "/usr/bin/google-chrome"
        
        prefs = {
            "download.default_directory": expiry_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)

        # Initialize WebDriver
        service = Service(CHROMEDRIVER_PATH)
        driver = webdriver.Chrome(service=service, options=chrome_options)

        url = f"https://upstox.com/option-chain/nifty/?expiry={expiry}"
        driver.get(url)

        try:
            wait = WebDriverWait(driver, 15)

            # Try clicking "Download CSV"
            try:
                download_div = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Download CSV')]")))
                ActionChains(driver).move_to_element(download_div).click().perform()
                print(f"✅ [{expiry}] Download CSV div clicked!")
            except Exception:
                print(f"⚠️ [{expiry}] Download CSV div not found, trying the download icon.")

                # Click the download icon (if div fails)
                download_icon = wait.until(EC.element_to_be_clickable((By.XPATH, "//img[@alt='download']")))
                ActionChains(driver).move_to_element(download_icon).click().perform()
                print(f"✅ [{expiry}] Download icon clicked!")

            # Wait for the file to download
            time.sleep(10)

            # Check if CSV file exists
            downloaded_files = [f for f in os.listdir(expiry_dir) if f.endswith(".csv")]
            if downloaded_files:
                print(f"✅ [{expiry}] File downloaded successfully: {downloaded_files[0]}")
            else:
                print(f"❌ [{expiry}] File download failed!")

        except Exception as e:
            print(f"❌ [{expiry}] Error during download: {e}")

        driver.quit()


# Define DAG default arguments
default_args = {
    'owner': 'Ballal',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 6, tzinfo=local_tz),
    'retries': 4,
    'retry_delay': timedelta(minutes=1),
}

# Define DAG
dag = DAG(
    'options_selenium_pipeline',
    default_args=default_args,
    description='Fetch and save Nifty option chain data',
    schedule_interval='*/1 9-16 * * 1-5',  # Runs every minute during trading hours
    catchup=False,
)

# Single task to get expiry dates and download CSVs
task_fetch_data = PythonOperator(
    task_id='fetch_nifty_options_data',
    python_callable=extract_options_data,
    dag=dag,
)

task_fetch_data
