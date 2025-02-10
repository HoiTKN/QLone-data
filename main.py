import os
import glob
import time
from datetime import datetime, timedelta
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, WebDriverException

def setup_chrome_options():
    """Setup Chrome options for running in GitHub Actions"""
    chrome_options = webdriver.ChromeOptions()
    
    # Basic Chrome settings
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    
    # Network and timeout settings
    chrome_options.add_argument('--dns-prefetch-disable')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--proxy-server="direct://"')
    chrome_options.add_argument('--proxy-bypass-list=*')
    
    # SSL and security settings
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--allow-insecure-localhost')
    chrome_options.add_argument('--allow-running-insecure-content')
    
    # Performance settings
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--disable-infobars')
    chrome_options.page_load_strategy = 'normal'
    
    # Additional preferences
    prefs = {
        'download.default_directory': os.getcwd(),
        'download.prompt_for_download': False,
        'download.directory_upgrade': True,
        'safebrowsing.enabled': True,
        'profile.default_content_setting_values.automatic_downloads': 1
    }
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    return chrome_options

def create_driver():
    """Create and configure WebDriver with appropriate timeouts"""
    print("Initializing Chrome WebDriver...")
    service = Service(ChromeDriverManager().install())
    options = setup_chrome_options()
    
    driver = webdriver.Chrome(service=service, options=options)
    
    # Set various timeouts
    driver.set_script_timeout(30)
    driver.set_page_load_timeout(60)
    
    # Set window size
    driver.set_window_size(1920, 1080)
    
    return driver

def safe_get_url(driver, url, max_retries=3):
    """Safely navigate to URL with retries"""
    for attempt in range(max_retries):
        try:
            print(f"Attempting to access {url} (attempt {attempt + 1}/{max_retries})")
            driver.get(url)
            return True
        except TimeoutException:
            print(f"Timeout on attempt {attempt + 1}")
            if attempt < max_retries - 1:
                print("Refreshing driver and retrying...")
                driver.execute_script("window.stop();")
                time.sleep(5)
            else:
                raise
        except WebDriverException as e:
            print(f"WebDriver error on attempt {attempt + 1}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                raise

def main():
    driver = None
    try:
        print("Starting the script...")
        
        # Initialize WebDriver
        driver = create_driver()
        print("WebDriver initialized successfully")
        
        # Access the website
        print("Attempting to access website...")
        site_url = "https://qlone.masancloud.com/sample-report.html"
        if not safe_get_url(driver, site_url):
            raise Exception("Failed to access the website after maximum retries")
        
        print("Successfully accessed the website")
        time.sleep(5)  # Give some time for the page to stabilize
        
        # Login process
        print("Starting login process...")
        username_field = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.NAME, "username"))
        )
        password_field = driver.find_element(By.NAME, "password")
        
        username_field.clear()
        password_field.clear()
        time.sleep(1)
        
        username_field.send_keys(os.environ['USERNAME'])
        time.sleep(1)
        password_field.send_keys(os.environ['PASSWORD'])
        time.sleep(1)
        
        login_button = driver.find_element(By.XPATH, "//input[@type='submit' and @value='Login']")
        login_button.click()
        print("Login credentials submitted")
        
        # Rest of your existing code for site selection, date setting, etc.
        # [Previous implementation continues here...]
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        if driver:
            print("Current page source:", driver.page_source)
        raise
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    main()
