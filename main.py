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

def setup_chrome_options():
    """Setup Chrome options for running in GitHub Actions"""
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')  # Run in headless mode
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    
    # Set up download preferences
    download_dir = os.getcwd()  # Use current directory for downloads
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    return chrome_options

def wait_for_download(initial_files, download_dir, timeout=120):
    """Wait for download to complete and return the new file path"""
    print("Waiting for file download...")
    end_time = time.time() + timeout
    
    while time.time() < end_time:
        current_files = set(glob.glob(os.path.join(download_dir, "*")))
        new_files = current_files - initial_files
        new_files = {f for f in new_files if not f.endswith('.crdownload') and not f.endswith('.tmp')}
        
        if new_files:
            newest_file = max(new_files, key=os.path.getctime)
            print(f"Found new file: {newest_file}")
            return newest_file
            
        time.sleep(5)
    
    return None

def process_data(df):
    """Process the DataFrame according to requirements"""
    # Filter out MFG.MBP and SHE.MBP from Charge department
    df = df[~df['Charge department'].isin(['MFG.MBP', 'SHE.MBP'])]
    
    # Remove rows where Test starts with 'CQ'
    df = df[~df['Test'].str.startswith('CQ', na=False)]
    
    # Keep only specified columns
    columns_to_keep = [
        'Receipt Date', 'Sample Name', 'Sample Type', 'Lot number',
        'Test description', 'Actual result', 'Lower limit', 'Upper limit',
        'Category description', 'Spec description', 'Spec category'
    ]
    
    return df[columns_to_keep]

def upload_to_google_sheets(df, credentials_path, sheet_url):
    """Upload DataFrame to Google Sheets"""
    gs_scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, gs_scope)
    gs_client = gspread.authorize(credentials)
    
    spreadsheet = gs_client.open_by_url(sheet_url)
    worksheet = spreadsheet.get_worksheet(0)
    
    existing_data = worksheet.get_all_values()
    last_row = len(existing_data)
    
    start_row = 1 if last_row == 0 else last_row + 1
    include_header = last_row == 0
    
    set_with_dataframe(
        worksheet,
        df,
        row=start_row,
        include_index=False,
        include_column_header=include_header,
        resize=False
    )
    
    print(f"Successfully uploaded {len(df)} new rows!")

def main():
    try:
        # Setup Chrome
        chrome_options = setup_chrome_options()
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        download_dir = os.getcwd()
        
        try:
            # Login to website
            driver.get("https://qlone.masancloud.com/sample-report.html")
            time.sleep(3)
            
            username_field = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "username"))
            )
            password_field = driver.find_element(By.NAME, "password")
            login_button = driver.find_element(By.XPATH, "//input[@type='submit' and @value='Login']")
            
            username_field.send_keys(os.environ['USERNAME'])
            password_field.send_keys(os.environ['PASSWORD'])
            login_button.click()
            print("Login successful!")
            time.sleep(5)
            
            # Select Site receive
            site_receive_container = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//label[contains(text(),'Site receive')]/following-sibling::*[1]")
                )
            )
            site_receive_container.click()
            time.sleep(1)
            
            site_option = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//li[contains(@class, 'select2-results__option') and normalize-space()='MBP - Nhà máy Masan Miền Bắc - MMB']")
                )
            )
            site_option.click()
            time.sleep(2)
            
            # Set date
            today = datetime.now()
            yesterday = today - timedelta(days=1)
            date_str = yesterday.strftime("%d/%m/%Y")
            
            request_date_field = driver.find_element(By.ID, "request_date")
            request_date_field.clear()
            request_date_field.send_keys(f"{date_str} - {date_str}")
            time.sleep(2)
            
            # Export and download
            initial_files = set(glob.glob(os.path.join(download_dir, "*")))
            
            export_button = driver.find_element(By.ID, "btnExport1")
            export_button.click()
            time.sleep(5)

            download_button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//a[@id='hidenPopup' and contains(text(),'Download')]")
                )
            )
            download_button.click()
            time.sleep(5)

            # Wait for download
            downloaded_file = wait_for_download(initial_files, download_dir)
            if not downloaded_file:
                raise Exception("No download file found after 120 seconds.")
            
            # Rename file
            fixed_file_path = os.path.join(download_dir, "Sample.xlsx")
            if os.path.exists(fixed_file_path):
                os.remove(fixed_file_path)
            os.rename(downloaded_file, fixed_file_path)
            
            # Process data
            df = pd.read_excel(fixed_file_path)
            df_processed = process_data(df)
            
            # Upload to Google Sheets
            credentials_path = "credentials.json"
            sheet_url = "https://docs.google.com/spreadsheets/d/1EUV8ZWSBeGWVgi3HShlpAd3rEFBJdjTBdQWzHFJkRhI/edit?gid=0#gid=0"
            
            upload_to_google_sheets(df_processed, credentials_path, sheet_url)

        finally:
            driver.quit()

    except Exception as e:
        print(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
