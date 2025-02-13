import pandas as pd
import numpy as np
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

def get_google_sheet_data(spreadsheet_id, range_name):
    """
    Connect to Google Sheets and get data
    """
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = Credentials.from_service_account_file(
        'path_to_your_credentials.json',  # You'll need to update this path
        scopes=scopes
    )
    
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(
        spreadsheetId=spreadsheet_id,
        range=range_name
    ).execute()
    
    values = result.get('values', [])
    df = pd.DataFrame(values[1:], columns=values[0])
    return df

def process_lot_dates(row):
    """
    Extract warehouse and supplier dates from lot numbers
    Only process for RM and PG sample types
    """
    lot_number = row['Lot number']
    sample_type = row['Sample Type']
    
    if not isinstance(lot_number, str):
        return pd.Series([None, None, None])
        
    if sample_type not in ['RM - Raw material', 'PG - Packaging']:
        # For other sample types, just extract the first date if it exists
        parts = lot_number.split('-')
        warehouse_date = None
        if len(parts[0]) >= 4:
            try:
                warehouse_date = pd.to_datetime(parts[0][:4] + '25', format='%m%d%y')
            except:
                pass
        return pd.Series([warehouse_date, None, None])
    
    parts = lot_number.split('-')
    warehouse_date = None
    supplier_date = None
    supplier_name = None
    
    if len(parts) >= 2:
        # Extract warehouse date (first date)
        if len(parts[0]) >= 4:
            try:
                warehouse_date = pd.to_datetime(parts[0][:4] + '25', format='%m%d%y')
            except:
                pass
        
        # Extract supplier name (excluding MBP)
        supplier_name = parts[1] if parts[1] != 'MBP' else None
        
        # For format like "100225-KIB09-080225-MBP"
        if len(parts) >= 3 and len(parts[2]) >= 4:
            try:
                supplier_date = pd.to_datetime(parts[2][:4] + '25', format='%m%d%y')
            except:
                pass
    
    return pd.Series([warehouse_date, supplier_date, supplier_name])

def prepare_data(spreadsheet_id, range_name):
    """
    Prepare data for analysis
    """
    # Get data from Google Sheets
    df = get_google_sheet_data(spreadsheet_id, range_name)
    
    # Convert Receipt Date to datetime
    df['Receipt Date'] = pd.to_datetime(df['Receipt Date'])
    
    # Process lot numbers
    lot_info = df.apply(process_lot_dates, axis=1)
    lot_info.columns = ['warehouse_date', 'supplier_date', 'supplier_name']
    df = pd.concat([df, lot_info], axis=1)
    
    # Convert Actual result to numeric, removing any non-numeric characters
    df['Actual result'] = pd.to_numeric(
        df['Actual result'].str.replace(',', '.').str.extract('(\d+\.?\d*)')[0],
        errors='coerce'
    )
    
    return df
