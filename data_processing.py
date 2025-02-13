# data_processing.py

import pandas as pd
import numpy as np
import streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

def get_google_sheet_data(spreadsheet_id):
    """
    Kết nối đến Google Sheets và lấy dữ liệu bằng cách sử dụng thông tin từ st.secrets.
    """
    credentials_dict = {
        "type": st.secrets["gcp_service_account"]["type"],
        "project_id": st.secrets["gcp_service_account"]["project_id"],
        "private_key_id": st.secrets["gcp_service_account"]["private_key_id"],
        "private_key": st.secrets["gcp_service_account"]["private_key"],
        "client_email": st.secrets["gcp_service_account"]["client_email"],
        "client_id": st.secrets["gcp_service_account"]["client_id"],
        "auth_uri": st.secrets["gcp_service_account"]["auth_uri"],
        "token_uri": st.secrets["gcp_service_account"]["token_uri"],
        "auth_provider_x509_cert_url": st.secrets["gcp_service_account"]["auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["gcp_service_account"]["client_x509_cert_url"]
    }

    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']

    try:
        creds = Credentials.from_service_account_info(credentials_dict, scopes=scopes)
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Lấy thông tin metadata của sheet để xác định tên sheet
        sheet_metadata = sheet.get(spreadsheetId=spreadsheet_id).execute()
        properties = sheet_metadata.get('sheets')[0].get('properties')
        sheet_name = properties.get('title')
        range_name = f'{sheet_name}!A:M'  # Điều chỉnh range nếu cần

        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()

        values = result.get('values', [])
        if not values:
            raise ValueError("No data found in the sheet")

        # Dòng đầu tiên là header, các dòng sau là dữ liệu
        df = pd.DataFrame(values[1:], columns=values[0])
        return df

    except Exception as e:
        st.error(f"Error accessing Google Sheets: {str(e)}")
        return None

def process_lot_dates(row):
    """
    Tách ngày warehouse_date và supplier_date từ 'Lot number'.

    - Nếu Sample Type là RM - Raw material hoặc PG - Packaging, 
      thì Lot number có dạng: DDMMYY-[SupplierName]DD-DDMMYY-MBP (thường 4 phần).
      -> parse 2 ngày:
         + parts[0] (DDMMYY) -> warehouse_date
         + parts[2] (DDMMYY) -> supplier_date (nếu đủ dài)
         + supplier_name = parts[1] (nếu != "MBP")
    - Nếu Sample Type khác (IP, FG, GHP...), thì chỉ có 1 ngày: DDMMYY-...
      -> parse 1 ngày:
         + parts[0] (DDMMYY) -> supplier_date
    """
    lot_number = row.get('Lot number', None)
    sample_type = row.get('Sample Type', None)

    # Mặc định trả về [None, None, None] nếu không parse được
    if not isinstance(lot_number, str):
        return pd.Series([None, None, None])

    parts = lot_number.split('-')
    # Trường hợp RM/PG
    if sample_type in ['RM - Raw material', 'PG - Packaging']:
        warehouse_date = None
        supplier_date = None
        supplier_name = None

        # parse warehouse_date từ parts[0] (nếu đủ 6 ký tự)
        if len(parts) >= 1 and len(parts[0]) == 6:
            try:
                warehouse_date = pd.to_datetime(parts[0], format='%d%m%y', errors='coerce')
            except:
                warehouse_date = None

        # parse supplier_name từ parts[1] (nếu != "MBP")
        if len(parts) >= 2:
            if parts[1] != 'MBP':
                supplier_name = parts[1]

        # parse supplier_date từ parts[2] (nếu tồn tại và đủ 6 ký tự)
        if len(parts) >= 3 and len(parts[2]) == 6:
            try:
                supplier_date = pd.to_datetime(parts[2], format='%d%m%y', errors='coerce')
            except:
                supplier_date = None

        return pd.Series([warehouse_date, supplier_date, supplier_name])

    else:
        # Trường hợp IP, FG, GHP... -> chỉ có 1 ngày (parts[0])
        supplier_date = None
        if len(parts) >= 1 and len(parts[0]) == 6:
            try:
                supplier_date = pd.to_datetime(parts[0], format='%d%m%y', errors='coerce')
            except:
                supplier_date = None

        return pd.Series([None, supplier_date, None])

def prepare_data():
    """
    Xử lý dữ liệu cho dashboard.
    """
    try:
        # Debug: in ra các key có trong st.secrets
        print("Available secrets sections:", st.secrets.keys())

        # Lấy URL từ phần [sheet] và tách lấy spreadsheet_id
        sheet_url = st.secrets["sheet"]["url"]
        spreadsheet_id = sheet_url.split('/')[5]
        print("Spreadsheet ID:", spreadsheet_id)

        # Lấy dữ liệu từ Google Sheets
        df = get_google_sheet_data(spreadsheet_id)
        if df is None:
            return None

        # Chuyển đổi cột "Receipt Date" (nếu có) với dayfirst=True 
        # để phù hợp với định dạng kiểu "18-01-2025 09:50:52"
        if "Receipt Date" in df.columns:
            df['Receipt Date'] = pd.to_datetime(df['Receipt Date'], errors='coerce', dayfirst=True)

        # Xử lý cột 'Lot number' -> tách warehouse_date, supplier_date, supplier_name
        lot_info = df.apply(process_lot_dates, axis=1)
        lot_info.columns = ['warehouse_date', 'supplier_date', 'supplier_name']
        df = pd.concat([df, lot_info], axis=1)

        # Chuyển đổi "Actual result" thành số (loại bỏ ký tự không phải số)
        if "Actual result" in df.columns:
            df['Actual result'] = pd.to_numeric(
                df['Actual result'].str.replace(',', '.').str.extract(r'(\d+\.?\d*)')[0],
                errors='coerce'
            )

        return df

    except Exception as e:
        st.error(f"Error processing data: {str(e)}")
        return None
