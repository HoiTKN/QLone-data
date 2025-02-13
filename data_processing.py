# data_processing.py

import pandas as pd
import numpy as np
import streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

def get_google_sheet_data(spreadsheet_id):
    """
    Kết nối Google Sheets và lấy dữ liệu, dùng credentials từ st.secrets
    """
    # Tạo dict credentials từ phần [gcp_service_account] trong secrets
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
        # Tạo credentials từ service account info
        creds = Credentials.from_service_account_info(credentials_dict, scopes=scopes)

        # Tạo service kết nối Sheets API
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Lấy thông tin metadata của sheet để xác định range
        sheet_metadata = sheet.get(spreadsheetId=spreadsheet_id).execute()
        properties = sheet_metadata.get('sheets')[0].get('properties')
        sheet_name = properties.get('title')
        
        # Ở đây ví dụ lấy cột A:M, bạn điều chỉnh tùy nhu cầu
        range_name = f'{sheet_name}!A:M'

        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()

        values = result.get('values', [])
        if not values:
            raise ValueError("No data found in the sheet")

        # Dòng đầu tiên (values[0]) là header, các dòng sau là data
        df = pd.DataFrame(values[1:], columns=values[0])
        return df

    except Exception as e:
        st.error(f"Error accessing Google Sheets: {str(e)}")
        return None


def process_lot_dates(row):
    """
    Tách ngày warehouse và supplier từ 'Lot number'
    Chỉ áp dụng cho RM - Raw material và PG - Packaging
    """
    lot_number = row['Lot number']
    sample_type = row['Sample Type']

    # Trường hợp lot_number không phải chuỗi
    if not isinstance(lot_number, str):
        return pd.Series([None, None, None])

    # Nếu sample_type không phải RM hoặc PG, chỉ lấy thử date đầu
    if sample_type not in ['RM - Raw material', 'PG - Packaging']:
        parts = lot_number.split('-')
        warehouse_date = None
        if len(parts[0]) >= 4:
            try:
                warehouse_date = pd.to_datetime(parts[0][:4] + '25', format='%m%d%y')
            except:
                pass
        return pd.Series([warehouse_date, None, None])

    # Với RM và PG, tách warehouse_date, supplier_date, supplier_name
    parts = lot_number.split('-')
    warehouse_date = None
    supplier_date = None
    supplier_name = None

    if len(parts) >= 2:
        # Tách warehouse_date (phần đầu)
        if len(parts[0]) >= 4:
            try:
                warehouse_date = pd.to_datetime(parts[0][:4] + '25', format='%m%d%y')
            except:
                pass

        # Tách supplier_name (nếu != "MBP")
        supplier_name = parts[1] if parts[1] != 'MBP' else None

        # Tách supplier_date (nếu có phần thứ 3)
        if len(parts) >= 3 and len(parts[2]) >= 4:
            try:
                supplier_date = pd.to_datetime(parts[2][:4] + '25', format='%m%d%y')
            except:
                pass

    return pd.Series([warehouse_date, supplier_date, supplier_name])


def prepare_data():
    """
    Hàm chính để xử lý dữ liệu cho dashboard
    """
    try:
        # Kiểm tra các phần trong secrets (debug)
        print("Available secrets sections:", st.secrets.keys())

        # Lấy URL từ secrets (phần [sheet]) và tách ID
        sheet_url = st.secrets["sheet"]["url"]
        spreadsheet_id = sheet_url.split('/')[5]
        print("Spreadsheet ID:", spreadsheet_id)

        # Gọi hàm lấy dữ liệu từ Google Sheets
        df = get_google_sheet_data(spreadsheet_id)
        if df is None:
            return None

        # Convert cột "Receipt Date" sang datetime
        df['Receipt Date'] = pd.to_datetime(df['Receipt Date'], errors='coerce')

        # Xử lý cột lot number để tách warehouse_date, supplier_date, supplier_name
        lot_info = df.apply(process_lot_dates, axis=1)
        lot_info.columns = ['warehouse_date', 'supplier_date', 'supplier_name']
        df = pd.concat([df, lot_info], axis=1)

        # Convert "Actual result" về dạng số
        # - Thay dấu phẩy thành dấu chấm
        # - Tách chuỗi chỉ lấy phần số
        df['Actual result'] = pd.to_numeric(
            df['Actual result'].str.replace(',', '.').str.extract(r'(\d+\.?\d*)')[0],
            errors='coerce'
        )

        return df

    except Exception as e:
        st.error(f"Error processing data: {str(e)}")
        return None
