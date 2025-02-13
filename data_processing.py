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
    Tách ngày warehouse và supplier từ 'Lot number'.
    Chỉ áp dụng cho các sample type RM - Raw material và PG - Packaging.
    """
    lot_number = row['Lot number']
    sample_type = row['Sample Type']

    if not isinstance(lot_number, str):
        return pd.Series([None, None, None])

    if sample_type not in ['RM - Raw material', 'PG - Packaging']:
        parts = lot_number.split('-')
        warehouse_date = None
        if len(parts[0]) >= 4:
            try:
                warehouse_date = pd.to_datetime(parts[0][:4] + '25', format='%m%d%y')
            except Exception:
                pass
        return pd.Series([warehouse_date, None, None])

    parts = lot_number.split('-')
    warehouse_date = None
    supplier_date = None
    supplier_name = None

    if len(parts) >= 2:
        if len(parts[0]) >= 4:
            try:
                warehouse_date = pd.to_datetime(parts[0][:4] + '25', format='%m%d%y')
            except Exception:
                pass

        supplier_name = parts[1] if parts[1] != 'MBP' else None

        if len(parts) >= 3 and len(parts[2]) >= 4:
            try:
                supplier_date = pd.to_datetime(parts[2][:4] + '25', format='%m%d%y')
            except Exception:
                pass

    return pd.Series([warehouse_date, supplier_date, supplier_name])


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

        # Chuyển đổi cột "Receipt Date" với dayfirst=True để phù hợp với định dạng "18-01-2025 09:50:52"
        df['Receipt Date'] = pd.to_datetime(df['Receipt Date'], errors='coerce', dayfirst=True)

        # Xử lý cột 'Lot number'
        lot_info = df.apply(process_lot_dates, axis=1)
        lot_info.columns = ['warehouse_date', 'supplier_date', 'supplier_name']
        df = pd.concat([df, lot_info], axis=1)

        # Chuyển đổi "Actual result" thành số (loại bỏ ký tự không phải số)
        df['Actual result'] = pd.to_numeric(
            df['Actual result'].str.replace(',', '.').str.extract(r'(\d+\.?\d*)')[0],
            errors='coerce'
        )

        return df

    except Exception as e:
        st.error(f"Error processing data: {str(e)}")
        return None
