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
        range_name = f'{sheet_name}!A:M'  # Điều chỉnh cột tùy nhu cầu

        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()

        values = result.get('values', [])
        if not values:
            raise ValueError("No data found in the sheet")

        # Dòng đầu tiên là header, các dòng sau là dữ liệu
        df = pd.DataFrame(values[1:], columns=values[0])
        # Loại bỏ khoảng trắng thừa trong tên các cột
        df.columns = df.columns.str.strip()
        return df

    except Exception as e:
        st.error(f"Error accessing Google Sheets: {str(e)}")
        return None


def parse_ddmmyy(s):
    """
    Parse chuỗi 6 ký tự dạng DDMMYY thành datetime (ví dụ '020125' -> 2025-01-02).
    Trả về None nếu parse thất bại.
    """
    s = s.strip()
    if len(s) < 6:
        return None
    s6 = s[:6]  # Lấy đúng 6 ký tự đầu
    try:
        return pd.to_datetime(s6, format='%d%m%y', errors='coerce')
    except:
        return None


def process_lot_dates(row):
    """
    Tách ngày warehouse_date và supplier_date từ 'Lot number'.

    Quy ước:
    - RM/PG: có 2 ngày. Ví dụ: '020125-KIB08-291224-MBP'
      -> warehouse_date = 02/01/2025, supplier_date = 29/12/2024
      -> supplier_name = 'KIB08' (nếu != 'MBP')
    - IP/FG/GHP/...: chỉ có 1 ngày. Ví dụ: '020125-F2-MBP'
      -> supplier_date = 02/01/2025, warehouse_date = None
    """
    lot_number = str(row.get('Lot number', '')).strip()
    sample_type = str(row.get('Sample Type', '')).strip().lower()

    # Tách theo dấu '-' và loại bỏ phần tử rỗng
    parts = [p.strip() for p in lot_number.split('-') if p.strip()]

    warehouse_date = None
    supplier_date = None
    supplier_name = None

    # Nếu Sample Type thuộc RM/PG (so khớp linh hoạt)
    if "rm" in sample_type or "raw material" in sample_type or "pg" in sample_type or "packaging" in sample_type:
        if len(parts) >= 1:
            warehouse_date = parse_ddmmyy(parts[0])
        if len(parts) >= 2 and parts[1] != 'MBP':
            supplier_name = parts[1]
        if len(parts) >= 3:
            supplier_date = parse_ddmmyy(parts[2])
    else:
        # Với các loại khác (IP, FG, GHP, ...)
        if len(parts) >= 1:
            supplier_date = parse_ddmmyy(parts[0])

    return pd.Series([warehouse_date, supplier_date, supplier_name])


def unify_date(row):
    """
    Trả về cột ngày thống nhất (final_date) cho mọi loại Sample:
      - Nếu là RM/PG: dùng warehouse_date (nếu có) hoặc supplier_date.
      - Nếu là IP/FG/GHP/...: dùng Receipt Date (nếu có), nếu không có thì fallback supplier_date.
    """
    sample_type = str(row.get("Sample Type", "")).lower()
    if "rm" in sample_type or "raw material" in sample_type or "pg" in sample_type or "packaging" in sample_type:
        if pd.notnull(row.get("warehouse_date")):
            return row["warehouse_date"]
        else:
            return row["supplier_date"]
    else:
        if "Receipt Date" in row and pd.notnull(row.get("Receipt Date")):
            return row["Receipt Date"]
        else:
            return row["supplier_date"]


def prepare_data():
    """
    Xử lý dữ liệu cho dashboard.
    """
    try:
        print("Available secrets sections:", st.secrets.keys())
        sheet_url = st.secrets["sheet"]["url"]
        spreadsheet_id = sheet_url.split('/')[5]
        print("Spreadsheet ID:", spreadsheet_id)

        df = get_google_sheet_data(spreadsheet_id)
        if df is None:
            return None

        if "Receipt Date" in df.columns:
            df['Receipt Date'] = pd.to_datetime(df['Receipt Date'], errors='coerce', dayfirst=True)

        # Tách các thông tin từ Lot number
        lot_info = df.apply(process_lot_dates, axis=1)
        lot_info.columns = ['warehouse_date', 'supplier_date', 'supplier_name']
        df = pd.concat([df, lot_info], axis=1)

        # Tạo cột final_date thống nhất
        df["final_date"] = df.apply(unify_date, axis=1)

        if "Actual result" in df.columns:
            df['Actual result'] = pd.to_numeric(
                df['Actual result'].str.replace(',', '.').str.extract(r'(\d+\.?\d*)')[0],
                errors='coerce'
            )

        return df

    except Exception as e:
        st.error(f"Error processing data: {str(e)}")
        return None
