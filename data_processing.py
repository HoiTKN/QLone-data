import pandas as pd
import numpy as np
import streamlit as st
from google.cloud import bigquery

def get_bigquery_data():
    """
    Kết nối đến Google BigQuery và lấy dữ liệu bằng cách sử dụng thông tin từ st.secrets.
    Yêu cầu st.secrets phải chứa:
      - gcp_service_account: Thông tin xác thực của Service Account.
      - bigquery.table: Tên bảng đầy đủ (project.dataset.table).
    """
    try:
        # Lấy thông tin credentials từ st.secrets
        credentials_info = st.secrets["gcp_service_account"]
        client = bigquery.Client.from_service_account_info(credentials_info)
        
        # Lấy tên bảng từ st.secrets
        table = st.secrets["GOOGLE_BIGQUERY"]["table"]
        query = f"SELECT * FROM `{table}`"
        
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error accessing BigQuery: {str(e)}")
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
    Lưu ý: Dữ liệu được lấy từ Google BigQuery thay vì Google Sheets.
    """
    try:
        # Lấy dữ liệu từ BigQuery
        df = get_bigquery_data()
        if df is None:
            return None

        # Nếu có cột "Receipt Date", chuyển đổi sang kiểu datetime
        if "Receipt Date" in df.columns:
            df['Receipt Date'] = pd.to_datetime(df['Receipt Date'], errors='coerce', dayfirst=True)

        # Tách thông tin từ "Lot number"
        lot_info = df.apply(process_lot_dates, axis=1)
        lot_info.columns = ['warehouse_date', 'supplier_date', 'supplier_name']
        df = pd.concat([df, lot_info], axis=1)

        # Tạo cột final_date thống nhất
        df["final_date"] = df.apply(unify_date, axis=1)

        # Chuyển đổi "Actual result" sang dạng số, nếu có
        if "Actual result" in df.columns:
            df['Actual result'] = pd.to_numeric(
                df['Actual result'].astype(str).str.replace(',', '.').str.extract(r'(\d+\.?\d*)')[0],
                errors='coerce'
            )

        return df

    except Exception as e:
        st.error(f"Error processing data: {str(e)}")
        return None
