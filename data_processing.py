# file: data_processing.py
import os
import json
import pandas as pd
import numpy as np
from google.cloud import bigquery

def get_bigquery_data():
    """
    Kết nối đến BigQuery sử dụng biến môi trường:
        GCP_SERVICE_ACCOUNT (chứa JSON credentials),
        GOOGLE_BIGQUERY_TABLE (tên bảng: project.dataset.table).
    Trả về DataFrame với các cột cần thiết.
    """
    try:
        credentials_json = os.getenv("GCP_SERVICE_ACCOUNT", "{}")
        credentials_info = json.loads(credentials_json)
        table = os.getenv("GOOGLE_BIGQUERY_TABLE", "project.dataset.table")

        client = bigquery.Client.from_service_account_info(credentials_info)
        
        query = f"""
            SELECT 
                `Receipt Date`,
                `Sample Name`,
                `Sample Type`,
                `Lot number`,
                `Sample ID`,
                `Test description`,
                `Actual result`,
                Inspec,
                `Lower limit`,
                `Upper limit`,
                `Category description`,
                `Spec description`,
                `Spec category`,
                Spec,
                `Charge department`
            FROM `{table}`
        """
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        print(f"[ERROR] get_bigquery_data: {e}")
        return None

def parse_ddmmyy(s):
    """
    Parse chuỗi 6 ký tự dạng DDMMYY thành datetime.
    Ví dụ: '020125' -> 2025-01-02.
    Trả về None nếu parse thất bại.
    """
    s = str(s).strip()
    if len(s) < 6:
        return None
    s6 = s[:6]
    try:
        return pd.to_datetime(s6, format='%d%m%y', errors='coerce')
    except:
        return None

def process_lot_dates(row):
    """
    Tách thông tin từ cột 'Lot number' để lấy ra:
      - warehouse_date
      - supplier_date
      - supplier_name
    """
    lot_number = str(row.get('Lot number', '')).strip()
    sample_type = str(row.get('Sample Type', '')).strip().lower()
    parts = [p.strip() for p in lot_number.split('-') if p.strip()]
    warehouse_date = None
    supplier_date = None
    supplier_name = None

    if "rm" in sample_type or "raw material" in sample_type or "pg" in sample_type or "packaging" in sample_type:
        if len(parts) >= 1:
            warehouse_date = parse_ddmmyy(parts[0])
        if len(parts) >= 2 and parts[1] != 'MBP':
            supplier_name = parts[1]
        if len(parts) >= 3:
            supplier_date = parse_ddmmyy(parts[2])
    else:
        if len(parts) >= 1:
            supplier_date = parse_ddmmyy(parts[0])
    return pd.Series([warehouse_date, supplier_date, supplier_name])

def unify_date(row):
    """
    Tạo cột 'final_date' thống nhất:
      - Với RM/PG: dùng warehouse_date nếu có, nếu không thì supplier_date
      - Với loại khác: dùng 'Receipt Date' nếu có, nếu không thì supplier_date
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

def remove_outliers(df, column='Actual result', method='IQR', factor=1.5):
    """
    Loại bỏ outlier theo cột chỉ định, trả về (df_cleaned, df_outliers).
    """
    if df is None or df.empty or column not in df.columns:
        return df, pd.DataFrame()

    if not pd.api.types.is_numeric_dtype(df[column]):
        return df, pd.DataFrame()

    if method == 'IQR':
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - factor * IQR
        upper_bound = Q3 + factor * IQR
    else:
        mean_val = df[column].mean()
        std_val = df[column].std()
        lower_bound = mean_val - factor * std_val
        upper_bound = mean_val + factor * std_val

    df_outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
    df_cleaned = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]

    return df_cleaned, df_outliers

def prepare_data():
    """
    - Lấy dữ liệu từ BigQuery
    - Bỏ các dòng Charge department = SHE.MBP, MFG.MBP
    - Convert 'Receipt Date' sang datetime
    - Tách cột Lot number -> warehouse_date, supplier_date, supplier_name
    - final_date = unify_date(...)
    - Convert Actual result sang numeric
    - remove_outliers(Actual result)
    Trả về (df_cleaned, df_outliers)
    """
    try:
        df = get_bigquery_data()
        if df is None or df.empty:
            return None, None

        # Loại bỏ SHE.MBP, MFG.MBP
        if "Charge department" in df.columns:
            df = df[~df["Charge department"].isin(["SHE.MBP", "MFG.MBP"])]

        if "Receipt Date" in df.columns:
            df['Receipt Date'] = pd.to_datetime(df['Receipt Date'], errors='coerce', dayfirst=True)
        
        lot_info = df.apply(process_lot_dates, axis=1)
        lot_info.columns = ['warehouse_date', 'supplier_date', 'supplier_name']
        df = pd.concat([df, lot_info], axis=1)
        
        df["final_date"] = df.apply(unify_date, axis=1)
        
        if "Actual result" in df.columns:
            df['Actual result'] = pd.to_numeric(
                df['Actual result'].astype(str)
                  .str.replace(',', '.')
                  .str.extract(r'(\d+\.?\d*)')[0],
                errors='coerce'
            )
        
        df_cleaned, df_outliers = remove_outliers(df, column='Actual result', method='IQR', factor=1.5)
        return df_cleaned, df_outliers

    except Exception as e:
        print(f"[ERROR] prepare_data: {e}")
        return None, None
