import pandas as pd
import numpy as np
import streamlit as st
from google.cloud import bigquery

def get_bigquery_data():
    """
    Kết nối đến BigQuery sử dụng thông tin từ st.secrets và trả về DataFrame.
    Yêu cầu st.secrets phải chứa:
      - [gcp_service_account]: Thông tin xác thực của Service Account.
      - [GOOGLE_BIGQUERY]: Chứa key "table" với giá trị là tên bảng đầy đủ (project.dataset.table).
    Chỉ lấy các cột cần thiết nhằm giảm kích thước dữ liệu:
      - Receipt Date, Sample Name, Sample Type, Lot number, Sample ID,
        Test description, Actual result, Inspec, Lower limit, Upper limit,
        Category description, Spec description, Spec category, Spec.
    """
    try:
        credentials_info = st.secrets["gcp_service_account"]
        client = bigquery.Client.from_service_account_info(credentials_info)
        table = st.secrets["GOOGLE_BIGQUERY"]["table"]
        
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
                Spec
            FROM `{table}`
        """
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error accessing BigQuery: {str(e)}")
        return None

def parse_ddmmyy(s):
    """
    Parse chuỗi 6 ký tự dạng DDMMYY thành datetime.
    Ví dụ: '020125' -> 2025-01-02.
    Trả về None nếu parse thất bại.
    """
    s = s.strip()
    if len(s) < 6:
        return None
    s6 = s[:6]
    try:
        return pd.to_datetime(s6, format='%d%m%y', errors='coerce')
    except Exception:
        return None

def process_lot_dates(row):
    """
    Tách thông tin từ cột 'Lot number' để lấy ra:
      - warehouse_date: ngày kho (ví dụ: '02/01/2025' trong RM/PG)
      - supplier_date: ngày của nhà cung cấp (ví dụ: '29/12/2024' trong RM/PG, 
        hoặc chỉ có một ngày cho IP/FG/GHP)
      - supplier_name: tên nhà cung cấp (nếu có, ví dụ: 'KIB08' nếu khác 'MBP')
    """
    lot_number = str(row.get('Lot number', '')).strip()
    sample_type = str(row.get('Sample Type', '')).strip().lower()
    parts = [p.strip() for p in lot_number.split('-') if p.strip()]
    warehouse_date = None
    supplier_date = None
    supplier_name = None

    # Nếu Sample Type thuộc RM/PG (hoặc Raw material, Packaging)
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
    Tạo cột 'final_date' cho dữ liệu:
      - Với RM/PG: sử dụng warehouse_date nếu có, nếu không có thì supplier_date.
      - Với các loại khác: sử dụng 'Receipt Date' nếu có, nếu không thì supplier_date.
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
    Loại bỏ các outlier khỏi DataFrame dựa trên cột chỉ định.
    - df: DataFrame đầu vào
    - column: tên cột numeric để xác định outlier (mặc định là 'Actual result')
    - method: phương pháp xác định outlier (mặc định dùng IQR)
    - factor: hệ số nhân cho IQR, mặc định là 1.5

    Trả về:
      - df_cleaned: DataFrame đã loại bỏ outlier
      - df_outliers: DataFrame chứa các hàng bị coi là outlier
    """
    if df is None or df.empty or column not in df.columns:
        return df, pd.DataFrame()

    # Đảm bảo cột là numeric
    if not pd.api.types.is_numeric_dtype(df[column]):
        return df, pd.DataFrame()

    if method == 'IQR':
        # Tính Q1, Q3
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - factor * IQR
        upper_bound = Q3 + factor * IQR
    else:
        # Nếu muốn dùng std-based
        mean_val = df[column].mean()
        std_val = df[column].std()
        lower_bound = mean_val - factor * std_val
        upper_bound = mean_val + factor * std_val

    # Tách ra 2 DataFrame: outlier và clean
    df_outliers = df[(df[column] < lower_bound) | (df[column] > upper_bound)]
    df_cleaned = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]

    return df_cleaned, df_outliers

def prepare_data():
    """
    Chuẩn bị dữ liệu cho dashboard:
      - Lấy dữ liệu từ BigQuery (chỉ lấy 14 cột cần thiết).
      - Nếu có cột "Receipt Date", chuyển đổi sang kiểu datetime với dayfirst=True và infer_datetime_format=True.
      - Xử lý cột "Lot number" để tạo ra các cột phụ: warehouse_date, supplier_date, supplier_name.
      - Tạo cột 'final_date' thống nhất.
      - Chuyển đổi "Actual result" sang dạng số (numeric).
      - Loại bỏ outlier (nếu có) và trả về hai DataFrame: clean & outliers.
    """
    try:
        df = get_bigquery_data()
        if df is None or df.empty:
            return None, None

        # Chuyển đổi "Receipt Date" nếu có
        if "Receipt Date" in df.columns:
            df['Receipt Date'] = pd.to_datetime(df['Receipt Date'], 
                                                errors='coerce', 
                                                dayfirst=True, 
                                                infer_datetime_format=True)

        # Xử lý "Lot number" để lấy thông tin bổ sung
        lot_info = df.apply(process_lot_dates, axis=1)
        lot_info.columns = ['warehouse_date', 'supplier_date', 'supplier_name']
        df = pd.concat([df, lot_info], axis=1)

        # Tạo cột 'final_date'
        df["final_date"] = df.apply(unify_date, axis=1)

        # Chuyển đổi "Actual result" sang dạng số
        if "Actual result" in df.columns:
            df['Actual result'] = pd.to_numeric(
                df['Actual result'].astype(str)
                  .str.replace(',', '.')
                  .str.extract(r'(\d+\.?\d*)')[0],
                errors='coerce'
            )

        # Loại bỏ outlier (nếu cần)
        df_cleaned, df_outliers = remove_outliers(df, column='Actual result', method='IQR', factor=1.5)

        return df_cleaned, df_outliers

    except Exception as e:
        st.error(f"Error processing data: {str(e)}")
        return None, None
