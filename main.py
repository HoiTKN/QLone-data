import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from data_processing import prepare_data

# Cấu hình Streamlit page
st.set_page_config(page_title="Quality Control Dashboard", layout="wide")

# Hàm load dữ liệu (cache trong 1 giờ)
@st.cache_data(ttl=3600)
def load_data():
    return prepare_data()

# Load dữ liệu từ Google Sheets qua hàm prepare_data()
df = load_data()

if df is None:
    st.error("Unable to load data. Please check your configuration.")
else:
    # ---------------------- SIDEBAR FILTERS ---------------------- #
    st.sidebar.header("Filter Options")

    # Bộ lọc theo nhóm
    # Kiểm tra và lấy các giá trị duy nhất nếu các cột tồn tại
    if "Category description" in df.columns:
        cat_options = sorted(df["Category description"].dropna().unique())
        selected_cat = st.sidebar.multiselect("Category description", options=cat_options, default=cat_options)
    else:
        selected_cat = None

    if "Spec category" in df.columns:
        spec_cat_options = sorted(df["Spec category"].dropna().unique())
        selected_spec_cat = st.sidebar.multiselect("Spec category", options=spec_cat_options, default=spec_cat_options)
    else:
        selected_spec_cat = None

    if "Spec description" in df.columns:
        spec_desc_options = sorted(df["Spec description"].dropna().unique())
        selected_spec_desc = st.sidebar.multiselect("Spec description", options=spec_desc_options, default=spec_desc_options)
    else:
        selected_spec_desc = None

    if "Test description" in df.columns:
        test_desc_options = sorted(df["Test description"].dropna().unique())
        selected_test_desc = st.sidebar.multiselect("Test description", options=test_desc_options, default=test_desc_options)
    else:
        selected_test_desc = None

    if "Sample Type" in df.columns:
        sample_type_options = sorted(df["Sample Type"].dropna().unique())
        selected_sample_type = st.sidebar.multiselect("Sample Type", options=sample_type_options, default=sample_type_options)
    else:
        selected_sample_type = None

    # Áp dụng các bộ lọc cho dữ liệu
    filtered_df = df.copy()
    if selected_cat is not None:
        filtered_df = filtered_df[filtered_df["Category description"].isin(selected_cat)]
    if selected_spec_cat is not None:
        filtered_df = filtered_df[filtered_df["Spec category"].isin(selected_spec_cat)]
    if selected_spec_desc is not None:
        filtered_df = filtered_df[filtered_df["Spec description"].isin(selected_spec_desc)]
    if selected_test_desc is not None:
        filtered_df = filtered_df[filtered_df["Test description"].isin(selected_test_desc)]
    if selected_sample_type is not None:
        filtered_df = filtered_df[filtered_df["Sample Type"].isin(selected_sample_type)]

    # ---------------------- DATE FILTER ---------------------- #
    st.sidebar.header("Date Filter")
    # Cho phép người dùng chọn cột ngày dùng để lọc: warehouse_date hoặc supplier_date
    date_field = st.sidebar.radio("Select Date Field", ("warehouse_date", "supplier_date"))

    # Lấy khoảng ngày có trong dữ liệu đã lọc (loại bỏ giá trị NA)
    available_dates = pd.to_datetime(filtered_df[date_field].dropna(), errors="coerce")
    if not available_dates.empty:
        min_date = available_dates.min().date()
        max_date = available_dates.max().date()
        selected_date_range = st.sidebar.date_input("Select Date Range", value=[min_date, max_date])
        if len(selected_date_range) == 2:
            start_date, end_date = selected_date_range
            # Lọc dữ liệu theo khoảng ngày dựa trên date_field đã chọn
            filtered_df = filtered_df[
                (pd.to_datetime(filtered_df[date_field], errors="coerce").dt.date >= start_date) &
                (pd.to_datetime(filtered_df[date_field], errors="coerce").dt.date <= end_date)
            ]
    else:
        st.sidebar.write("No available dates in the selected date field.")

    # ---------------------- OTHER OPTIONS ---------------------- #
    # Checkbox cho phép nhóm dữ liệu theo Supplier (áp dụng cho Sample Type RM, PG)
    group_by_supplier = st.sidebar.checkbox("Group by Supplier", value=False)
    
    # Lựa chọn Test Description để vẽ biểu đồ
    test_options = sorted(filtered_df["Test description"].dropna().unique())
    selected_test = st.sidebar.selectbox("Select Test for Chart", options=test_options)
    
    # Lọc dữ liệu cho Test được chọn
    test_data = filtered_df[filtered_df["Test description"] == selected_test].copy()

    # ---------------------- MAIN CONTENT ---------------------- #
    st.title("Quality Control Dashboard")
    st.write(f"Total records after filtering: {len(filtered_df)}")
    
    if not test_data.empty:
        fig = go.Figure()

        # Lấy giá trị Lower limit và Upper limit nếu có
        if "Lower limit" in test_data.columns and "Upper limit" in test_data.columns:
            lsl = test_data["Lower limit"].iloc[0]
            usl = test_data["Upper limit"].iloc[0]
        else:
            lsl = usl = None

        # Vẽ biểu đồ: nếu chọn Group by Supplier thì vẽ từng trace theo supplier_name
        if group_by_supplier:
            suppliers = test_data["supplier_name"].dropna().unique()
            for sup in suppliers:
                sup_data = test_data[test_data["supplier_name"] == sup]
                fig.add_trace(go.Scatter(
                    x=sup_data[date_field],
                    y=sup_data["Actual result"],
                    mode="lines+markers",
                    name=f"Supplier {sup}"
                ))
        else:
            # Vẽ tất cả dữ liệu vào 1 trace
            fig.add_trace(go.Scatter(
                x=test_data[date_field],
                y=test_data["Actual result"],
                mode="lines+markers",
                name="Actual Result"
            ))

        # Thêm đường giới hạn nếu có
        if lsl is not None and pd.notnull(lsl):
            fig.add_hline(y=lsl, line_dash="dash", line_color="red", annotation_text="LSL")
        if usl is not None and pd.notnull(usl):
            fig.add_hline(y=usl, line_dash="dash", line_color="red", annotation_text="USL")

        # Tính CP, CPK nếu có giới hạn (CP = (USL - LSL) / (6 * sigma); CPK = min((USL - mean), (mean - LSL))/(3 * sigma))
        if lsl is not None and usl is not None and pd.notnull(lsl) and pd.notnull(usl):
            mean_val = test_data["Actual result"].mean()
            sigma = test_data["Actual result"].std()
            if sigma > 0:
                cp = (usl - lsl) / (6 * sigma)
                cpk = min((usl - mean_val), (mean_val - lsl)) / (3 * sigma)
            else:
                cp = cpk = None
        else:
            cp = cpk = None

        fig.update_layout(
            title=f"{selected_test} Results Over Time",
            xaxis_title="Date",
            yaxis_title="Result",
            showlegend=True
        )

        st.plotly_chart(fig, use_container_width=True)

        # Hiển thị CP, CPK nếu có
        if cp is not None and cpk is not None:
            st.write(f"CP: {cp:.2f}, CPK: {cpk:.2f}")
    else:
        st.error("No data available for the selected test after filtering.")
