import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from data_processing import prepare_data

# Cấu hình trang cho Streamlit
st.set_page_config(page_title="Quality Control Dashboard", layout="wide")

# Hàm load dữ liệu (cache 1 giờ)
@st.cache_data(ttl=3600)
def load_data():
    return prepare_data()

df = load_data()

if df is None:
    st.error("Unable to load data. Please check your configuration.")
else:
    # ==================== SIDEBAR FILTERS ==================== #
    st.sidebar.header("Filter Options")
    
    # Sử dụng expander cho từng nhóm filter giúp giao diện gọn gàng hơn
    with st.sidebar.expander("Category description"):
        if "Category description" in df.columns:
            cat_options = sorted(df["Category description"].dropna().unique())
            selected_cat = st.multiselect("Select Category", options=cat_options, default=cat_options)
        else:
            selected_cat = None

    with st.sidebar.expander("Spec category"):
        if "Spec category" in df.columns:
            spec_cat_options = sorted(df["Spec category"].dropna().unique())
            selected_spec_cat = st.multiselect("Select Spec Category", options=spec_cat_options, default=spec_cat_options)
        else:
            selected_spec_cat = None

    with st.sidebar.expander("Spec description"):
        if "Spec description" in df.columns:
            spec_desc_options = sorted(df["Spec description"].dropna().unique())
            selected_spec_desc = st.multiselect("Select Spec Description", options=spec_desc_options, default=spec_desc_options)
        else:
            selected_spec_desc = None

    with st.sidebar.expander("Test description"):
        if "Test description" in df.columns:
            test_desc_options = sorted(df["Test description"].dropna().unique())
            selected_test_desc = st.multiselect("Select Test Description", options=test_desc_options, default=test_desc_options)
        else:
            selected_test_desc = None

    with st.sidebar.expander("Sample Type"):
        if "Sample Type" in df.columns:
            sample_type_options = sorted(df["Sample Type"].dropna().unique())
            selected_sample_type = st.multiselect("Select Sample Type", options=sample_type_options, default=sample_type_options)
        else:
            selected_sample_type = None

    # Áp dụng các filter lên dataframe
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
    
    # ==================== DATE FILTER ==================== #
    st.sidebar.header("Date Filter")
    # Cho phép người dùng chọn cột ngày dùng để lọc (warehouse_date hoặc supplier_date)
    date_field = st.sidebar.radio("Select Date Field", ("warehouse_date", "supplier_date"))

    # Sử dụng slider cho việc chọn khoảng ngày (thao tác kéo chuột)
    available_dates = pd.to_datetime(filtered_df[date_field].dropna(), errors="coerce")
    if not available_dates.empty:
        min_date = available_dates.min().date()
        max_date = available_dates.max().date()
        selected_date_range = st.sidebar.slider(
            "Select Date Range",
            min_value=min_date,
            max_value=max_date,
            value=(min_date, max_date),
            format="YYYY-MM-DD"
        )
        start_date, end_date = selected_date_range
        filtered_df = filtered_df[
            (pd.to_datetime(filtered_df[date_field], errors="coerce").dt.date >= start_date) &
            (pd.to_datetime(filtered_df[date_field], errors="coerce").dt.date <= end_date)
        ]
    else:
        st.sidebar.write("No available dates in the selected date field.")
    
    # ==================== OTHER OPTIONS ==================== #
    # Cho phép nhóm dữ liệu theo Supplier (áp dụng cho Sample Type RM, PG)
    group_by_supplier = st.sidebar.checkbox("Group by Supplier", value=False)
    
    with st.sidebar.expander("Chart Options"):
        if "Test description" in filtered_df.columns:
            test_options = sorted(filtered_df["Test description"].dropna().unique())
            selected_test = st.selectbox("Select Test for Chart", options=test_options)
        else:
            selected_test = None

    # ==================== MAIN CONTENT ==================== #
    st.title("Quality Control Dashboard")
    st.write(f"Total records after filtering: {len(filtered_df)}")
    
    if selected_test is None:
        st.error("No Test selected for Chart")
    else:
        # Lọc dữ liệu cho Test được chọn
        test_data = filtered_df[filtered_df["Test description"] == selected_test].copy()
        if test_data.empty:
            st.error("No data available for the selected test after filtering.")
        else:
            fig = go.Figure()

            # Xử lý Lower limit và Upper limit: ép sang số để tránh lỗi khi tính toán
            lsl = None
            usl = None
            if "Lower limit" in test_data.columns and "Upper limit" in test_data.columns:
                lsl = pd.to_numeric(test_data["Lower limit"].iloc[0], errors="coerce")
                usl = pd.to_numeric(test_data["Upper limit"].iloc[0], errors="coerce")
            
            # Vẽ biểu đồ: nếu chọn nhóm theo Supplier thì vẽ từng trace theo supplier_name
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
            
            # Thêm đường giới hạn nếu có (chỉ khi giá trị là số)
            if lsl is not None and pd.notnull(lsl):
                fig.add_hline(y=lsl, line_dash="dash", line_color="red", annotation_text="LSL")
            if usl is not None and pd.notnull(usl):
                fig.add_hline(y=usl, line_dash="dash", line_color="red", annotation_text="USL")
            
            fig.update_layout(
                title=f"{selected_test} Results Over Time",
                xaxis_title="Date",
                yaxis_title="Result",
                showlegend=True
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Tính CP, CPK nếu có giới hạn (và sigma > 0)
            if lsl is not None and usl is not None and pd.notnull(lsl) and pd.notnull(usl):
                mean_val = test_data["Actual result"].mean()
                sigma = test_data["Actual result"].std()
                if sigma > 0:
                    cp = (usl - lsl) / (6 * sigma)
                    cpk = min((usl - mean_val), (mean_val - lsl)) / (3 * sigma)
                    st.write(f"CP: {cp:.2f}, CPK: {cpk:.2f}")
                else:
                    st.write("Standard deviation is zero, cannot calculate CP and CPK.")
