import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from data_processing import prepare_data
from scipy.stats import norm  # Đảm bảo module scipy được import

# Đặt cấu hình trang
st.set_page_config(page_title="Quality Control Dashboard", layout="wide")

@st.cache_data(ttl=3600)
def load_data():
    """
    Gọi prepare_data() để lấy 2 DataFrame:
      - df: dữ liệu sạch (đã loại bỏ outliers)
      - df_outliers: dữ liệu outliers
    """
    return prepare_data()

# Tải dữ liệu
df, df_outliers = load_data()

if df is None or df.empty:
    st.error("Unable to load data. Please check your configuration.")
    st.stop()

# Hiển thị thông tin dữ liệu trong sidebar (dưới cùng)
with st.sidebar.expander("Thông tin dữ liệu", expanded=False):
    st.info("Data loaded successfully from BigQuery!")
    st.caption(f"Min final_date: {df['final_date'].min()}")
    st.caption(f"Max final_date: {df['final_date'].max()}")

###################### HÀM HỖ TRỢ FILTER (Sidebar) ######################
def sidebar_multiselect_filter(df, col_name, label):
    """
    Tạo filter multiselect trên sidebar.
    Nếu không chọn gì thì không lọc.
    """
    if df is None or df.empty or col_name not in df.columns:
        return df
    all_vals = sorted(df[col_name].dropna().unique())
    selected_vals = st.sidebar.multiselect(label, options=all_vals, default=[], help="Chọn giá trị để lọc (để trống = không lọc)")
    if selected_vals:
        return df[df[col_name].isin(selected_vals)]
    else:
        return df

def numeric_or_none(val):
    try:
        return float(val)
    except:
        return None

###################### SIDEBAR FILTERS ######################
st.sidebar.title("Filters")

# Sắp xếp theo thứ tự: Category description, Sample Type, Spec description, Test description.
filtered_df = df.copy()
filtered_outliers = df_outliers.copy() if df_outliers is not None else pd.DataFrame()

for col, label in [("Category description", "Category description"),
                   ("Sample Type", "Sample Type"),
                   ("Spec description", "Spec description"),
                   ("Test description", "Test description")]:
    filtered_df = sidebar_multiselect_filter(filtered_df, col, label)
    if not filtered_outliers.empty:
        filtered_outliers = sidebar_multiselect_filter(filtered_outliers, col, label)

# Lọc theo date range (final_date)
st.sidebar.markdown("---")
st.sidebar.subheader("Date Range Filter (final_date)")
available_dates = pd.to_datetime(filtered_df["final_date"], errors='coerce').dropna()
if not available_dates.empty:
    min_date = available_dates.min().date()
    max_date = available_dates.max().date()
    st.sidebar.write(f"Data range: {min_date} -> {max_date}")
    date_range = st.sidebar.date_input("Select Date Range", value=(min_date, max_date))
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_date, end_date = date_range
        mask = (pd.to_datetime(filtered_df["final_date"], errors='coerce').dt.date >= start_date) & \
               (pd.to_datetime(filtered_df["final_date"], errors='coerce').dt.date <= end_date)
        filtered_df = filtered_df[mask]
        if not filtered_outliers.empty:
            mask_out = (pd.to_datetime(filtered_outliers["final_date"], errors='coerce').dt.date >= start_date) & \
                       (pd.to_datetime(filtered_outliers["final_date"], errors='coerce').dt.date <= end_date)
            filtered_outliers = filtered_outliers[mask_out]
else:
    st.sidebar.write("No valid final_date found.")

st.sidebar.markdown(f"**Total records after filtering**: {len(filtered_df)}")

###################### TABS ######################
tabs = st.tabs(["Time Series", "SPC Chart", "Boxplot", "Distribution (Histogram)", "Pareto Chart"])

# ================= TAB 1: TIME SERIES ================= #
with tabs[0]:
    st.header("Time Series Chart")
    if filtered_df.empty:
        st.warning("No data available for Time Series Chart.")
    else:
        # Sắp xếp theo thời gian
        ts_data = filtered_df.sort_values(by="final_date")
        fig_ts = go.Figure()
        # Vẽ từng trace cho mỗi giá trị của Test description
        tests = ts_data["Test description"].dropna().unique()
        for test in tests:
            sub_df = ts_data[ts_data["Test description"] == test]
            fig_ts.add_trace(go.Scatter(
                x=sub_df["final_date"],
                y=sub_df["Actual result"],
                mode="lines+markers",
                line_shape="spline",
                name=str(test)
            ))
        fig_ts.update_layout(
            title="Time Series Chart",
            xaxis_title="Date",
            yaxis_title="Actual Result",
            template="plotly_white"
        )
        fig_ts.update_xaxes(tickformat='%Y-%m-%d')
        st.plotly_chart(fig_ts, use_container_width=True)
        if not filtered_outliers.empty:
            st.subheader("Outliers (excluded from chart)")
            st.dataframe(filtered_outliers[["final_date", "Actual result", "Lot number"]].sort_values("final_date"))

# ================= TAB 2: SPC CHART ================= #
with tabs[1]:
    st.header("SPC Chart")
    if filtered_df.empty:
        st.warning("No data available for SPC Chart.")
    else:
        spc_data = filtered_df.sort_values(by="final_date")
        fig_spc = go.Figure()
        fig_spc.add_trace(go.Scatter(
            x=spc_data["final_date"],
            y=spc_data["Actual result"],
            mode="lines+markers",
            line_shape="spline",
            name="Actual Result"
        ))
        # Thêm LSL và USL nếu có
        lsl_val = numeric_or_none(spc_data["Lower limit"].iloc[0]) if "Lower limit" in spc_data.columns and not spc_data.empty else None
        usl_val = numeric_or_none(spc_data["Upper limit"].iloc[0]) if "Upper limit" in spc_data.columns and not spc_data.empty else None
        if lsl_val is not None:
            fig_spc.add_hline(y=lsl_val, line_dash="dash", line_color="red", annotation_text="LSL")
        if usl_val is not None:
            fig_spc.add_hline(y=usl_val, line_dash="dash", line_color="red", annotation_text="USL")
        fig_spc.update_layout(
            title="SPC Chart",
            xaxis_title="Date",
            yaxis_title="Actual Result",
            template="plotly_white"
        )
        fig_spc.update_xaxes(tickformat='%Y-%m-%d')
        st.plotly_chart(fig_spc, use_container_width=True)

# ================= TAB 3: BOXPLOT ================= #
with tabs[2]:
    st.header("Boxplot")
    # Boxplot cho RM/PG và non-RM/PG như cũ
    rm_pg_data = filtered_df[filtered_df["Sample Type"].isin(["RM - Raw material", "PG - Packaging"])].copy()
    non_rm_pg_data = filtered_df[~filtered_df["Sample Type"].isin(["RM - Raw material", "PG - Packaging"])].copy()

    with st.expander("Boxplot for RM/PG by Supplier", expanded=False):
        if rm_pg_data.empty:
            st.info("No RM/PG data available for this filter.")
        else:
            rm_pg_data["SupplierShort"] = rm_pg_data["supplier_name"].dropna().apply(lambda x: x[:3] if isinstance(x, str) else x)
            fig_box_rm = px.box(
                rm_pg_data,
                x="SupplierShort",
                y="Actual result",
                points="all",
                title="RM/PG - Boxplot by Supplier",
                template="plotly_white"
            )
            st.plotly_chart(fig_box_rm, use_container_width=True)

    with st.expander("Boxplot for Non-RM/PG (IP, FG...) by Month", expanded=False):
        if non_rm_pg_data.empty:
            st.info("No non-RM/PG data available for this filter.")
        else:
            non_rm_pg_data["Month"] = pd.to_datetime(non_rm_pg_data["final_date"], errors='coerce').dt.to_period("M").astype(str)
            fig_box_non = px.box(
                non_rm_pg_data,
                x="Month",
                y="Actual result",
                points="all",
                title="Non-RM/PG - Boxplot by Month",
                template="plotly_white"
            )
            st.plotly_chart(fig_box_non, use_container_width=True)

    with st.expander("Boxplot All Data (MBP) by Month", expanded=False):
        # Tạo nhóm MBP Overall và theo tháng
        all_mbp_data = filtered_df.copy()
        all_mbp_data = all_mbp_data.assign(Plant="MBP")
        all_mbp_data["Month"] = pd.to_datetime(all_mbp_data["final_date"], errors='coerce').dt.to_period("M").astype(str)
        mbp_overall = all_mbp_data.copy()
        mbp_overall["Group"] = "MBP Overall"
        mbp_monthly = all_mbp_data.copy()
        mbp_monthly["Group"] = mbp_monthly["Month"]
        mbp_combined = pd.concat([mbp_overall, mbp_monthly])
        fig_box_mbp = px.box(
            mbp_combined,
            x="Group",
            y="Actual result",
            points="all",
            title="All Data (MBP): Overall and by Month",
            template="plotly_white"
        )
        st.plotly_chart(fig_box_mbp, use_container_width=True)

# ================= TAB 4: DISTRIBUTION (Histogram & CP/CPK) ================= #
with tabs[3]:
    st.header("Distribution Histogram with CP/CPK")
    if filtered_df.empty:
        st.warning("No data to display distribution.")
    else:
        actual = pd.to_numeric(filtered_df["Actual result"], errors='coerce').dropna()
        mean_val = actual.mean()
        std_val = actual.std()
        lsl_val = numeric_or_none(filtered_df["Lower limit"].iloc[0]) if "Lower limit" in filtered_df.columns and not filtered_df.empty else None
        usl_val = numeric_or_none(filtered_df["Upper limit"].iloc[0]) if "Upper limit" in filtered_df.columns and not filtered_df.empty else None
        if std_val > 0 and lsl_val is not None and usl_val is not None:
            cp = (usl_val - lsl_val) / (6 * std_val)
            cpk = min((usl_val - mean_val), (mean_val - lsl_val)) / (3 * std_val)
            cp_text = f"CP: {cp:.2f}, CPK: {cpk:.2f}"
        else:
            cp_text = "Không thể tính CP/CPK do std = 0 hoặc thiếu giới hạn."

        fig_hist = px.histogram(
            filtered_df,
            x="Actual result",
            nbins=30,
            histnorm='density',
            template="plotly_white",
            title="Distribution of Actual Results"
        )
        x_vals = np.linspace(actual.min(), actual.max(), 100)
        y_vals = norm.pdf(x_vals, mean_val, std_val)
        fig_hist.add_trace(go.Scatter(x=x_vals, y=y_vals, mode='lines', name="Normal Curve"))
        fig_hist.update_layout(
            annotations=[dict(text=cp_text, xref="paper", yref="paper", x=1, y=0, showarrow=False, font=dict(size=12))]
        )
        st.plotly_chart(fig_hist, use_container_width=True)

# ================= TAB 5: PARETO CHART ================= #
with tabs[4]:
    st.header("Pareto Chart (Out-of-Spec)")
    pareto_group_option = st.selectbox("Group by:", ["Test description", "Spec description"], key="pareto")
    temp_df = filtered_df.copy()
    if "Lower limit" in temp_df.columns and "Upper limit" in temp_df.columns:
        temp_df["LSL"] = temp_df["Lower limit"].apply(numeric_or_none)
        temp_df["USL"] = temp_df["Upper limit"].apply(numeric_or_none)
        temp_df["Act"] = temp_df["Actual result"].apply(numeric_or_none)
        temp_df["OutOfSpec"] = temp_df.apply(
            lambda r: (r["Act"] is not None) and (r["LSL"] is not None) and (r["USL"] is not None) and (r["Act"] < r["LSL"] or r["Act"] > r["USL"]),
            axis=1
        )
        pareto_df = temp_df[temp_df["OutOfSpec"] == True].copy()
        if pareto_df.empty:
            st.info("No out-of-spec samples found.")
        else:
            grp_counts = pareto_df.groupby(pareto_group_option).size().reset_index(name="Count")
            grp_counts = grp_counts.sort_values("Count", ascending=False)
            grp_counts["Cumulative"] = grp_counts["Count"].cumsum()
            grp_counts["Cumulative %"] = 100 * grp_counts["Cumulative"] / grp_counts["Count"].sum()
            fig_pareto = go.Figure()
            fig_pareto.add_trace(go.Bar(
                x=grp_counts[pareto_group_option],
                y=grp_counts["Count"],
                name="Count"
            ))
            fig_pareto.add_trace(go.Scatter(
                x=grp_counts[pareto_group_option],
                y=grp_counts["Cumulative %"],
                name="Cumulative %",
                yaxis="y2",
                mode="lines+markers"
            ))
            fig_pareto.update_layout(
                title="Pareto Chart - Out-of-Spec",
                xaxis_title=pareto_group_option,
                yaxis=dict(title="Count"),
                yaxis2=dict(title="Cumulative %", overlaying="y", side="right", range=[0, 110]),
                template="plotly_white"
            )
            st.plotly_chart(fig_pareto, use_container_width=True)
    else:
        st.warning("Missing 'Lower limit' or 'Upper limit' columns. Cannot compute out-of-spec.")

###################### ĐỀ XUẤT BIỂU ĐỒ MỚI ######################
st.markdown("---")
st.subheader("Đề xuất biểu đồ thêm cho báo cáo chất lượng")
st.markdown("""
- **CUSUM Chart:** Theo dõi thay đổi nhỏ trong trung bình quá trình qua thời gian.
- **Control Chart (X-bar & R Chart):** Giám sát độ ổn định của quy trình sản xuất.
- **Scatter Plot:** So sánh kết quả kiểm tra với giới hạn (LSL/USL) để nhận diện mối tương quan.
""")
