import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from data_processing import prepare_data

# --- (Nếu cần, xóa cache) ---
# st.cache_data.clear()

# Cấu hình trang
st.set_page_config(page_title="Quality Control Dashboard", layout="wide")

# Hàm load dữ liệu (cache 1 giờ)
@st.cache_data(ttl=3600)
def load_data():
    return prepare_data()

df = load_data()
if df is None:
    st.error("Unable to load data. Please check your configuration.")
    st.stop()

# Debug: In ra danh sách các cột để kiểm tra cột final_date có tồn tại không
st.write("Columns in DataFrame:", list(df.columns))

if "final_date" not in df.columns:
    st.error("Column 'final_date' does not exist. Vui lòng xóa cache hoặc kiểm tra lại data_processing.py!")
    st.stop()

# -------------------------- HÀM TIỆN ÍCH -------------------------- #
def apply_multiselect_filter(df, col_name, label):
    """
    Lọc dữ liệu theo các giá trị người dùng chọn.
    Nếu không chọn gì, trả về dữ liệu gốc.
    """
    if col_name not in df.columns:
        return df

    all_vals = sorted(df[col_name].dropna().unique())
    selected_vals = st.sidebar.multiselect(label, options=all_vals)
    if selected_vals:
        return df[df[col_name].isin(selected_vals)]
    else:
        return df

def numeric_or_none(val):
    try:
        return float(val)
    except:
        return None

# -------------------------- SIDEBAR FILTERS ------------------------ #
st.sidebar.title("Filters")

filtered_df = df.copy()

# Lọc theo các trường: Category description, Spec category, Spec description, Test description, Sample Type
filtered_df = apply_multiselect_filter(filtered_df, "Category description", "Category description")
filtered_df = apply_multiselect_filter(filtered_df, "Spec category", "Spec category")
filtered_df = apply_multiselect_filter(filtered_df, "Spec description", "Spec description")
filtered_df = apply_multiselect_filter(filtered_df, "Test description", "Test description")
filtered_df = apply_multiselect_filter(filtered_df, "Sample Type", "Sample Type")

st.sidebar.markdown(f"**Total records after filtering**: {len(filtered_df)}")

# --------------------- CHỌN KHOẢNG THỜI GIAN (dựa trên final_date) ---------------------- #
st.sidebar.markdown("---")
st.sidebar.subheader("Date Range Filter (Based on final_date)")

available_dates = pd.to_datetime(filtered_df["final_date"], errors='coerce').dropna()
if not available_dates.empty:
    min_date = available_dates.min().date()
    max_date = available_dates.max().date()
    date_range = st.sidebar.date_input("Select Date Range", value=(min_date, max_date))
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_date, end_date = date_range
        mask = (pd.to_datetime(filtered_df["final_date"], errors='coerce').dt.date >= start_date) & \
               (pd.to_datetime(filtered_df["final_date"], errors='coerce').dt.date <= end_date)
        filtered_df = filtered_df[mask]
else:
    st.sidebar.write("No valid final_date in the current filtered data.")

st.sidebar.markdown(f"**Total records after date filter**: {len(filtered_df)}")

# --------------------- TẠO CÁC TAB HIỂN THỊ ----------------------- #
tabs = st.tabs(["Time Series", "SPC Chart", "Boxplot", "Distribution", "Pareto Chart"])

# ======================== TAB 1: TIME SERIES =========================
with tabs[0]:
    st.header("Time Series Chart")
    tests_ts = sorted(filtered_df["Test description"].dropna().unique())
    if not tests_ts:
        st.warning("No 'Test description' available in the filtered data.")
    else:
        selected_test_ts = st.selectbox("Select Test for Time Series", options=tests_ts)
        ts_data = filtered_df[filtered_df["Test description"] == selected_test_ts].copy()

        if ts_data.empty:
            st.warning("No data for the selected test.")
        else:
            fig_ts = go.Figure()
            group_supplier_ts = st.checkbox("Group by Supplier (RM/PG)", value=False)
            if group_supplier_ts:
                for sup in ts_data["supplier_name"].dropna().unique():
                    sup_data = ts_data[ts_data["supplier_name"] == sup]
                    fig_ts.add_trace(go.Scatter(
                        x=sup_data["final_date"],
                        y=sup_data["Actual result"],
                        mode="lines+markers",
                        name=f"Supplier {sup}"
                    ))
            else:
                fig_ts.add_trace(go.Scatter(
                    x=ts_data["final_date"],
                    y=ts_data["Actual result"],
                    mode="lines+markers",
                    name="Actual Result"
                ))
            fig_ts.update_layout(
                title=f"Time Series - {selected_test_ts}",
                xaxis_title="Date",
                yaxis_title="Actual Result",
                showlegend=True
            )
            fig_ts.update_xaxes(type='date')
            st.plotly_chart(fig_ts, use_container_width=True)

# ======================== TAB 2: SPC CHART ===========================
with tabs[1]:
    st.header("SPC Chart")
    tests_spc = sorted(filtered_df["Test description"].dropna().unique())
    if not tests_spc:
        st.warning("No 'Test description' available in the filtered data.")
    else:
        selected_test_spc = st.selectbox("Select Test for SPC", options=tests_spc)
        spc_data = filtered_df[filtered_df["Test description"] == selected_test_spc].copy()

        if spc_data.empty:
            st.warning("No data for the selected test.")
        else:
            fig_spc = go.Figure()
            fig_spc.add_trace(go.Scatter(
                x=spc_data["final_date"],
                y=spc_data["Actual result"],
                mode="lines+markers",
                name="Actual Result"
            ))
            lsl_val = numeric_or_none(spc_data["Lower limit"].iloc[0]) if "Lower limit" in spc_data.columns else None
            usl_val = numeric_or_none(spc_data["Upper limit"].iloc[0]) if "Upper limit" in spc_data.columns else None
            if lsl_val is not None:
                fig_spc.add_hline(y=lsl_val, line_dash="dash", line_color="red", annotation_text="LSL")
            if usl_val is not None:
                fig_spc.add_hline(y=usl_val, line_dash="dash", line_color="red", annotation_text="USL")
            fig_spc.update_layout(
                title=f"SPC Chart - {selected_test_spc}",
                xaxis_title="Date",
                yaxis_title="Actual Result",
                showlegend=True
            )
            fig_spc.update_xaxes(type='date')
            st.plotly_chart(fig_spc, use_container_width=True)
            if (lsl_val is not None) and (usl_val is not None):
                mean_val = spc_data["Actual result"].mean()
                std_val = spc_data["Actual result"].std()
                if std_val and std_val > 0:
                    cp = (usl_val - lsl_val) / (6 * std_val)
                    cpk = min((usl_val - mean_val), (mean_val - lsl_val)) / (3 * std_val)
                    st.write(f"**CP**: {cp:.2f}, **CPK**: {cpk:.2f}")
                else:
                    st.write("Standard Deviation is zero, cannot compute CP/CPK.")

# ======================== TAB 3: BOXPLOT ============================
with tabs[2]:
    st.header("Boxplot (RM/PG) by Supplier")
    box_data = filtered_df[filtered_df["Sample Type"].isin(["RM - Raw material", "PG - Packaging"])].copy()
    if box_data.empty:
        st.warning("No RM/PG data available.")
    else:
        box_data["SupplierShort"] = box_data["supplier_name"].dropna().apply(lambda x: x[:3] if isinstance(x, str) else x)
        fig_box = px.box(
            box_data,
            x="SupplierShort",
            y="Actual result",
            points="all",
            title="Boxplot of Actual Results by Supplier (Short Name)"
        )
        st.plotly_chart(fig_box, use_container_width=True)

# ======================== TAB 4: DISTRIBUTION =======================
with tabs[3]:
    st.header("Distribution Chart of Actual Result")
    if filtered_df.empty:
        st.warning("No data to display distribution.")
    else:
        fig_dist = px.histogram(
            filtered_df,
            x="Actual result",
            nbins=30,
            marginal="box",
            title="Distribution of Actual Results"
        )
        st.plotly_chart(fig_dist, use_container_width=True)

# ======================== TAB 5: PARETO ============================
with tabs[4]:
    st.header("Pareto Chart (Out-of-Spec)")
    pareto_group_option = st.selectbox("Group by:", ["Test description", "Spec description"])
    temp_df = filtered_df.copy()
    if "Lower limit" in temp_df.columns and "Upper limit" in temp_df.columns:
        temp_df["LSL"] = temp_df["Lower limit"].apply(numeric_or_none)
        temp_df["USL"] = temp_df["Upper limit"].apply(numeric_or_none)
        temp_df["Act"] = temp_df["Actual result"].apply(numeric_or_none)
        temp_df["OutOfSpec"] = temp_df.apply(
            lambda r: (r["Act"] is not None) and (r["LSL"] is not None) and (r["USL"] is
