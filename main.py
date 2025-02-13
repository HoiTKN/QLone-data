import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from data_processing import prepare_data

# Cấu hình trang
st.set_page_config(page_title="Quality Control Dashboard", layout="wide")

# Tải dữ liệu, cache 1 giờ
@st.cache_data(ttl=3600)
def load_data():
    return prepare_data()

df = load_data()
if df is None:
    st.error("Unable to load data. Please check your configuration.")
    st.stop()

# ---------------------------------------------------------------------
# HÀM TIỆN ÍCH
# ---------------------------------------------------------------------
def apply_multiselect_filter(df, col_name, label):
    """
    Trả về df đã được lọc dựa trên các giá trị người dùng chọn trong multiselect.
    Nếu người dùng không chọn gì -> không lọc.
    """
    if col_name not in df.columns:
        return df  # cột không tồn tại, bỏ qua

    all_vals = sorted(df[col_name].dropna().unique())
    selected_vals = st.sidebar.multiselect(label, options=all_vals)
    if selected_vals:  # nếu user chọn ít nhất 1
        return df[df[col_name].isin(selected_vals)]
    else:
        return df  # user chưa chọn gì -> giữ nguyên

def numeric_or_none(val):
    """Chuyển val về số (float) nếu được, ngược lại None."""
    try:
        return float(val)
    except:
        return None

# ---------------------------------------------------------------------
# PHẦN FILTERS TRONG SIDEBAR
# ---------------------------------------------------------------------
st.sidebar.title("Filters")

filtered_df = df.copy()

# 1) Category description
filtered_df = apply_multiselect_filter(filtered_df, "Category description", "Category description")

# 2) Spec category
filtered_df = apply_multiselect_filter(filtered_df, "Spec category", "Spec category")

# 3) Spec description
filtered_df = apply_multiselect_filter(filtered_df, "Spec description", "Spec description")

# 4) Test description
filtered_df = apply_multiselect_filter(filtered_df, "Test description", "Test description")

# 5) Sample Type
filtered_df = apply_multiselect_filter(filtered_df, "Sample Type", "Sample Type")

st.sidebar.markdown(f"**Total records after filtering**: {len(filtered_df)}")

# ---------------------------------------------------------------------
# XÁC ĐỊNH CÁCH CHỌN CỘT NGÀY
# ---------------------------------------------------------------------
# Kiểm tra Sample Type sau khi lọc
unique_stypes = filtered_df["Sample Type"].dropna().unique()
rm_pg_set = {"RM - Raw material", "PG - Packaging"}

# Nếu toàn bộ sample type đều là RM/PG -> cho user chọn cột ngày
# Nếu có mix (RM/PG + IP/FG/khác) hoặc toàn IP/FG -> dùng supplier_date
if len(unique_stypes) == 0:
    # Không có sample type -> tạm mặc định supplier_date
    date_field_mode = "supplier_date"
elif set(unique_stypes).issubset(rm_pg_set):
    # Tất cả đều là RM hoặc PG
    st.sidebar.markdown("---")
    st.sidebar.subheader("Date Field for RM/PG")
    date_choice = st.sidebar.radio(
        "Chọn loại ngày để phân tích",
        ("Ngày nhập kho (warehouse_date)", "NSX của NCC (supplier_date)"),
    )
    date_field_mode = "warehouse_date" if "nhập kho" in date_choice.lower() else "supplier_date"
else:
    # Trường hợp mix hoặc IP/FG => chỉ có 1 date => supplier_date
    st.sidebar.info("Mixed or non-RM/PG sample types detected. Using supplier_date.")
    date_field_mode = "supplier_date"

# ---------------------------------------------------------------------
# TẠO CÁC TAB HIỂN THỊ
# ---------------------------------------------------------------------
tabs = st.tabs(["Time Series", "SPC Chart", "Boxplot", "Distribution", "Pareto Chart"])

# ======================== TAB 1: TIME SERIES =========================
with tabs[0]:
    st.header("Time Series Chart")
    # Chọn 1 test để vẽ
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
            # Người dùng có thể muốn group theo supplier_name hay không
            group_supplier_ts = st.checkbox("Group by Supplier (RM/PG)", value=False)

            if group_supplier_ts:
                # Vẽ từng supplier_name
                for sup in ts_data["supplier_name"].dropna().unique():
                    sup_data = ts_data[ts_data["supplier_name"] == sup]
                    fig_ts.add_trace(go.Scatter(
                        x=sup_data[date_field_mode],
                        y=sup_data["Actual result"],
                        mode="lines+markers",
                        name=f"Supplier {sup}"
                    ))
            else:
                fig_ts.add_trace(go.Scatter(
                    x=ts_data[date_field_mode],
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
                x=spc_data[date_field_mode],
                y=spc_data["Actual result"],
                mode="lines+markers",
                name="Actual Result"
            ))
            # Thêm LSL/USL nếu có
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
            st.plotly_chart(fig_spc, use_container_width=True)

            # Tính CP/CPK nếu có đủ giới hạn
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

    # Chỉ lấy RM, PG
    box_data = filtered_df[filtered_df["Sample Type"].isin(["RM - Raw material", "PG - Packaging"])].copy()
    if box_data.empty:
        st.warning("No RM/PG data available.")
    else:
        # Rút gọn supplier_name còn 3 ký tự đầu (nếu không rỗng)
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
    # Chọn nhóm hiển thị: Test description hoặc Spec description
    pareto_group_option = st.selectbox("Group by:", ["Test description", "Spec description"])

    temp_df = filtered_df.copy()
    if "Lower limit" in temp_df.columns and "Upper limit" in temp_df.columns:
        temp_df["LSL"] = temp_df["Lower limit"].apply(numeric_or_none)
        temp_df["USL"] = temp_df["Upper limit"].apply(numeric_or_none)
        temp_df["Act"] = temp_df["Actual result"].apply(numeric_or_none)
        temp_df["OutOfSpec"] = temp_df.apply(
            lambda r: (r["Act"] is not None) and (r["LSL"] is not None) and (r["USL"] is not None)
                      and (r["Act"] < r["LSL"] or r["Act"] > r["USL"]),
            axis=1
        )
        pareto_df = temp_df[temp_df["OutOfSpec"] == True].copy()
        if pareto_df.empty:
            st.info("No out-of-spec samples found.")
        else:
            # Tính Pareto
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
                yaxis2=dict(title="Cumulative %", overlaying="y", side="right", range=[0, 110])
            )
            st.plotly_chart(fig_pareto, use_container_width=True)
    else:
        st.warning("Missing 'Lower limit' or 'Upper limit' columns. Cannot compute out-of-spec.")

