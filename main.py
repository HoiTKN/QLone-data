import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from data_processing import prepare_data
st.set_page_config(page_title="Quality Control Dashboard", layout="wide")

@st.cache_data(ttl=3600)
def load_data():
    return prepare_data()

df = load_data()
if df is None:
    st.error("Unable to load data. Please check your configuration.")
    st.stop()

# Đảm bảo cột final_date tồn tại
if "final_date" not in df.columns:
    st.error("Column 'final_date' does not exist. Check data_processing.py!")
    st.stop()

def apply_multiselect_filter(df, col_name, label):
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

# ------------------ SIDEBAR FILTERS ------------------ #
st.sidebar.title("Filters")

filtered_df = df.copy()
filtered_df = apply_multiselect_filter(filtered_df, "Category description", "Category description")
filtered_df = apply_multiselect_filter(filtered_df, "Spec category", "Spec category")
filtered_df = apply_multiselect_filter(filtered_df, "Spec description", "Spec description")
filtered_df = apply_multiselect_filter(filtered_df, "Test description", "Test description")
filtered_df = apply_multiselect_filter(filtered_df, "Sample Type", "Sample Type")

st.sidebar.markdown(f"**Total records after filtering**: {len(filtered_df)}")

# -------------- DATE RANGE FILTER (final_date) -------------- #
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

# ------------------ TABS ------------------ #
tabs = st.tabs(["Time Series", "SPC Chart", "Boxplot", "Distribution", "Pareto Chart"])

# ================= TAB 1: TIME SERIES ================= #
with tabs[0]:
    st.header("Time Series Chart")

    tests_ts = sorted(filtered_df["Test description"].dropna().unique())
    if not tests_ts:
        st.warning("No 'Test description' available.")
    else:
        selected_test_ts = st.selectbox("Select Test for Time Series", options=tests_ts)
        ts_data = filtered_df[filtered_df["Test description"] == selected_test_ts].copy()

        if ts_data.empty:
            st.warning("No data for the selected test.")
        else:
            # Sắp xếp theo final_date
            ts_data.sort_values(by="final_date", inplace=True)

            fig_ts = go.Figure()
            group_supplier_ts = st.checkbox("Group by Supplier (RM/PG)", value=False)

            if group_supplier_ts:
                suppliers = ts_data["supplier_name"].dropna().unique()
                for sup in suppliers:
                    sup_data = ts_data[ts_data["supplier_name"] == sup]
                    fig_ts.add_trace(go.Scatter(
                        x=sup_data["final_date"],
                        y=sup_data["Actual result"],
                        mode="lines+markers",
                        line_shape="spline",
                        connectgaps=False,
                        name=f"Supplier {sup}"
                    ))
            else:
                fig_ts.add_trace(go.Scatter(
                    x=ts_data["final_date"],
                    y=ts_data["Actual result"],
                    mode="lines+markers",
                    line_shape="spline",
                    connectgaps=False,
                    name="Actual Result"
                ))

            fig_ts.update_layout(
                title=f"Time Series - {selected_test_ts}",
                xaxis_title="Date",
                yaxis_title="Actual Result",
                showlegend=True
            )
            # Chỉ lấy ngày (không giờ)
            fig_ts.update_xaxes(
                type='date',
                tickformat='%Y-%m-%d'   # hoặc '%d-%m-%Y' nếu muốn ngày-tháng-năm
            )
            st.plotly_chart(fig_ts, use_container_width=True)

# ================= TAB 2: SPC CHART ================= #
with tabs[1]:
    st.header("SPC Chart")

    tests_spc = sorted(filtered_df["Test description"].dropna().unique())
    if not tests_spc:
        st.warning("No 'Test description' available.")
    else:
        selected_test_spc = st.selectbox("Select Test for SPC", options=tests_spc)
        spc_data = filtered_df[filtered_df["Test description"] == selected_test_spc].copy()

        if spc_data.empty:
            st.warning("No data for the selected test.")
        else:
            # Sắp xếp theo final_date
            spc_data.sort_values(by="final_date", inplace=True)

            fig_spc = go.Figure()
            fig_spc.add_trace(go.Scatter(
                x=spc_data["final_date"],
                y=spc_data["Actual result"],
                mode="lines+markers",
                line_shape="spline",
                connectgaps=False,
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
            fig_spc.update_xaxes(
                type='date',
                tickformat='%Y-%m-%d'
            )
            st.plotly_chart(fig_spc, use_container_width=True)

            # Tính CP/CPK
            if (lsl_val is not None) and (usl_val is not None):
                mean_val = spc_data["Actual result"].mean()
                std_val = spc_data["Actual result"].std()
                if std_val and std_val > 0:
                    cp = (usl_val - lsl_val) / (6 * std_val)
                    cpk = min((usl_val - mean_val), (mean_val - lsl_val)) / (3 * std_val)
                    st.write(f"**CP**: {cp:.2f}, **CPK**: {cpk:.2f}")
                else:
                    st.write("Standard Deviation is zero, cannot compute CP/CPK.")

# ================= TAB 3: BOXPLOT ================= #
with tabs[2]:
    st.header("Boxplot")

    # Chia data thành 2 nhóm: RM/PG vs. non-RM/PG
    rm_pg_data = filtered_df[filtered_df["Sample Type"].isin(["RM - Raw material", "PG - Packaging"])].copy()
    non_rm_pg_data = filtered_df[~filtered_df["Sample Type"].isin(["RM - Raw material", "PG - Packaging"])].copy()

    with st.expander("Boxplot for RM/PG by Supplier", expanded=False):
        if rm_pg_data.empty:
            st.info("No RM/PG data available.")
        else:
            rm_pg_data["SupplierShort"] = rm_pg_data["supplier_name"].dropna().apply(
                lambda x: x[:3] if isinstance(x, str) else x
            )
            fig_box_rm = px.box(
                rm_pg_data,
                x="SupplierShort",
                y="Actual result",
                points="all",
                title="RM/PG - Boxplot by Supplier"
            )
            st.plotly_chart(fig_box_rm, use_container_width=True)

    with st.expander("Boxplot for Non-RM/PG (IP, FG...) by Month", expanded=False):
        if non_rm_pg_data.empty:
            st.info("No non-RM/PG data available.")
        else:
            non_rm_pg_data["Month"] = pd.to_datetime(non_rm_pg_data["final_date"], errors='coerce').dt.to_period("M")
            non_rm_pg_data["Month"] = non_rm_pg_data["Month"].astype(str)
            fig_box_non = px.box(
                non_rm_pg_data,
                x="Month",
                y="Actual result",
                points="all",
                title="Non-RM/PG - Boxplot by Month"
            )
            st.plotly_chart(fig_box_non, use_container_width=True)

    # Boxplot tổng hợp cho tất cả (MBP) theo tháng
    with st.expander("Boxplot All Data (MBP) by Month", expanded=False):
        if filtered_df.empty:
            st.info("No data available for MBP boxplot.")
        else:
            all_mbp_data = filtered_df.copy()
            # Tạo cột Month từ final_date
            all_mbp_data["Month"] = pd.to_datetime(all_mbp_data["final_date"], errors='coerce').dt.to_period("M")
            all_mbp_data["Month"] = all_mbp_data["Month"].astype(str)

            # Tạo 1 cột 'Plant' = 'MBP' để x-axis chỉ có 1 cột
            all_mbp_data["Plant"] = "MBP"

            fig_box_mbp = px.box(
                all_mbp_data,
                x="Plant",           # Chỉ 1 nhóm: MBP
                y="Actual result",
                color="Month",       # Mỗi tháng 1 màu
                points="all",
                title="All Data (MBP) by Month"
            )
            st.plotly_chart(fig_box_mbp, use_container_width=True)

# ================= TAB 4: DISTRIBUTION ================= #
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

# ================= TAB 5: PARETO ================= #
with tabs[4]:
    st.header("Pareto Chart (Out-of-Spec)")
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
