import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from data_processing import prepare_data

# Cấu hình trang
st.set_page_config(page_title="Quality Control Dashboard", layout="wide")

# Load dữ liệu (cache trong 1 giờ)
@st.cache_data(ttl=3600)
def load_data():
    return prepare_data()

df = load_data()
if df is None:
    st.error("Unable to load data. Please check your configuration.")
    st.stop()

# Tạo một bản sao dữ liệu để áp dụng cascading filters
filtered_df = df.copy()

st.sidebar.header("Cascading Filters")

# --- Filter 1: Category description ---
with st.sidebar.expander("Category description"):
    if "Category description" in filtered_df.columns:
        cat_options = sorted(filtered_df["Category description"].dropna().unique())
        selected_cat = st.multiselect("Select Category description", options=cat_options, default=cat_options)
        if selected_cat:
            filtered_df = filtered_df[filtered_df["Category description"].isin(selected_cat)]
    else:
        selected_cat = None

# --- Filter 2: Spec category ---
with st.sidebar.expander("Spec category"):
    if "Spec category" in filtered_df.columns:
        spec_cat_options = sorted(filtered_df["Spec category"].dropna().unique())
        selected_spec_cat = st.multiselect("Select Spec category", options=spec_cat_options, default=spec_cat_options)
        if selected_spec_cat:
            filtered_df = filtered_df[filtered_df["Spec category"].isin(selected_spec_cat)]
    else:
        selected_spec_cat = None

# --- Filter 3: Spec description ---
with st.sidebar.expander("Spec description"):
    if "Spec description" in filtered_df.columns:
        spec_desc_options = sorted(filtered_df["Spec description"].dropna().unique())
        selected_spec_desc = st.multiselect("Select Spec description", options=spec_desc_options, default=spec_desc_options)
        if selected_spec_desc:
            filtered_df = filtered_df[filtered_df["Spec description"].isin(selected_spec_desc)]
    else:
        selected_spec_desc = None

# --- Filter 4: Test description ---
with st.sidebar.expander("Test description"):
    if "Test description" in filtered_df.columns:
        test_desc_options = sorted(filtered_df["Test description"].dropna().unique())
        selected_test_desc = st.multiselect("Select Test description", options=test_desc_options, default=test_desc_options)
        if selected_test_desc:
            filtered_df = filtered_df[filtered_df["Test description"].isin(selected_test_desc)]
    else:
        selected_test_desc = None

# --- Filter 5: Sample Type ---
with st.sidebar.expander("Sample Type"):
    if "Sample Type" in filtered_df.columns:
        sample_type_options = sorted(filtered_df["Sample Type"].dropna().unique())
        selected_sample_type = st.multiselect("Select Sample Type", options=sample_type_options, default=sample_type_options)
        if selected_sample_type:
            filtered_df = filtered_df[filtered_df["Sample Type"].isin(selected_sample_type)]
    else:
        selected_sample_type = None

# --- Date Filter ---
st.sidebar.header("Date Filter")
# Cho phép chọn cột ngày dùng để lọc (warehouse_date hoặc supplier_date)
date_filter_field = st.sidebar.radio("Select Date Field for Filtering", ("warehouse_date", "supplier_date"))
available_dates = pd.to_datetime(filtered_df[date_filter_field].dropna(), errors="coerce")
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
        (pd.to_datetime(filtered_df[date_filter_field], errors="coerce").dt.date >= start_date) &
        (pd.to_datetime(filtered_df[date_filter_field], errors="coerce").dt.date <= end_date)
    ]
else:
    st.sidebar.write("No available dates in the selected date field.")

st.sidebar.write(f"Total records after filtering: {len(filtered_df)}")

# Tùy chọn nhóm theo Supplier (áp dụng cho RM, PG)
group_by_supplier = st.sidebar.checkbox("Group by Supplier (for RM, PG)", value=False)

# Tùy chọn nhóm cho Pareto Chart
pareto_group_option = st.sidebar.selectbox("Pareto Grouping", options=["Test description", "Spec description"])

# Tạo các tab cho các biểu đồ
tabs = st.tabs(["Time Series", "SPC Chart", "Boxplot", "Distribution", "Pareto Chart"])

####################################
# Tab 1: Time Series Chart
####################################
with tabs[0]:
    st.header("Time Series Chart")
    # Cho phép người dùng chọn loại ngày: Ngày nhập kho (warehouse_date) hoặc NSX của NCC (supplier_date)
    ts_date_option = st.selectbox("Select Date Field for Time Series", 
                                  options=["Ngày nhập kho (warehouse_date)", "NSX của NCC (supplier_date)"])
    ts_date_field = "warehouse_date" if "nhập kho" in ts_date_option.lower() else "supplier_date"
    
    # Lựa chọn Test cho biểu đồ (dựa trên dữ liệu đã lọc)
    available_tests_ts = sorted(filtered_df["Test description"].dropna().unique())
    selected_test_ts = st.selectbox("Select Test for Time Series", options=available_tests_ts)
    ts_data = filtered_df[filtered_df["Test description"] == selected_test_ts].copy()
    
    if ts_data.empty:
        st.warning("No data available for the selected Test in Time Series Chart.")
    else:
        fig_ts = go.Figure()
        if group_by_supplier:
            suppliers = ts_data["supplier_name"].dropna().unique()
            for sup in suppliers:
                sup_data = ts_data[ts_data["supplier_name"] == sup]
                fig_ts.add_trace(go.Scatter(
                    x=sup_data[ts_date_field],
                    y=sup_data["Actual result"],
                    mode="lines+markers",
                    name=f"Supplier {sup}"
                ))
        else:
            fig_ts.add_trace(go.Scatter(
                x=ts_data[ts_date_field],
                y=ts_data["Actual result"],
                mode="lines+markers",
                name="Actual Result"
            ))
        fig_ts.update_layout(
            title=f"Time Series Chart - {selected_test_ts}",
            xaxis_title="Date",
            yaxis_title="Actual Result",
            showlegend=True
        )
        st.plotly_chart(fig_ts, use_container_width=True)

####################################
# Tab 2: SPC Chart
####################################
with tabs[1]:
    st.header("SPC Chart")
    # Cho phép chọn cột ngày cho SPC Chart
    spc_date_option = st.selectbox("Select Date Field for SPC Chart", 
                                   options=["Lot Date (warehouse_date)", "NSX Date (supplier_date)"])
    spc_date_field = "warehouse_date" if "lot" in spc_date_option.lower() else "supplier_date"
    
    available_tests_spc = sorted(filtered_df["Test description"].dropna().unique())
    selected_test_spc = st.selectbox("Select Test for SPC Chart", options=available_tests_spc)
    spc_data = filtered_df[filtered_df["Test description"] == selected_test_spc].copy()
    
    if spc_data.empty:
        st.warning("No data available for the selected Test in SPC Chart.")
    else:
        fig_spc = go.Figure()
        fig_spc.add_trace(go.Scatter(
            x=spc_data[spc_date_field],
            y=spc_data["Actual result"],
            mode="lines+markers",
            name="Actual Result"
        ))
        # Ép giá trị LSL và USL về numeric
        lsl_val = None
        usl_val = None
        if "Lower limit" in spc_data.columns and "Upper limit" in spc_data.columns:
            lsl_val = pd.to_numeric(spc_data["Lower limit"].iloc[0], errors="coerce")
            usl_val = pd.to_numeric(spc_data["Upper limit"].iloc[0], errors="coerce")
        if lsl_val is not None and pd.notnull(lsl_val):
            fig_spc.add_hline(y=lsl_val, line_dash="dash", line_color="red", annotation_text="LSL")
        if usl_val is not None and pd.notnull(usl_val):
            fig_spc.add_hline(y=usl_val, line_dash="dash", line_color="red", annotation_text="USL")
        fig_spc.update_layout(
            title=f"SPC Chart - {selected_test_spc}",
            xaxis_title="Date",
            yaxis_title="Actual Result",
            showlegend=True
        )
        st.plotly_chart(fig_spc, use_container_width=True)
        # Tính CP, CPK nếu có
        if lsl_val is not None and usl_val is not None and pd.notnull(lsl_val) and pd.notnull(usl_val):
            mean_val = spc_data["Actual result"].mean()
            sigma = spc_data["Actual result"].std()
            if sigma > 0:
                cp = (usl_val - lsl_val) / (6 * sigma)
                cpk = min((usl_val - mean_val), (mean_val - lsl_val)) / (3 * sigma)
                st.write(f"CP: {cp:.2f}, CPK: {cpk:.2f}")
            else:
                st.write("Standard deviation is zero, cannot compute CP/CPK.")

####################################
# Tab 3: Boxplot (For RM, PG by Supplier)
####################################
with tabs[2]:
    st.header("Boxplot of Actual Results by Supplier (RM, PG)")
    box_data = filtered_df[filtered_df["Sample Type"].isin(["RM - Raw material", "PG - Packaging"])].copy()
    if box_data.empty:
        st.warning("No data available for RM/PG samples.")
    else:
        fig_box = px.box(box_data, x="supplier_name", y="Actual result", points="all",
                         title="Boxplot of Actual Results by Supplier")
        st.plotly_chart(fig_box, use_container_width=True)

####################################
# Tab 4: Distribution Chart
####################################
with tabs[3]:
    st.header("Distribution Chart")
    if filtered_df.empty:
        st.warning("No data available for Distribution Chart.")
    else:
        fig_dist = px.histogram(filtered_df, x="Actual result", nbins=30, marginal="box",
                                title="Distribution of Actual Results")
        st.plotly_chart(fig_dist, use_container_width=True)

####################################
# Tab 5: Pareto Chart for Out-of-Spec Samples
####################################
with tabs[4]:
    st.header("Pareto Chart for Out-of-Spec Samples")
    temp_df = filtered_df.copy()
    if "Lower limit" in temp_df.columns and "Upper limit" in temp_df.columns:
        temp_df["Lower limit num"] = pd.to_numeric(temp_df["Lower limit"], errors="coerce")
        temp_df["Upper limit num"] = pd.to_numeric(temp_df["Upper limit"], errors="coerce")
        temp_df["Actual result num"] = pd.to_numeric(temp_df["Actual result"], errors="coerce")
        temp_df["out_spec"] = ((temp_df["Actual result num"] < temp_df["Lower limit num"]) |
                               (temp_df["Actual result num"] > temp_df["Upper limit num"]))
        pareto_df = temp_df[temp_df["out_spec"] == True].copy()
        if pareto_df.empty:
            st.warning("No out-of-spec samples found.")
        else:
            group_field = pareto_group_option
            pareto_counts = pareto_df.groupby(group_field).size().reset_index(name="Count")
            pareto_counts = pareto_counts.sort_values(by="Count", ascending=False)
            pareto_counts["Cumulative Count"] = pareto_counts["Count"].cumsum()
            pareto_counts["Cumulative %"] = 100 * pareto_counts["Cumulative Count"] / pareto_counts["Count"].sum()
            fig_pareto = go.Figure()
            fig_pareto.add_trace(go.Bar(
                x=pareto_counts[group_field],
                y=pareto_counts["Count"],
                name="Count"
            ))
            fig_pareto.add_trace(go.Scatter(
                x=pareto_counts[group_field],
                y=pareto_counts["Cumulative %"],
                name="Cumulative %",
                yaxis="y2",
                mode="lines+markers"
            ))
            fig_pareto.update_layout(
                title="Pareto Chart for Out-of-Spec Samples",
                xaxis_title=group_field,
                yaxis=dict(title="Count"),
                yaxis2=dict(title="Cumulative %", overlaying="y", side="right", range=[0, 110])
            )
            st.plotly_chart(fig_pareto, use_container_width=True)
    else:
        st.warning("Lower limit and Upper limit columns are not available for Pareto Chart.")
