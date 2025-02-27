# file: app.py
import os
import pandas as pd
import numpy as np
import dash
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from scipy.stats import norm
import time
import json
from functools import lru_cache
from google.cloud import bigquery

# --------------------- Optimized Data Processing Functions ---------------------

def get_bigquery_data(date_limit=90, limit=None):
    """
    Optimized function to fetch data from BigQuery with filtering at source
    """
    try:
        start_time = time.time()
        credentials_json = os.getenv("GCP_SERVICE_ACCOUNT", "{}")
        credentials_info = json.loads(credentials_json)
        table = os.getenv("GOOGLE_BIGQUERY_TABLE", "project.dataset.table")

        client = bigquery.Client.from_service_account_info(credentials_info)
        
        # Filter data at source
        limit_clause = f"LIMIT {limit}" if limit else ""
        date_clause = ""
        if date_limit:
            date_clause = f"WHERE `Receipt Date` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {date_limit} DAY)"
        
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
            {date_clause}
            {limit_clause}
        """
        
        # Optimize query performance
        job_config = bigquery.QueryJobConfig(
            maximum_bytes_billed=1_000_000_000,  # 1GB
            use_query_cache=True,
        )
        
        query_job = client.query(query, job_config=job_config)
        df = query_job.to_dataframe()
        
        print(f"Data fetched from BigQuery in {time.time() - start_time:.2f} seconds")
        return df
    except Exception as e:
        print(f"[ERROR] get_bigquery_data: {e}")
        return None

def parse_dates_vectorized(df):
    """
    Vectorized processing of date fields
    """
    # Convert Receipt Date
    if "Receipt Date" in df.columns:
        df['Receipt Date'] = pd.to_datetime(df['Receipt Date'], errors='coerce', dayfirst=True)
    
    # Process lot numbers
    lot_numbers = df['Lot number'].astype(str).str.strip()
    sample_types = df['Sample Type'].astype(str).str.strip().str.lower()
    
    # Initialize columns
    df['warehouse_date'] = pd.NaT
    df['supplier_date'] = pd.NaT
    df['supplier_name'] = None
    
    # Split lot numbers
    lot_parts = lot_numbers.str.split('-', expand=True)
    
    # Create masks for different sample types
    rm_pg_mask = (sample_types.str.contains('rm') | 
                  sample_types.str.contains('raw material') | 
                  sample_types.str.contains('pg') | 
                  sample_types.str.contains('packaging'))
    
    # Process dates from lot parts
    if 0 in lot_parts.columns:
        # For RM/PG: First part is warehouse date
        first_part = lot_parts[0].str.strip()
        warehouse_dates = pd.to_datetime(first_part.str[:6], format='%d%m%y', errors='coerce')
        df.loc[rm_pg_mask, 'warehouse_date'] = warehouse_dates[rm_pg_mask]
        
        # For non-RM/PG: First part is supplier date
        df.loc[~rm_pg_mask, 'supplier_date'] = warehouse_dates[~rm_pg_mask]
    
    # Process supplier names from second part
    if 1 in lot_parts.columns:
        valid_supplier_mask = (rm_pg_mask) & (lot_parts[1] != 'MBP')
        df.loc[valid_supplier_mask, 'supplier_name'] = lot_parts[1][valid_supplier_mask]
    
    # Process supplier dates for RM/PG from third part
    if 2 in lot_parts.columns:
        third_part = lot_parts[2].str.strip()
        supplier_dates = pd.to_datetime(third_part.str[:6], format='%d%m%y', errors='coerce')
        df.loc[rm_pg_mask, 'supplier_date'] = supplier_dates[rm_pg_mask]
    
    # Create final_date column
    df['final_date'] = pd.NaT
    
    # For RM/PG: Use warehouse_date if available, otherwise supplier_date
    df.loc[rm_pg_mask, 'final_date'] = df.loc[rm_pg_mask, 'warehouse_date']
    missing_warehouse = rm_pg_mask & df['warehouse_date'].isna()
    df.loc[missing_warehouse, 'final_date'] = df.loc[missing_warehouse, 'supplier_date']
    
    # For non-RM/PG: Use Receipt Date if available, otherwise supplier_date
    df.loc[~rm_pg_mask, 'final_date'] = df.loc[~rm_pg_mask, 'Receipt Date']
    missing_receipt = ~rm_pg_mask & df['Receipt Date'].isna()
    df.loc[missing_receipt, 'final_date'] = df.loc[missing_receipt, 'supplier_date']
    
    return df

def remove_outliers(df, column='Actual result', method='IQR', factor=1.5):
    """
    Optimized function to remove outliers
    """
    if df is None or df.empty or column not in df.columns:
        return df, pd.DataFrame()

    # Convert column to numeric
    df[column] = pd.to_numeric(
        df[column].astype(str)
          .str.replace(',', '.')
          .str.extract(r'(\d+\.?\d*)')[0],
        errors='coerce'
    )
    
    # Skip NaN values
    valid_mask = ~df[column].isna()
    if sum(valid_mask) == 0:
        return df, pd.DataFrame()
        
    if method == 'IQR':
        Q1 = df.loc[valid_mask, column].quantile(0.25)
        Q3 = df.loc[valid_mask, column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - factor * IQR
        upper_bound = Q3 + factor * IQR
    else:
        mean_val = df.loc[valid_mask, column].mean()
        std_val = df.loc[valid_mask, column].std()
        lower_bound = mean_val - factor * std_val
        upper_bound = mean_val + factor * std_val

    # Use boolean indexing for efficiency
    outlier_mask = (df[column] < lower_bound) | (df[column] > upper_bound)
    return df[~outlier_mask], df[outlier_mask]

@lru_cache(maxsize=2)
def prepare_data(date_limit=90, limit=None):
    """
    Cached function to prepare data
    """
    try:
        start_time = time.time()
        
        # Get data from BigQuery
        df = get_bigquery_data(date_limit, limit)
        if df is None or df.empty:
            return None, None

        # Filter unwanted departments
        if "Charge department" in df.columns:
            df = df[~df["Charge department"].isin(["SHE.MBP", "MFG.MBP"])]

        # Process dates
        df = parse_dates_vectorized(df)
        
        # Remove outliers
        df_cleaned, df_outliers = remove_outliers(df, column='Actual result', method='IQR', factor=1.5)
        
        print(f"Total data preparation time: {time.time() - start_time:.2f} seconds")
        return df_cleaned, df_outliers

    except Exception as e:
        print(f"[ERROR] prepare_data: {e}")
        return None, None

# --------------------- App Configuration ---------------------
# Khởi tạo Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server  # Để deploy Heroku, v.v.

# Read environment variables for data loading
date_limit = int(os.getenv('DATE_LIMIT', '90'))
record_limit = os.getenv('RECORD_LIMIT')
if record_limit:
    record_limit = int(record_limit)

# Load data with progress tracking
print(f"Loading data with date_limit={date_limit}, record_limit={record_limit}")
start_time = time.time()
df, df_outliers = prepare_data(date_limit, record_limit)
print(f"Data loaded in {time.time() - start_time:.2f} seconds")

if df is None:
    df = pd.DataFrame()
if df_outliers is None:
    df_outliers = pd.DataFrame()

print(f"Data loaded: {len(df)} records, {len(df_outliers)} outliers")

# --------------------- LAYOUT ---------------------
# Filter region (Dropdown + DatePicker)
filter_card = dbc.Card(
    [
        html.H5("Filters", className="card-title"),
        html.Label("Category description"),
        dcc.Dropdown(
            id="category-desc-dd",
            options=[{"label": x, "value": x} for x in sorted(df["Category description"].dropna().unique())],
            multi=True
        ),
        html.Label("Sample Type"),
        dcc.Dropdown(
            id="sample-type-dd",
            options=[{"label": x, "value": x} for x in sorted(df["Sample Type"].dropna().unique())],
            multi=True
        ),
        html.Label("Spec description"),
        dcc.Dropdown(
            id="spec-desc-dd",
            options=[{"label": x, "value": x} for x in sorted(df["Spec description"].dropna().unique())],
            multi=True
        ),
        html.Label("Test description"),
        dcc.Dropdown(
            id="test-desc-dd",
            options=[{"label": x, "value": x} for x in sorted(df["Test description"].dropna().unique())],
            multi=True
        ),
        html.Label("Date Range (final_date)"),
        dcc.DatePickerRange(
            id="date-range",
            min_date_allowed=df["final_date"].min() if not df.empty else None,
            max_date_allowed=df["final_date"].max() if not df.empty else None,
            start_date=df["final_date"].min() if not df.empty else None,
            end_date=df["final_date"].max() if not df.empty else None
        ),
        html.Div(id="filter-info", style={"marginTop": "10px", "fontStyle": "italic"})
    ],
    body=True
)

tabs_component = dcc.Tabs(
    id="main-tabs", value="tab-timeseries", children=[
        dcc.Tab(label="Time Series", value="tab-timeseries"),
        dcc.Tab(label="SPC Chart", value="tab-spc"),
        dcc.Tab(label="Boxplot", value="tab-box"),
        dcc.Tab(label="Distribution", value="tab-dist"),
        dcc.Tab(label="Pareto Chart", value="tab-pareto")
    ]
)

app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(filter_card, width=3),
        dbc.Col([
            html.H2("Quality Control Dashboard - Dash", style={"marginTop":"20px"}),
            html.Div([
                html.Span(f"Data loaded: {len(df):,} records, "),
                html.Span(f"Date range: {df['final_date'].min()} to {df['final_date'].max()}" if not df.empty else "No data")
            ], style={"marginBottom": "10px", "fontSize": "0.9em", "color": "#666"}),
            tabs_component,
            html.Div(id="tabs-content", style={"marginTop":"20px"})
        ], width=9)
    ], style={"marginTop":"20px"}),
    dcc.Store(id="filtered-df-store"),      # Lưu trữ df đã filter
    dcc.Store(id="filtered-outliers-store") # Lưu trữ outliers đã filter
], fluid=True)

# --------------------- CALLBACKS ---------------------

def numeric_or_none(val):
    try:
        return float(val)
    except:
        return None

@app.callback(
    Output("filtered-df-store", "data"),
    Output("filtered-outliers-store", "data"),
    Output("filter-info", "children"),
    Input("category-desc-dd", "value"),
    Input("sample-type-dd", "value"),
    Input("spec-desc-dd", "value"),
    Input("test-desc-dd", "value"),
    Input("date-range", "start_date"),
    Input("date-range", "end_date")
)
def filter_data(category_vals, sample_vals, spec_vals, test_vals, start_date, end_date):
    """
    Optimized filtering function
    """
    start_time = time.time()
    
    if df.empty:
        return None, None, "DataFrame is empty."

    # Apply filters using boolean masks, not copying the dataframe until necessary
    mask_main = pd.Series(True, index=df.index)
    mask_out = pd.Series(True, index=df_outliers.index) if not df_outliers.empty else None

    # Lọc category
    if category_vals and len(category_vals) > 0:
        cat_mask = df["Category description"].isin(category_vals)
        mask_main &= cat_mask
        if mask_out is not None:
            mask_out &= df_outliers["Category description"].isin(category_vals)

    # Lọc sample type
    if sample_vals and len(sample_vals) > 0:
        sample_mask = df["Sample Type"].isin(sample_vals)
        mask_main &= sample_mask
        if mask_out is not None:
            mask_out &= df_outliers["Sample Type"].isin(sample_vals)

    # Lọc spec desc
    if spec_vals and len(spec_vals) > 0:
        spec_mask = df["Spec description"].isin(spec_vals)
        mask_main &= spec_mask
        if mask_out is not None:
            mask_out &= df_outliers["Spec description"].isin(spec_vals)

    # Lọc test desc
    if test_vals and len(test_vals) > 0:
        test_mask = df["Test description"].isin(test_vals)
        mask_main &= test_mask
        if mask_out is not None:
            mask_out &= df_outliers["Test description"].isin(test_vals)

    # Lọc date range
    if start_date and end_date:
        date_mask = (pd.to_datetime(df["final_date"], errors="coerce") >= start_date) & \
                    (pd.to_datetime(df["final_date"], errors="coerce") <= end_date)
        mask_main &= date_mask
        
        if mask_out is not None:
            date_mask_out = (pd.to_datetime(df_outliers["final_date"], errors="coerce") >= start_date) & \
                           (pd.to_datetime(df_outliers["final_date"], errors="coerce") <= end_date)
            mask_out &= date_mask_out

    # Only make copies when all filters have been applied
    filtered_main = df[mask_main]
    filtered_out = df_outliers[mask_out] if not df_outliers.empty and mask_out is not None else pd.DataFrame()

    info_txt = f"Records after filtering: {len(filtered_main):,} (filtered in {time.time() - start_time:.2f}s)"

    # Convert only essential columns to JSON to reduce memory usage
    # This significantly reduces the size of the data being transferred
    main_columns = ['final_date', 'Actual result', 'Lower limit', 'Upper limit', 
                    'Test description', 'Sample Type', 'Category description', 
                    'Spec description', 'Lot number', 'supplier_name']
    
    outlier_columns = ['final_date', 'Actual result', 'Lot number']
    
    filtered_main_json = filtered_main[
        [col for col in main_columns if col in filtered_main.columns]
    ].to_json(date_format="iso", orient="split")
    
    filtered_out_json = filtered_out[
        [col for col in outlier_columns if col in filtered_out.columns]
    ].to_json(date_format="iso", orient="split") if not filtered_out.empty else None

    return filtered_main_json, filtered_out_json, info_txt

@app.callback(
    Output("tabs-content", "children"),
    Input("main-tabs", "value"),
    State("filtered-df-store", "data"),
    State("filtered-outliers-store", "data")
)
def render_tab_content(tab, filtered_main_json, filtered_out_json):
    """
    Render the content for each tab
    """
    start_time = time.time()
    
    if not filtered_main_json:
        return html.Div("No data available.")
    
    filtered_main = pd.read_json(filtered_main_json, orient="split")
    filtered_out = pd.DataFrame()
    if filtered_out_json:
        filtered_out = pd.read_json(filtered_out_json, orient="split")

    content = html.Div("Tab not recognized.")
    
    if tab == "tab-timeseries":
        content = render_time_series(filtered_main, filtered_out)
    elif tab == "tab-spc":
        content = render_spc_chart(filtered_main)
    elif tab == "tab-box":
        content = render_boxplot(filtered_main)
    elif tab == "tab-dist":
        content = render_distribution(filtered_main)
    elif tab == "tab-pareto":
        content = render_pareto(filtered_main)
    
    print(f"Rendered {tab} in {time.time() - start_time:.2f} seconds")
    return content

# --------------- CHART RENDERING FUNCTIONS ---------------

def render_time_series(df_main, df_out):
    if df_main.empty:
        return html.Div("No data for Time Series.")

    # Sorting with inplace=True to avoid copy
    ts_data = df_main.sort_values(by="final_date")
    
    # Check if we need to limit the number of lines (for performance)
    tests = ts_data["Test description"].dropna().unique()
    
    # If too many unique tests, limit to top 10 by frequency
    if len(tests) > 10:
        top_tests = ts_data["Test description"].value_counts().head(10).index.tolist()
        tests = top_tests
        warning = html.Div(f"Showing top 10 tests out of {len(ts_data['Test description'].dropna().unique())}", 
                         style={"color": "orange", "marginBottom": "10px"})
    else:
        warning = None
    
    # Build the figure
    fig_ts = go.Figure()
    for test in tests:
        sub_df = ts_data[ts_data["Test description"] == test]
        if len(sub_df) > 1:  # Only add a trace if there are at least 2 points
            fig_ts.add_trace(go.Scatter(
                x=sub_df["final_date"],
                y=sub_df["Actual result"],
                mode="lines+markers",
                name=str(test)
            ))
    
    fig_ts.update_layout(
        title="Time Series Chart", 
        xaxis_title="Date", 
        yaxis_title="Actual Result",
        height=500  # Fixed height for better performance
    )
    
    # Show outliers in a collapsible section
    outlier_table = html.Div()
    if not df_out.empty:
        outlier_table = dbc.Collapse(
            dbc.Card(
                dbc.CardBody([
                    html.H6("Outliers (excluded from chart):"),
                    dbc.Table.from_dataframe(
                        df_out[["final_date","Actual result","Lot number"]].sort_values("final_date"),
                        striped=True, bordered=True, hover=True, size="sm"
                    )
                ])
            ),
            id="outlier-collapse",
            is_open=False,
        )
        outlier_toggle = dbc.Button(
            "Show/Hide Outliers",
            id="outlier-toggle",
            color="secondary",
            size="sm",
            className="mb-3",
            n_clicks=0
        )
    else:
        outlier_toggle = None

    # The final layout
    components = []
    if warning:
        components.append(warning)
    components.append(dcc.Graph(figure=fig_ts, config={'displayModeBar': True}))
    if outlier_toggle:
        components.append(outlier_toggle)
    if not df_out.empty:
        components.append(outlier_table)
    
    return html.Div(components)

def render_spc_chart(df_main):
    if df_main.empty:
        return html.Div("No data for SPC Chart.")
        
    spc_data = df_main.sort_values(by="final_date")
    
    # Limit points for performance (if needed)
    if len(spc_data) > 1000:
        spc_data = spc_data.iloc[::int(len(spc_data)/1000)]
        warning = html.Div(f"Data sampled to 1000 points for performance (from {len(df_main)} total)", 
                         style={"color": "orange", "marginBottom": "10px"})
    else:
        warning = None
    
    fig_spc = go.Figure()
    fig_spc.add_trace(go.Scatter(
        x=spc_data["final_date"],
        y=spc_data["Actual result"],
        mode="lines+markers",
        name="Actual Result"
    ))
    
    # Add limits if available
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
        height=500
    )
    
    components = []
    if warning:
        components.append(warning)
    components.append(dcc.Graph(figure=fig_spc))
    
    return html.Div(components)

def render_boxplot(df_main):
    if df_main.empty:
        return html.Div("No data for Boxplot.")
        
    # Simplify boxplot for better performance
    sample_counts = df_main["Sample Type"].value_counts()
    
    # Only use sample types with significant data
    relevant_samples = sample_counts[sample_counts > 5].index.tolist()
    if not relevant_samples:
        return html.Div("Not enough data per sample type for meaningful boxplot.")
    
    filtered_for_box = df_main[df_main["Sample Type"].isin(relevant_samples)]
    
    fig_box = px.box(
        filtered_for_box, 
        x="Sample Type", 
        y="Actual result", 
        points="outliers",  # Only show outlier points for cleaner visualization
        title="Boxplot by Sample Type",
        height=500
    )
    
    return dcc.Graph(figure=fig_box)

def render_distribution(df_main):
    if df_main.empty:
        return html.Div("No data for Distribution.")
        
    # Convert to numeric, dropping NaN values
    actual = pd.to_numeric(df_main["Actual result"], errors='coerce').dropna()
    
    if actual.empty:
        return html.Div("No numeric data for Distribution.")
        
    # Calculate statistics
    mean_val = actual.mean()
    std_val = actual.std()
    
    # Create more efficient histogram with fewer bins for large datasets
    n_bins = min(30, max(10, int(len(actual) / 50)))
    
    fig_hist = px.histogram(
        actual, 
        nbins=n_bins, 
        histnorm='density', 
        title="Distribution of Actual Results",
        height=500
    )
    
    # Add normal curve if there's enough data
    if len(actual) > 10 and std_val > 0:
        x_vals = np.linspace(actual.min(), actual.max(), 100)
        y_vals = norm.pdf(x_vals, mean_val, std_val)
        fig_hist.add_trace(go.Scatter(x=x_vals, y=y_vals, mode="lines", name="Normal Curve"))
    
    fig_hist.update_layout(xaxis_title="Actual result", yaxis_title="Density")
    
    return dcc.Graph(figure=fig_hist)

def render_pareto(df_main):
    if df_main.empty:
        return html.Div("No data for Pareto Chart.")
    
    # Simplified demo implementation
    test_counts = df_main["Test description"].value_counts().reset_index()
    test_counts.columns = ["Test description", "Count"]
    test_counts = test_counts.sort_values("Count", ascending=False).head(10)
    test_counts["Cumulative"] = test_counts["Count"].cumsum()
    test_counts["Cumulative %"] = 100 * test_counts["Cumulative"] / test_counts["Count"].sum()
    
    fig_pareto = go.Figure()
    fig_pareto.add_trace(go.Bar(
        x=test_counts["Test description"],
        y=test_counts["Count"],
        name="Count"
    ))
    fig_pareto.add_trace(go.Scatter(
        x=test_counts["Test description"],
        y=test_counts["Cumulative %"],
        name="Cumulative %",
        yaxis="y2",
        mode="lines+markers"
    ))
    
    fig_pareto.update_layout(
        title="Top 10 Tests Pareto Chart",
        xaxis_title="Test description",
        yaxis=dict(title="Count"),
        yaxis2=dict(title="Cumulative %", overlaying="y", side="right", range=[0, 110]),
        height=500
    )
    
    return dcc.Graph(figure=fig_pareto)

# --------------- ADDITIONAL CALLBACK FOR OUTLIER TOGGLE ---------------
@app.callback(
    Output("outlier-collapse", "is_open"),
    Input("outlier-toggle", "n_clicks"),
    State("outlier-collapse", "is_open"),
)
def toggle_outlier_collapse(n, is_open):
    if n:
        return not is_open
    return is_open

# Chạy local
if __name__ == "__main__":
    # Determine port and host from environment variables (for Hugging Face)
    port = int(os.environ.get("PORT", 8050))
    host = os.environ.get("HOST", "0.0.0.0")
    debug = os.environ.get("DEBUG", "False").lower() == "true"
    
    print(f"Starting server on {host}:{port}, debug={debug}")
    app.run_server(debug=debug, host=host, port=port)
