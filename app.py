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

# Import hàm xử lý data
from data_processing import prepare_data

# --------------------- Load Data Once ---------------------
df, df_outliers = prepare_data()
if df is None:
    df = pd.DataFrame()
if df_outliers is None:
    df_outliers = pd.DataFrame()

# Khởi tạo Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server  # Để deploy Heroku, v.v.

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
            min_date_allowed=df["final_date"].min(),
            max_date_allowed=df["final_date"].max(),
            start_date=df["final_date"].min(),
            end_date=df["final_date"].max()
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
    Lọc df và df_outliers theo các filter (multiselect + date range).
    Trả về df lọc dưới dạng JSON (để store).
    """
    if df.empty:
        return None, None, "DataFrame is empty."

    # copy
    filtered_main = df.copy()
    filtered_out = df_outliers.copy()

    # Lọc category
    if category_vals and len(category_vals) > 0:
        filtered_main = filtered_main[filtered_main["Category description"].isin(category_vals)]
        filtered_out = filtered_out[filtered_out["Category description"].isin(category_vals)]

    # Lọc sample type
    if sample_vals and len(sample_vals) > 0:
        filtered_main = filtered_main[filtered_main["Sample Type"].isin(sample_vals)]
        filtered_out = filtered_out[filtered_out["Sample Type"].isin(sample_vals)]

    # Lọc spec desc
    if spec_vals and len(spec_vals) > 0:
        filtered_main = filtered_main[filtered_main["Spec description"].isin(spec_vals)]
        filtered_out = filtered_out[filtered_out["Spec description"].isin(spec_vals)]

    # Lọc test desc
    if test_vals and len(test_vals) > 0:
        filtered_main = filtered_main[filtered_main["Test description"].isin(test_vals)]
        filtered_out = filtered_out[filtered_out["Test description"].isin(test_vals)]

    # Lọc date range
    if start_date and end_date:
        mask_main = (pd.to_datetime(filtered_main["final_date"], errors="coerce") >= start_date) & \
                    (pd.to_datetime(filtered_main["final_date"], errors="coerce") <= end_date)
        filtered_main = filtered_main[mask_main]

        mask_out = (pd.to_datetime(filtered_out["final_date"], errors="coerce") >= start_date) & \
                   (pd.to_datetime(filtered_out["final_date"], errors="coerce") <= end_date)
        filtered_out = filtered_out[mask_out]

    info_txt = f"Records after filtering: {len(filtered_main)}"

    # Convert to JSON
    return filtered_main.to_json(date_format="iso", orient="split"), \
           filtered_out.to_json(date_format="iso", orient="split"), \
           info_txt

@app.callback(
    Output("tabs-content", "children"),
    Input("main-tabs", "value"),
    State("filtered-df-store", "data"),
    State("filtered-outliers-store", "data")
)
def render_tab_content(tab, filtered_main_json, filtered_out_json):
    """
    Render nội dung cho mỗi Tab dựa trên df đã filter.
    """
    if not filtered_main_json:
        return html.Div("No data available.")
    filtered_main = pd.read_json(filtered_main_json, orient="split")
    filtered_out = pd.DataFrame()
    if filtered_out_json:
        filtered_out = pd.read_json(filtered_out_json, orient="split")

    if tab == "tab-timeseries":
        return render_time_series(filtered_main, filtered_out)
    elif tab == "tab-spc":
        return render_spc_chart(filtered_main)
    elif tab == "tab-box":
        return render_boxplot(filtered_main)
    elif tab == "tab-dist":
        return render_distribution(filtered_main)
    elif tab == "tab-pareto":
        return render_pareto(filtered_main)
    return html.Div("Tab not recognized.")

# --------------- HÀM VẼ CHI TIẾT ---------------

def render_time_series(df_main, df_out):
    if df_main.empty:
        return html.Div("No data for Time Series.")
    # Tương tự Streamlit: vẽ lines cho mỗi Test description
    ts_data = df_main.sort_values(by="final_date")
    fig_ts = go.Figure()
    tests = ts_data["Test description"].dropna().unique()
    for test in tests:
        sub_df = ts_data[ts_data["Test description"] == test]
        fig_ts.add_trace(go.Scatter(
            x=sub_df["final_date"],
            y=sub_df["Actual result"],
            mode="lines+markers",
            name=str(test)
        ))
    fig_ts.update_layout(title="Time Series Chart", xaxis_title="Date", yaxis_title="Actual Result")
    # Hiển thị outliers
    outlier_table = html.Div()
    if not df_out.empty:
        outlier_table = html.Div([
            html.H6("Outliers (excluded from chart):"),
            dbc.Table.from_dataframe(
                df_out[["final_date","Actual result","Lot number"]].sort_values("final_date"),
                striped=True, bordered=True, hover=True
            )
        ])
    return html.Div([
        dcc.Graph(figure=fig_ts),
        outlier_table
    ])

def render_spc_chart(df_main):
    if df_main.empty:
        return html.Div("No data for SPC Chart.")
    spc_data = df_main.sort_values(by="final_date")
    fig_spc = go.Figure()
    fig_spc.add_trace(go.Scatter(
        x=spc_data["final_date"],
        y=spc_data["Actual result"],
        mode="lines+markers",
        name="Actual Result"
    ))
    lsl_val = numeric_or_none(spc_data["Lower limit"].iloc[0]) if "Lower limit" in spc_data.columns and not spc_data.empty else None
    usl_val = numeric_or_none(spc_data["Upper limit"].iloc[0]) if "Upper limit" in spc_data.columns and not spc_data.empty else None
    if lsl_val is not None:
        fig_spc.add_hline(y=lsl_val, line_dash="dash", line_color="red", annotation_text="LSL")
    if usl_val is not None:
        fig_spc.add_hline(y=usl_val, line_dash="dash", line_color="red", annotation_text="USL")
    fig_spc.update_layout(title="SPC Chart", xaxis_title="Date", yaxis_title="Actual Result")
    return dcc.Graph(figure=fig_spc)

def render_boxplot(df_main):
    if df_main.empty:
        return html.Div("No data for Boxplot.")
    # Tách RM/PG vs non-RM/PG v.v. tuỳ bạn
    fig_box = px.box(df_main, x="Sample Type", y="Actual result", points="all", title="Boxplot by Sample Type")
    return dcc.Graph(figure=fig_box)

def render_distribution(df_main):
    if df_main.empty:
        return html.Div("No data for Distribution.")
    actual = pd.to_numeric(df_main["Actual result"], errors='coerce').dropna()
    if actual.empty:
        return html.Div("No numeric data for Distribution.")
    mean_val = actual.mean()
    std_val = actual.std()
    fig_hist = px.histogram(actual, nbins=30, histnorm='density', title="Distribution of Actual Results")
    # Thêm đường cong chuẩn
    x_vals = np.linspace(actual.min(), actual.max(), 100)
    y_vals = norm.pdf(x_vals, mean_val, std_val)
    fig_hist.add_trace(go.Scatter(x=x_vals, y=y_vals, mode="lines", name="Normal Curve"))
    fig_hist.update_layout(xaxis_title="Actual result", yaxis_title="Density")
    return dcc.Graph(figure=fig_hist)

def render_pareto(df_main):
    if df_main.empty:
        return html.Div("No data for Pareto Chart.")
    # Tương tự code cũ, group by out-of-spec ...
    # Ở đây demo đơn giản
    return html.Div("Pareto Chart logic here...")

# Chạy local
if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)
