import streamlit as st
import plotly.graph_objects as go
from data_processing import prepare_data

# Configuration
st.set_page_config(page_title="Quality Control Dashboard", layout="wide")

# Load data
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_data():
    return prepare_data()

try:
    df = load_data()
    
    if df is not None:
        # Sidebar filters
        st.sidebar.header('Filters')
        
        # Test description filter
        test_descriptions = sorted(df['Test description'].unique())
        selected_test = st.sidebar.selectbox(
            'Select Test Description',
            options=test_descriptions
        )
        
        # Main content
        st.title('Quality Control Dashboard')
        
        # Filter data for selected test
        test_data = df[df['Test description'] == selected_test].copy()
        
        if len(test_data) > 0:
            # Create line chart
            fig = go.Figure()
            
            # Add actual results
            fig.add_trace(go.Scatter(
                x=test_data['warehouse_date'],
                y=test_data['Actual result'],
                mode='lines+markers',
                name='Actual Result'
            ))
            
            # Add limit lines if they exist
            if 'Lower limit' in test_data.columns and 'Upper limit' in test_data.columns:
                lsl = test_data['Lower limit'].iloc[0]
                usl = test_data['Upper limit'].iloc[0]
                
                if pd.notnull(lsl):
                    fig.add_hline(y=lsl, line_dash="dash", line_color="red", name="LSL")
                if pd.notnull(usl):
                    fig.add_hline(y=usl, line_dash="dash", line_color="red", name="USL")
            
            fig.update_layout(
                title=f"{selected_test} Results Over Time",
                xaxis_title="Date",
                yaxis_title="Result",
                showlegend=True
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
    else:
        st.error("Unable to load data. Please check your configuration.")
        
except Exception as e:
    st.error(f"Error in application: {str(e)}")
