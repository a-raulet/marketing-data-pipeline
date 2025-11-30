"""
Marketing Analytics Dashboard
Visualizes marketing performance data from BigQuery
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables (local only)
load_dotenv()

# Page config
st.set_page_config(
    page_title="Marketing Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
    }
    .stPlotlyChart {
        background-color: white;
        border-radius: 10px;
        padding: 10px;
    }
    </style>
    """, unsafe_allow_html=True)


@st.cache_resource
def get_bigquery_client():
    """Initialize BigQuery client with credentials"""
    try:
        # Check if running on Streamlit Cloud by trying to access secrets
        try:
            # This will only work on Streamlit Cloud
            _ = st.secrets
            # If we get here, we're on Streamlit Cloud
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            project_id = st.secrets["GCP_PROJECT_ID"]
            st.sidebar.success("🌐 Running on Streamlit Cloud")
        except (FileNotFoundError, KeyError):
            # We're running locally
            if not os.path.exists('credentials.json'):
                raise FileNotFoundError("credentials.json not found. Please add it to the project root.")
            
            credentials = service_account.Credentials.from_service_account_file(
                'credentials.json'
            )
            project_id = os.getenv('GCP_PROJECT_ID')
            st.sidebar.info("💻 Running locally")
        
        return bigquery.Client(credentials=credentials, project=project_id)
    except Exception as e:
        st.error(f"Error connecting to BigQuery: {e}")
        return None


def get_project_id():
    """Get project ID from secrets or env"""
    try:
        return st.secrets["GCP_PROJECT_ID"]
    except:
        return os.getenv('GCP_PROJECT_ID')


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_marketing_data(_client, start_date=None, end_date=None):
    """Load marketing data from BigQuery"""
    if _client is None:
        return pd.DataFrame()
    
    project_id = get_project_id()
    
    query = f"""
    SELECT 
        date,
        marketing_source,
        spend,
        sessions,
        conversions,
        revenue
    FROM `{project_id}.marketing_analytics.stg_marketing_daily`
    WHERE date >= @start_date
      AND date <= @end_date
    ORDER BY date DESC
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )
    
    try:
        df = _client.query(query, job_config=job_config).to_dataframe()
        df['date'] = pd.to_datetime(df['date'])
        
        # Convert marketing_source to string if it's not already
        if 'marketing_source' in df.columns:
            df['marketing_source'] = df['marketing_source'].astype(str)
        
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()


def display_kpis(df):
    """Display key performance indicators"""
    col1, col2, col3, col4 = st.columns(4)
    
    total_spend = df['spend'].sum()
    total_revenue = df['revenue'].sum()
    total_conversions = df['conversions'].sum()
    total_sessions = df['sessions'].sum()
    
    # Calculate metrics
    avg_roas = (total_revenue / total_spend) if total_spend > 0 else 0
    avg_roi = ((total_revenue - total_spend) / total_spend * 100) if total_spend > 0 else 0
    conversion_rate = (total_conversions / total_sessions * 100) if total_sessions > 0 else 0
    
    with col1:
        st.metric(
            label="💰 Total Spend",
            value=f"${total_spend:,.0f}",
            delta=None
        )
    
    with col2:
        st.metric(
            label="📈 Total Revenue",
            value=f"${total_revenue:,.0f}",
            delta=f"{avg_roi:.1f}% ROI"
        )
    
    with col3:
        st.metric(
            label="🎯 Conversions",
            value=f"{total_conversions:,.0f}",
            delta=f"{conversion_rate:.2f}% CVR"
        )
    
    with col4:
        st.metric(
            label="📊 ROAS",
            value=f"{avg_roas:.2f}x",
            delta=None
        )


def plot_daily_trend(df):
    """Plot daily performance trends"""
    daily_agg = df.groupby('date').agg({
        'spend': 'sum',
        'revenue': 'sum',
        'conversions': 'sum'
    }).reset_index()
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=daily_agg['date'],
        y=daily_agg['spend'],
        name='Spend',
        mode='lines+markers',
        line=dict(color='#FF6B6B', width=2),
        marker=dict(size=6)
    ))
    
    fig.add_trace(go.Scatter(
        x=daily_agg['date'],
        y=daily_agg['revenue'],
        name='Revenue',
        mode='lines+markers',
        line=dict(color='#4ECDC4', width=2),
        marker=dict(size=6),
        yaxis='y2'
    ))
    
    fig.update_layout(
        title='Daily Spend vs Revenue Trend',
        xaxis_title='Date',
        yaxis_title='Spend ($)',
        yaxis2=dict(
            title='Revenue ($)',
            overlaying='y',
            side='right'
        ),
        hovermode='x unified',
        height=400,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig


def plot_channel_performance(df):
    """Plot performance by source"""
    source_agg = df.groupby('marketing_source').agg({
        'spend': 'sum',
        'revenue': 'sum',
        'conversions': 'sum',
        'sessions': 'sum'
    }).reset_index()
    
    source_agg['roas'] = source_agg['revenue'] / source_agg['spend']
    source_agg = source_agg.sort_values('revenue', ascending=True)
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=source_agg['marketing_source'],
        x=source_agg['spend'],
        name='Spend',
        orientation='h',
        marker=dict(color='#FF6B6B'),
        text=source_agg['spend'].apply(lambda x: f'${x:,.0f}'),
        textposition='auto'
    ))
    
    fig.add_trace(go.Bar(
        y=source_agg['marketing_source'],
        x=source_agg['revenue'],
        name='Revenue',
        orientation='h',
        marker=dict(color='#4ECDC4'),
        text=source_agg['revenue'].apply(lambda x: f'${x:,.0f}'),
        textposition='auto'
    ))
    
    fig.update_layout(
        title='Source Performance: Spend vs Revenue',
        xaxis_title='Amount ($)',
        yaxis_title='Source',
        barmode='group',
        height=400,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig


def plot_roas_by_source(df):
    """Plot ROAS by source"""
    source_agg = df.groupby('marketing_source').agg({
        'spend': 'sum',
        'revenue': 'sum'
    }).reset_index()
    
    source_agg['roas'] = source_agg['revenue'] / source_agg['spend']
    source_agg = source_agg.sort_values('roas', ascending=True)
    
    # Color code: green if ROAS > 2, yellow if > 1, red otherwise
    colors = ['#FF6B6B' if x < 1 else '#FFD93D' if x < 2 else '#6BCF7F' 
              for x in source_agg['roas']]
    
    fig = go.Figure(go.Bar(
        y=source_agg['marketing_source'],
        x=source_agg['roas'],
        orientation='h',
        marker=dict(color=colors),
        text=source_agg['roas'].apply(lambda x: f'{x:.2f}x'),
        textposition='auto'
    ))
    
    fig.update_layout(
        title='ROAS by Source (Target: >2.0x)',
        xaxis_title='ROAS (Revenue / Spend)',
        yaxis_title='Source',
        height=400,
        showlegend=False
    )
    
    # Add target line at 2.0x
    fig.add_vline(x=2.0, line_dash="dash", line_color="gray", 
                  annotation_text="Target: 2.0x")
    
    return fig


def plot_conversion_funnel(df):
    """Plot conversion funnel metrics"""
    funnel_data = {
        'Sessions': df['sessions'].sum(),
        'Conversions': df['conversions'].sum()
    }
    
    fig = go.Figure(go.Funnel(
        y=list(funnel_data.keys()),
        x=list(funnel_data.values()),
        textinfo="value+percent initial",
        marker=dict(color=['#4ECDC4', '#3A9E99'])
    ))
    
    fig.update_layout(
        title='Marketing Funnel Performance',
        height=400
    )
    
    return fig


def main():
    """Main dashboard function"""
    st.title("📊 Marketing Analytics Dashboard")
    st.markdown("Real-time insights from your marketing campaigns")
    
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    # Initialize BigQuery client
    client = get_bigquery_client()
    
    if client is None:
        st.error("Unable to connect to BigQuery. Please check your credentials.")
        return
    
    # Date range selector
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date",
            value=datetime(2024, 9, 1)
        )
    with col2:
        end_date = st.date_input(
            "End Date",
            value=datetime(2024, 11, 15)
        )
    
    # Load data
    with st.spinner("Loading data from BigQuery..."):
        df = load_marketing_data(client, start_date, end_date)
    
    if df.empty:
        st.warning("No data available for the selected date range.")
        return
    
    # Display summary stats in sidebar
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📈 Summary Stats")
    st.sidebar.metric("Total Records", f"{len(df):,}")
    st.sidebar.metric("Date Range", f"{len(df['date'].unique())} days")
    st.sidebar.metric("Sources", f"{df['marketing_source'].nunique()}")
    
    # Source filter
    sources = ['All'] + sorted(df['marketing_source'].unique().tolist())
    selected_source = st.sidebar.selectbox("Select Source", sources)
    
    if selected_source != 'All':
        df = df[df['marketing_source'] == selected_source]
    
    # Main dashboard
    st.markdown("---")
    
    # KPIs
    st.subheader("📊 Key Performance Indicators")
    display_kpis(df)
    
    st.markdown("---")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.plotly_chart(plot_daily_trend(df), use_container_width=True)
        st.plotly_chart(plot_roas_by_source(df), use_container_width=True)
    
    with col2:
        st.plotly_chart(plot_channel_performance(df), use_container_width=True)
        st.plotly_chart(plot_conversion_funnel(df), use_container_width=True)
    
    # Data table
    st.markdown("---")
    st.subheader("📋 Raw Data")
    st.dataframe(
        df.sort_values('date', ascending=False),
        use_container_width=True,
        height=400
    )
    
    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: gray;'>"
        "Last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") +
        "</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()