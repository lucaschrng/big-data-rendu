import json
from io import BytesIO
import os
import time

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
import requests

from flows.config import BUCKET_GOLD, get_minio_client

# ============================================================================
# CONFIGURATION & SETUP
# ============================================================================
st.set_page_config(
    page_title="Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

API_URL = os.getenv("API_URL", "http://localhost:8000")

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

@st.cache_data(ttl=300)
def load_csv_from_gold(object_name: str) -> pd.DataFrame:
    """Load CSV from Gold bucket with caching."""
    try:
        client = get_minio_client()
        response = client.get_object(BUCKET_GOLD, object_name)
        data = response.read()
        response.close()
        response.release_conn()
        return pd.read_csv(BytesIO(data))
    except Exception as e:
        st.error(f"Error loading {object_name}: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_json_from_gold(object_name: str) -> dict:
    """Load JSON from Gold bucket with caching."""
    try:
        client = get_minio_client()
        response = client.get_object(BUCKET_GOLD, object_name)
        data = response.read()
        response.close()
        response.release_conn()
        return json.loads(data)
    except Exception as e:
        st.error(f"Error loading {object_name}: {str(e)}")
        return {}

def fetch_api_data(endpoint, params=None):
    """Fetch data from FastAPI."""
    try:
        response = requests.get(f"{API_URL}/{endpoint}", params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Error fetching data from {endpoint}: {e}")
        return None

def format_currency(value):
    return f"${value:,.2f}"

def format_percentage(value):
    return f"{value:+.2f}%" if pd.notna(value) else "N/A"

def format_number(value):
    return f"{int(value):,}" if pd.notna(value) else "N/A"

# ============================================================================
# SIDEBAR & MODE SELECTION
# ============================================================================
st.sidebar.markdown("# 📊 Analytics Hub")
st.sidebar.markdown("---")

dashboard_mode = st.sidebar.selectbox(
    "Select Data Source",
    ["Data Lake (Historical/MinIO)", "Operational (Live/MongoDB)"],
    index=0,
    help="Choose between historical batch data (MinIO) or operational live data (MongoDB via API)"
)

st.sidebar.markdown("---")

# ============================================================================
# DATA LAKE DASHBOARD (MinIO)
# ============================================================================
if dashboard_mode == "Data Lake (Historical/MinIO)":
    st.sidebar.markdown("### 🗄️ Data Lake Navigation")
    
    page = st.sidebar.radio(
        "Go to",
        ["🏠 Overview", "📈 Temporal Analysis", "👥 Client Analytics", 
         "📦 Product Analytics", "🌍 Geographic Analysis", "📊 Statistics", "🚀 Benchmark"]
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### About")
    st.sidebar.info(
        "This dashboard displays business metrics and KPIs from the Gold layer "
        "of our data lake (Bronze → Silver → Gold)."
    )
    
    # Refresh button
    if st.sidebar.button("🔄 Refresh Data Lake Cache"):
        st.cache_data.clear()
        st.rerun()
        
    # Load data
    @st.cache_data(ttl=300)
    def load_all_datalake_data():
        return {
            'summary': load_json_from_gold('gold_summary.json'),
            'stats': load_json_from_gold('statistical_distributions.json'),
            'client_kpis': load_csv_from_gold('client_kpis.csv'),
            'product_analytics': load_csv_from_gold('product_analytics.csv'),
            'country_analytics': load_csv_from_gold('country_analytics.csv'),
            'daily_sales': load_csv_from_gold('daily_sales.csv'),
            'weekly_sales': load_csv_from_gold('weekly_sales.csv'),
            'monthly_sales': load_csv_from_gold('monthly_sales.csv')
        }
        
    def load_benchmark_results():
        """Load benchmark results from local file."""
        try:
            if os.path.exists("data/benchmark_results.json"):
                with open("data/benchmark_results.json", "r") as f:
                    return json.load(f)
        except Exception:
            pass
        return None

    def load_refresh_benchmark_results():
        """Load refresh benchmark results from local file."""
        try:
            if os.path.exists("data/benchmark_refresh_results.json"):
                with open("data/benchmark_refresh_results.json", "r") as f:
                    return json.load(f)
        except Exception:
            pass
        return None

    data = load_all_datalake_data()
    benchmark_data = load_benchmark_results()
    refresh_data = load_refresh_benchmark_results()

    # ------------------------------------------------------------------------
    # PAGE: OVERVIEW
    # ------------------------------------------------------------------------
    if page == "🏠 Overview":
        st.markdown('<p class="main-header">📊 Business Overview (Data Lake)</p>', unsafe_allow_html=True)
        
        if not data['stats']:
            st.warning("No data available. Please run the Gold aggregation flow first.")
            st.stop()
        
        # Key Metrics
        st.markdown("### 💰 Key Performance Indicators")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_revenue = data['stats']['overall_metrics']['total_revenue']
            st.metric("Total Revenue", format_currency(total_revenue))
        
        with col2:
            total_orders = data['stats']['overall_metrics']['total_orders']
            st.metric("Total Orders", format_number(total_orders))
        
        with col3:
            avg_order_value = data['stats']['overall_metrics']['avg_order_value']
            st.metric("Avg Order Value", format_currency(avg_order_value))
        
        with col4:
            total_customers = data['stats']['overall_metrics']['total_customers']
            st.metric("Total Customers", format_number(total_customers))
        
        st.markdown("---")
        
        # Client Segmentation
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 👥 Client Segmentation")
            if not data['client_kpis'].empty:
                active_clients = data['client_kpis']['is_active'].sum()
                high_value_clients = data['client_kpis']['is_high_value'].sum()
                
                seg_col1, seg_col2 = st.columns(2)
                with seg_col1:
                    st.metric("Active Clients", format_number(active_clients))
                with seg_col2:
                    st.metric("High Value Clients", format_number(high_value_clients))
                
                # Pie chart
                fig = go.Figure(data=[go.Pie(
                    labels=['Active', 'Inactive'],
                    values=[active_clients, len(data['client_kpis']) - active_clients],
                    hole=0.4,
                    marker_colors=['#2ecc71', '#e74c3c']
                )])
                fig.update_layout(title="Client Activity Status", height=300)
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("### 🏆 Top Performers")
            if data['summary']:
                top_perf = data['summary'].get('top_performers', {})
                st.markdown(f"**🥇 Best Product:** {top_perf.get('top_product_by_revenue', 'N/A')}")
                st.markdown(f"**🌍 Best Country:** {top_perf.get('top_country_by_revenue', 'N/A')}")
                st.markdown(f"**👤 Top Client:** {top_perf.get('top_client_by_spending', 'N/A')}")
                
                if not data['product_analytics'].empty:
                    fig = px.bar(
                        data['product_analytics'].head(5),
                        x='product',
                        y='total_revenue',
                        title="Top 5 Products by Revenue",
                        color='total_revenue',
                        color_continuous_scale='Blues'
                    )
                    fig.update_layout(height=300, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Recent trends
        st.markdown("### 📈 Recent Trends (Last 30 Days)")
        
        if not data['daily_sales'].empty:
            daily_df = data['daily_sales'].copy()
            daily_df['date'] = pd.to_datetime(daily_df['date'])
            daily_df = daily_df.sort_values('date').tail(30)
            
            fig = make_subplots(rows=1, cols=2, subplot_titles=("Daily Revenue", "Daily Orders"))
            fig.add_trace(go.Scatter(x=daily_df['date'], y=daily_df['total_revenue'], mode='lines+markers', name='Revenue', line=dict(color='#1f77b4', width=2)), row=1, col=1)
            fig.add_trace(go.Scatter(x=daily_df['date'], y=daily_df['total_orders'], mode='lines+markers', name='Orders', line=dict(color='#ff7f0e', width=2)), row=1, col=2)
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------------
    # PAGE: TEMPORAL ANALYSIS
    # ------------------------------------------------------------------------
    elif page == "📈 Temporal Analysis":
        st.markdown('<p class="main-header">📈 Temporal Analysis</p>', unsafe_allow_html=True)
        
        granularity = st.radio("Select Time Granularity", ["Daily", "Weekly", "Monthly"], horizontal=True)
        
        if granularity == "Daily":
            df = data['daily_sales'].copy()
            df['date'] = pd.to_datetime(df['date'])
            x_col = 'date'
        elif granularity == "Weekly":
            df = data['weekly_sales'].copy()
            x_col = 'year_week'
        else:
            df = data['monthly_sales'].copy()
            x_col = 'year_month'
        
        if df.empty:
            st.warning("No data available.")
            st.stop()
        
        st.markdown(f"### 💰 {granularity} Revenue Trend")
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df[x_col], y=df['total_revenue'],
            mode='lines+markers', name='Revenue',
            line=dict(color='#2ecc71', width=3), fill='tozeroy',
            fillcolor='rgba(46, 204, 113, 0.1)'
        ))
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown(f"### 📋 {granularity} Data Table")
        st.dataframe(df, use_container_width=True)

    # ------------------------------------------------------------------------
    # PAGE: CLIENT ANALYTICS
    # ------------------------------------------------------------------------
    elif page == "👥 Client Analytics":
        st.markdown('<p class="main-header">👥 Client Analytics</p>', unsafe_allow_html=True)
        
        if data['client_kpis'].empty:
            st.warning("No client data available.")
            st.stop()
        
        df = data['client_kpis'].copy()
        
        col1, col2, col3, col4 = st.columns(4)
        with col1: st.metric("Total Clients", format_number(len(df)))
        with col2: st.metric("Active Clients", format_number(df['is_active'].sum()))
        with col3: st.metric("Avg LTV", format_currency(df['total_spent'].mean()))
        with col4: st.metric("Avg Frequency", f"{df['purchase_frequency_per_month'].mean():.2f}/mo")
        
        st.markdown("### 🏆 Top 10 Clients by Spending")
        st.dataframe(df.nlargest(10, 'total_spent'), use_container_width=True)

    # ------------------------------------------------------------------------
    # PAGE: PRODUCT ANALYTICS
    # ------------------------------------------------------------------------
    elif page == "📦 Product Analytics":
        st.markdown('<p class="main-header">📦 Product Analytics</p>', unsafe_allow_html=True)
        
        if data['product_analytics'].empty:
            st.warning("No product data available.")
            st.stop()
        
        df = data['product_analytics'].copy()
        
        fig = px.bar(
            df.sort_values('total_revenue', ascending=True),
            y='product', x='total_revenue', orientation='h',
            title="Total Revenue by Product", color='total_revenue'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(df, use_container_width=True)

    # ------------------------------------------------------------------------
    # PAGE: GEOGRAPHIC ANALYSIS
    # ------------------------------------------------------------------------
    elif page == "🌍 Geographic Analysis":
        st.markdown('<p class="main-header">🌍 Geographic Analysis</p>', unsafe_allow_html=True)
        
        if data['country_analytics'].empty:
            st.warning("No country data available.")
            st.stop()
        
        df = data['country_analytics'].copy()
        
        fig = px.bar(
            df.sort_values('total_revenue', ascending=True),
            y='country', x='total_revenue', orientation='h',
            title="Total Revenue by Country", color='total_revenue'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(df, use_container_width=True)

    # ------------------------------------------------------------------------
    # PAGE: STATISTICS
    # ------------------------------------------------------------------------
    elif page == "📊 Statistics":
        st.markdown('<p class="main-header">📊 Statistical Distributions</p>', unsafe_allow_html=True)
        
        if not data['stats']:
            st.warning("No statistics available.")
            st.stop()
        
        st.json(data['stats'])

    # ------------------------------------------------------------------------
    # PAGE: BENCHMARK
    # ------------------------------------------------------------------------
    elif page == "🚀 Benchmark":
        st.markdown('<p class="main-header">🚀 Performance Benchmark</p>', unsafe_allow_html=True)
        
        tab1, tab2 = st.tabs(["Processing Engines (Pandas vs Spark)", "Refresh Performance (Gold -> Mongo)"])
        
        with tab1:
            st.subheader("Processing Engines Comparison")
            if not benchmark_data:
                st.warning("No benchmark data available. Please run `python flows/benchmark.py` to generate results.")
            else:
                # Header metrics
                comp = benchmark_data['comparison']
                winner = comp['winner'].upper()
                winner_color = "green" if winner == "PANDAS" else "blue"
                
                st.markdown(f"### 🏆 Winner: :{winner_color}[{winner}]")
                st.info(f"**Reason:** {comp['winner_reason']}")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Speedup", f"{comp['total_speedup']:.2f}x")
                with col2:
                    st.metric("Pandas Total Time", f"{benchmark_data['pandas']['timing']['total']:.2f}s")
                with col3:
                    st.metric("Spark Total Time", f"{benchmark_data['spark']['timing']['total']:.2f}s")
                    
                st.markdown("---")
                
                # Detailed Timing Comparison
                st.markdown("### ⏱️ Execution Time by Step")
                
                steps = ['bronze_ingestion', 'silver_transformation', 'gold_aggregation']
                step_labels = ['Bronze Ingestion', 'Silver Transformation', 'Gold Aggregation']
                
                pandas_times = [benchmark_data['pandas']['timing'].get(s, 0) for s in steps]
                spark_times = [benchmark_data['spark']['timing'].get(s, 0) for s in steps]
                
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    name='Pandas',
                    x=step_labels,
                    y=pandas_times,
                    text=[f"{t:.2f}s" for t in pandas_times],
                    textposition='auto',
                    marker_color='#2ecc71'
                ))
                fig.add_trace(go.Bar(
                    name='Spark',
                    x=step_labels,
                    y=spark_times,
                    text=[f"{t:.2f}s" for t in spark_times],
                    textposition='auto',
                    marker_color='#3498db'
                ))
                
                fig.update_layout(
                    barmode='group',
                    title="Processing Time per Layer (Lower is Better)",
                    yaxis_title="Time (seconds)",
                    height=500
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Data Quality Score
                st.markdown("### ✅ Data Quality")
                
                q_col1, q_col2 = st.columns(2)
                with q_col1:
                    st.metric("Pandas Quality Score", f"{benchmark_data['pandas']['data_quality_score']}%")
                with q_col2:
                    st.metric("Spark Quality Score", f"{benchmark_data['spark']['data_quality_score']}%")
                    
                # Raw Data
                with st.expander("See Raw Benchmark Data"):
                    st.json(benchmark_data)

        with tab2:
            st.subheader("Refresh Cycle Performance")
            st.markdown("Benchmark of the operational refresh pipeline: **Gold Aggregation (Pandas) → Parquet → MongoDB Load**")
            
            if not refresh_data:
                st.warning("No refresh benchmark data available. Please run `python flows/benchmark_refresh.py`.")
            else:
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("⚡ Total Refresh Time", f"{refresh_data['total_time']:.4f}s")
                with col2:
                    st.metric("Gold Generation", f"{refresh_data['gold_duration']:.4f}s")
                with col3:
                    st.metric("ETL to Mongo", f"{refresh_data['etl_duration']:.4f}s")
                
                # Waterfall chart for breakdown
                fig = go.Figure(go.Waterfall(
                    name = "Refresh Cycle",
                    orientation = "v",
                    measure = ["relative", "relative", "total"],
                    x = ["Gold Generation", "ETL to Mongo", "Total Time"],
                    textposition = "outside",
                    text = [f"{refresh_data['gold_duration']:.2f}s", f"{refresh_data['etl_duration']:.2f}s", f"{refresh_data['total_time']:.2f}s"],
                    y = [refresh_data['gold_duration'], refresh_data['etl_duration'], 0],
                    connector = {"line":{"color":"rgb(63, 63, 63)"}},
                ))

                fig.update_layout(
                    title = "Refresh Cycle Breakdown",
                    showlegend = False,
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
                
                st.success(f"Last updated: {refresh_data.get('timestamp', 'N/A')}")
                
                with st.expander("See Raw Refresh Data"):
                    st.json(refresh_data)

# ============================================================================
# OPERATIONAL DASHBOARD (API)
# ============================================================================
else:
    st.sidebar.markdown("### ⚡ Live API Navigation")
    page = st.sidebar.radio("Go to", ["Overview", "Sales Analysis", "Product Performance", "Client Insights"])
    
    st.sidebar.markdown("---")
    if st.sidebar.button("🔄 Refresh API Data"):
        st.cache_data.clear()
        st.rerun()

    st.title("⚡ Operational Dashboard")
    st.markdown(f"Data source: MongoDB via FastAPI ({API_URL})")

    # ------------------------------------------------------------------------
    # PAGE: OVERVIEW (API)
    # ------------------------------------------------------------------------
    if page == "Overview":
        st.header("Global KPI Overview")
        
        kpis = fetch_api_data("kpis/global")
        
        if kpis:
            col1, col2, col3, col4 = st.columns(4)
            with col1: st.metric("Total Revenue", f"${kpis['total_revenue']:,.2f}")
            with col2: st.metric("Total Orders", f"{kpis['total_orders']:,}")
            with col3: st.metric("Active Customers", f"{kpis['total_customers']:,}")
            with col4: st.metric("Avg Order Value", f"${kpis['avg_order_value']:.2f}")
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Daily Sales Trend")
            daily_data = fetch_api_data("sales/temporal", {"period": "daily"})
            if daily_data:
                df_daily = pd.DataFrame(daily_data)
                fig = px.line(df_daily, x="date", y="total_revenue", title="Revenue over Time")
                st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.subheader("Top Countries by Revenue")
            country_data = fetch_api_data("countries/analytics")
            if country_data:
                df_country = pd.DataFrame(country_data).head(10)
                fig = px.bar(df_country, x="country", y="total_revenue", title="Revenue by Country")
                st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------------
    # PAGE: SALES ANALYSIS (API)
    # ------------------------------------------------------------------------
    elif page == "Sales Analysis":
        st.header("Sales Performance")
        tab1, tab2, tab3 = st.tabs(["Daily", "Weekly", "Monthly"])
        
        with tab1:
            daily_data = fetch_api_data("sales/temporal", {"period": "daily"})
            if daily_data:
                df = pd.DataFrame(daily_data)
                st.dataframe(df.tail(10), use_container_width=True)
                fig = px.line(df, x="date", y="total_revenue", title="Daily Revenue")
                st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            weekly_data = fetch_api_data("sales/temporal", {"period": "weekly"})
            if weekly_data:
                df = pd.DataFrame(weekly_data)
                fig = px.bar(df, x="year_week", y="total_revenue", title="Weekly Revenue", color="revenue_growth_pct")
                st.plotly_chart(fig, use_container_width=True)

        with tab3:
            monthly_data = fetch_api_data("sales/temporal", {"period": "monthly"})
            if monthly_data:
                df = pd.DataFrame(monthly_data)
                fig = px.bar(df, x="year_month", y="total_revenue", title="Monthly Revenue", text_auto='.2s')
                st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------------
    # PAGE: PRODUCT PERFORMANCE (API)
    # ------------------------------------------------------------------------
    elif page == "Product Performance":
        st.header("Product Analytics")
        product_data = fetch_api_data("products/analytics", {"limit": 20})
        if product_data:
            df = pd.DataFrame(product_data)
            col1, col2 = st.columns([2, 1])
            with col1:
                fig = px.bar(df, x="product", y="total_revenue", title="Revenue by Product", color="total_quantity_sold")
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = px.pie(df, names="product", values="total_orders", title="Order Distribution")
                st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True)

    # ------------------------------------------------------------------------
    # PAGE: CLIENT INSIGHTS (API)
    # ------------------------------------------------------------------------
    elif page == "Client Insights":
        st.header("Client Segmentation")
        dist_data = fetch_api_data("clients/distribution")
        
        if dist_data:
            col1, col2 = st.columns(2)
            with col1:
                fig = go.Figure(data=[go.Pie(labels=['Active', 'Inactive'], values=[dist_data['active'], dist_data['inactive']], hole=.3)])
                fig.update_layout(title_text="Active vs Inactive Clients")
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = go.Figure(data=[go.Pie(labels=['High Value', 'Standard Value'], values=[dist_data['high_value'], dist_data['standard_value']], hole=.3)])
                fig.update_layout(title_text="Client Value Segmentation")
                st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Top High Value Clients")
        top_clients = fetch_api_data("clients/top", {"limit": 20})
        if top_clients:
            st.dataframe(pd.DataFrame(top_clients), use_container_width=True)

# Footer
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #7f8c8d;'>"
    "📊 Analytics Hub | Architecture: Data Lake (MinIO) + Operational NoSQL (MongoDB)"
    "</div>",
    unsafe_allow_html=True
)
