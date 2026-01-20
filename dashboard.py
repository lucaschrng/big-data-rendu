import json
from io import BytesIO

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from flows.config import BUCKET_GOLD, get_minio_client


# Page configuration
st.set_page_config(
    page_title="Gold Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

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


def format_currency(value):
    """Format value as currency."""
    return f"${value:,.2f}"


def format_percentage(value):
    """Format value as percentage."""
    return f"{value:+.2f}%" if pd.notna(value) else "N/A"


def format_number(value):
    """Format value as number with thousands separator."""
    return f"{int(value):,}" if pd.notna(value) else "N/A"


# Sidebar
st.sidebar.markdown("# 📊 Gold Analytics")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigation",
    ["🏠 Overview", "📈 Temporal Analysis", "👥 Client Analytics", 
     "📦 Product Analytics", "🌍 Geographic Analysis", "📊 Statistics"]
)

st.sidebar.markdown("---")
st.sidebar.markdown("### About")
st.sidebar.info(
    "This dashboard displays business metrics and KPIs from the Gold layer "
    "of our data lake (Bronze → Silver → Gold)."
)

# Refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()


# Load data
@st.cache_data(ttl=300)
def load_all_data():
    """Load all Gold data."""
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


data = load_all_data()


# ============================================================================
# PAGE: OVERVIEW
# ============================================================================
if page == "🏠 Overview":
    st.markdown('<p class="main-header">📊 Business Overview</p>', unsafe_allow_html=True)
    
    if not data['stats']:
        st.warning("No data available. Please run the Gold aggregation flow first.")
        st.stop()
    
    # Key Metrics
    st.markdown("### 💰 Key Performance Indicators")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_revenue = data['stats']['overall_metrics']['total_revenue']
        st.metric(
            "Total Revenue",
            format_currency(total_revenue),
            help="Total revenue across all sales"
        )
    
    with col2:
        total_orders = data['stats']['overall_metrics']['total_orders']
        st.metric(
            "Total Orders",
            format_number(total_orders),
            help="Total number of orders"
        )
    
    with col3:
        avg_order_value = data['stats']['overall_metrics']['avg_order_value']
        st.metric(
            "Avg Order Value",
            format_currency(avg_order_value),
            help="Average value per order"
        )
    
    with col4:
        total_customers = data['stats']['overall_metrics']['total_customers']
        st.metric(
            "Total Customers",
            format_number(total_customers),
            help="Unique customers"
        )
    
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
            fig.update_layout(
                title="Client Activity Status",
                height=300,
                showlegend=True
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### 🏆 Top Performers")
        if data['summary']:
            top_perf = data['summary'].get('top_performers', {})
            
            st.markdown(f"**🥇 Best Product:** {top_perf.get('top_product_by_revenue', 'N/A')}")
            st.markdown(f"**🌍 Best Country:** {top_perf.get('top_country_by_revenue', 'N/A')}")
            st.markdown(f"**👤 Top Client:** {top_perf.get('top_client_by_spending', 'N/A')}")
            
            # Product revenue distribution
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
        
        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=("Daily Revenue", "Daily Orders")
        )
        
        fig.add_trace(
            go.Scatter(
                x=daily_df['date'],
                y=daily_df['total_revenue'],
                mode='lines+markers',
                name='Revenue',
                line=dict(color='#1f77b4', width=2)
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=daily_df['date'],
                y=daily_df['total_orders'],
                mode='lines+markers',
                name='Orders',
                line=dict(color='#ff7f0e', width=2)
            ),
            row=1, col=2
        )
        
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)


# ============================================================================
# PAGE: TEMPORAL ANALYSIS
# ============================================================================
elif page == "📈 Temporal Analysis":
    st.markdown('<p class="main-header">📈 Temporal Analysis</p>', unsafe_allow_html=True)
    
    # Granularity selector
    granularity = st.radio(
        "Select Time Granularity",
        ["Daily", "Weekly", "Monthly"],
        horizontal=True
    )
    
    # Load appropriate data
    if granularity == "Daily":
        df = data['daily_sales'].copy()
        df['date'] = pd.to_datetime(df['date'])
        x_col = 'date'
        title_suffix = "Daily"
    elif granularity == "Weekly":
        df = data['weekly_sales'].copy()
        x_col = 'year_week'
        title_suffix = "Weekly"
    else:
        df = data['monthly_sales'].copy()
        x_col = 'year_month'
        title_suffix = "Monthly"
    
    if df.empty:
        st.warning("No data available.")
        st.stop()
    
    # Revenue trend
    st.markdown(f"### 💰 {title_suffix} Revenue Trend")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df[x_col],
        y=df['total_revenue'],
        mode='lines+markers',
        name='Revenue',
        line=dict(color='#2ecc71', width=3),
        fill='tozeroy',
        fillcolor='rgba(46, 204, 113, 0.1)'
    ))
    
    if granularity == "Daily" and 'revenue_7day_avg' in df.columns:
        fig.add_trace(go.Scatter(
            x=df[x_col],
            y=df['revenue_7day_avg'],
            mode='lines',
            name='7-Day Avg',
            line=dict(color='#e74c3c', width=2, dash='dash')
        ))
    
    fig.update_layout(
        height=400,
        xaxis_title="Period",
        yaxis_title="Revenue ($)",
        hovermode='x unified'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Growth metrics
    st.markdown(f"### 📊 {title_suffix} Growth Metrics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Revenue growth
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df[x_col],
            y=df['revenue_growth_pct'],
            marker_color=df['revenue_growth_pct'].apply(
                lambda x: '#2ecc71' if x >= 0 else '#e74c3c'
            ),
            name='Revenue Growth %'
        ))
        fig.update_layout(
            title="Revenue Growth (%)",
            height=350,
            yaxis_title="Growth %",
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Orders and customers
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig.add_trace(
            go.Bar(
                x=df[x_col],
                y=df['total_orders'],
                name='Orders',
                marker_color='#3498db'
            ),
            secondary_y=False
        )
        
        fig.add_trace(
            go.Scatter(
                x=df[x_col],
                y=df['unique_customers'],
                name='Customers',
                mode='lines+markers',
                line=dict(color='#e74c3c', width=2)
            ),
            secondary_y=True
        )
        
        fig.update_layout(
            title="Orders vs Customers",
            height=350
        )
        fig.update_yaxes(title_text="Orders", secondary_y=False)
        fig.update_yaxes(title_text="Customers", secondary_y=True)
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Data table
    st.markdown(f"### 📋 {title_suffix} Data Table")
    st.dataframe(
        df.style.format({
            'total_revenue': '${:,.2f}',
            'revenue_growth_pct': '{:+.2f}%',
            'avg_order_value': '${:,.2f}' if 'avg_order_value' in df.columns else None
        }),
        use_container_width=True,
        height=400
    )


# ============================================================================
# PAGE: CLIENT ANALYTICS
# ============================================================================
elif page == "👥 Client Analytics":
    st.markdown('<p class="main-header">👥 Client Analytics</p>', unsafe_allow_html=True)
    
    if data['client_kpis'].empty:
        st.warning("No client data available.")
        st.stop()
    
    df = data['client_kpis'].copy()
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Clients", format_number(len(df)))
    with col2:
        st.metric("Active Clients", format_number(df['is_active'].sum()))
    with col3:
        avg_ltv = df['total_spent'].mean()
        st.metric("Avg LTV", format_currency(avg_ltv))
    with col4:
        avg_freq = df['purchase_frequency_per_month'].mean()
        st.metric("Avg Frequency", f"{avg_freq:.2f}/mo")
    
    st.markdown("---")
    
    # RFM Analysis
    st.markdown("### 📊 RFM Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Recency distribution
        fig = px.histogram(
            df,
            x='days_since_last_purchase',
            nbins=30,
            title="Recency Distribution (Days Since Last Purchase)",
            color_discrete_sequence=['#3498db']
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Frequency distribution
        fig = px.histogram(
            df,
            x='total_purchases',
            nbins=20,
            title="Frequency Distribution (Total Purchases)",
            color_discrete_sequence=['#2ecc71']
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    # Monetary analysis
    st.markdown("### 💰 Monetary Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Total spent distribution
        fig = px.box(
            df,
            y='total_spent',
            title="Total Spent Distribution",
            color_discrete_sequence=['#e74c3c']
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Average order value by segment
        segment_aov = df.groupby('is_high_value')['avg_order_value'].mean().reset_index()
        segment_aov['segment'] = segment_aov['is_high_value'].map({True: 'High Value', False: 'Standard'})
        
        fig = px.bar(
            segment_aov,
            x='segment',
            y='avg_order_value',
            title="Avg Order Value by Segment",
            color='segment',
            color_discrete_map={'High Value': '#f39c12', 'Standard': '#95a5a6'}
        )
        fig.update_layout(height=350, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    # Top clients
    st.markdown("### 🏆 Top 10 Clients by Spending")
    top_clients = df.nlargest(10, 'total_spent')[
        ['name', 'email', 'country', 'total_spent', 'total_purchases', 'avg_order_value']
    ]
    st.dataframe(
        top_clients.style.format({
            'total_spent': '${:,.2f}',
            'avg_order_value': '${:,.2f}'
        }),
        use_container_width=True
    )


# ============================================================================
# PAGE: PRODUCT ANALYTICS
# ============================================================================
elif page == "📦 Product Analytics":
    st.markdown('<p class="main-header">📦 Product Analytics</p>', unsafe_allow_html=True)
    
    if data['product_analytics'].empty:
        st.warning("No product data available.")
        st.stop()
    
    df = data['product_analytics'].copy()
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Products", format_number(len(df)))
    with col2:
        total_revenue = df['total_revenue'].sum()
        st.metric("Total Revenue", format_currency(total_revenue))
    with col3:
        total_qty = df['total_quantity_sold'].sum()
        st.metric("Units Sold", format_number(total_qty))
    with col4:
        avg_price = df['avg_price'].mean()
        st.metric("Avg Price", format_currency(avg_price))
    
    st.markdown("---")
    
    # Revenue by product
    st.markdown("### 💰 Revenue by Product")
    fig = px.bar(
        df.sort_values('total_revenue', ascending=True),
        y='product',
        x='total_revenue',
        orientation='h',
        title="Total Revenue by Product",
        color='total_revenue',
        color_continuous_scale='Viridis'
    )
    fig.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
    
    # Product metrics
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📊 Quantity Sold")
        fig = px.pie(
            df,
            values='total_quantity_sold',
            names='product',
            title="Market Share by Quantity"
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### 👥 Unique Customers")
        fig = px.bar(
            df.sort_values('unique_customers', ascending=False),
            x='product',
            y='unique_customers',
            title="Unique Customers per Product",
            color='unique_customers',
            color_continuous_scale='Blues'
        )
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    # Price analysis
    st.markdown("### 💵 Price Analysis")
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df['product'],
        y=df['avg_price'],
        name='Avg Price',
        marker_color='#3498db'
    ))
    fig.add_trace(go.Scatter(
        x=df['product'],
        y=df['max_price'],
        mode='markers',
        name='Max Price',
        marker=dict(size=10, color='#e74c3c', symbol='diamond')
    ))
    fig.add_trace(go.Scatter(
        x=df['product'],
        y=df['min_price'],
        mode='markers',
        name='Min Price',
        marker=dict(size=10, color='#2ecc71', symbol='diamond')
    ))
    
    fig.update_layout(
        title="Price Range by Product",
        height=400,
        yaxis_title="Price ($)"
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Product details table
    st.markdown("### 📋 Product Details")
    st.dataframe(
        df.style.format({
            'total_revenue': '${:,.2f}',
            'avg_price': '${:,.2f}',
            'min_price': '${:,.2f}',
            'max_price': '${:,.2f}',
            'avg_quantity_per_order': '{:.2f}'
        }),
        use_container_width=True
    )


# ============================================================================
# PAGE: GEOGRAPHIC ANALYSIS
# ============================================================================
elif page == "🌍 Geographic Analysis":
    st.markdown('<p class="main-header">🌍 Geographic Analysis</p>', unsafe_allow_html=True)
    
    if data['country_analytics'].empty:
        st.warning("No country data available.")
        st.stop()
    
    df = data['country_analytics'].copy()
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Countries", format_number(len(df)))
    with col2:
        total_revenue = df['total_revenue'].sum()
        st.metric("Total Revenue", format_currency(total_revenue))
    with col3:
        top_country = df.iloc[0]['country']
        st.metric("Top Country", top_country)
    with col4:
        top_share = df.iloc[0]['market_share_pct']
        st.metric("Top Market Share", f"{top_share:.1f}%")
    
    st.markdown("---")
    
    # Revenue by country
    st.markdown("### 💰 Revenue by Country")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            df.sort_values('total_revenue', ascending=True),
            y='country',
            x='total_revenue',
            orientation='h',
            title="Total Revenue by Country",
            color='total_revenue',
            color_continuous_scale='RdYlGn'
        )
        fig.update_layout(height=500, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.pie(
            df,
            values='total_revenue',
            names='country',
            title="Market Share by Revenue"
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    
    # Customer metrics
    st.markdown("### 👥 Customer Metrics by Country")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            df.sort_values('unique_customers', ascending=False),
            x='country',
            y='unique_customers',
            title="Unique Customers by Country",
            color='unique_customers',
            color_continuous_scale='Blues'
        )
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.scatter(
            df,
            x='unique_customers',
            y='avg_revenue_per_customer',
            size='total_revenue',
            color='country',
            title="Customers vs Revenue per Customer",
            hover_data=['total_orders']
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Order metrics
    st.markdown("### 🛒 Order Metrics")
    
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Total Orders", "Average Order Value")
    )
    
    fig.add_trace(
        go.Bar(
            x=df['country'],
            y=df['total_orders'],
            marker_color='#3498db',
            name='Orders'
        ),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Bar(
            x=df['country'],
            y=df['avg_order_value'],
            marker_color='#2ecc71',
            name='AOV'
        ),
        row=1, col=2
    )
    
    fig.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
    
    # Country details table
    st.markdown("### 📋 Country Details")
    st.dataframe(
        df.style.format({
            'total_revenue': '${:,.2f}',
            'avg_order_value': '${:,.2f}',
            'avg_revenue_per_customer': '${:,.2f}',
            'market_share_pct': '{:.2f}%'
        }),
        use_container_width=True
    )


# ============================================================================
# PAGE: STATISTICS
# ============================================================================
elif page == "📊 Statistics":
    st.markdown('<p class="main-header">📊 Statistical Distributions</p>', unsafe_allow_html=True)
    
    if not data['stats']:
        st.warning("No statistics available.")
        st.stop()
    
    stats = data['stats']
    
    # Order value distribution
    st.markdown("### 💰 Order Value Distribution")
    
    order_dist = stats['order_value_distribution']
    
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Mean", format_currency(order_dist['mean']))
    with col2:
        st.metric("Median", format_currency(order_dist['median']))
    with col3:
        st.metric("Std Dev", format_currency(order_dist['std']))
    with col4:
        st.metric("Min", format_currency(order_dist['min']))
    with col5:
        st.metric("Max", format_currency(order_dist['max']))
    
    # Box plot
    quartiles = [
        order_dist['min'],
        order_dist['q25'],
        order_dist['median'],
        order_dist['q75'],
        order_dist['max']
    ]
    
    fig = go.Figure()
    fig.add_trace(go.Box(
        y=quartiles,
        name='Order Value',
        marker_color='#3498db',
        boxmean='sd'
    ))
    fig.update_layout(
        title="Order Value Box Plot",
        height=400,
        yaxis_title="Value ($)"
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Percentiles
    st.markdown("#### Percentiles")
    perc_col1, perc_col2, perc_col3, perc_col4 = st.columns(4)
    with perc_col1:
        st.metric("25th Percentile", format_currency(order_dist['q25']))
    with perc_col2:
        st.metric("50th Percentile", format_currency(order_dist['median']))
    with perc_col3:
        st.metric("75th Percentile", format_currency(order_dist['q75']))
    with perc_col4:
        st.metric("90th Percentile", format_currency(order_dist['q90']))
    
    st.markdown("---")
    
    # Quantity distribution
    st.markdown("### 📦 Quantity Distribution")
    
    qty_dist = stats['quantity_distribution']
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Mean", f"{qty_dist['mean']:.2f}")
    with col2:
        st.metric("Median", format_number(qty_dist['median']))
    with col3:
        st.metric("Mode", format_number(qty_dist['mode']))
    with col4:
        st.metric("Max", format_number(qty_dist['max']))
    
    st.markdown("---")
    
    # Price distribution
    st.markdown("### 💵 Price Distribution")
    
    price_dist = stats['price_distribution']
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Mean", format_currency(price_dist['mean']))
    with col2:
        st.metric("Median", format_currency(price_dist['median']))
    with col3:
        st.metric("Std Dev", format_currency(price_dist['std']))
    with col4:
        st.metric("Range", format_currency(price_dist['max'] - price_dist['min']))
    
    st.markdown("---")
    
    # Overall metrics summary
    st.markdown("### 📈 Overall Metrics Summary")
    
    overall = stats['overall_metrics']
    
    metrics_df = pd.DataFrame({
        'Metric': [
            'Total Revenue',
            'Total Orders',
            'Total Customers',
            'Total Products',
            'Total Countries',
            'Average Order Value',
            'Average Items per Order'
        ],
        'Value': [
            format_currency(overall['total_revenue']),
            format_number(overall['total_orders']),
            format_number(overall['total_customers']),
            format_number(overall['total_products']),
            format_number(overall['total_countries']),
            format_currency(overall['avg_order_value']),
            f"{overall['avg_items_per_order']:.2f}"
        ]
    })
    
    st.dataframe(metrics_df, use_container_width=True, hide_index=True)


# Footer
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #7f8c8d;'>"
    "📊 Gold Analytics Dashboard | Data Lake Architecture (Bronze → Silver → Gold)"
    "</div>",
    unsafe_allow_html=True
)
