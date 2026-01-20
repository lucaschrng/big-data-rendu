import json
from datetime import datetime
from io import BytesIO, StringIO

import pandas as pd
from prefect import flow, task

from config import BUCKET_GOLD, BUCKET_SILVER, get_minio_client


@task(name="read_from_silver", retries=2)
def read_csv_from_silver(object_name: str) -> pd.DataFrame:
    """
    Read CSV data from silver bucket into pandas DataFrame.
    
    Args:
        object_name: Name of object in silver bucket
        
    Returns:
        DataFrame with cleaned data
    """
    client = get_minio_client()
    
    response = client.get_object(BUCKET_SILVER, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    
    df = pd.read_csv(BytesIO(data))
    print(f"Read {len(df)} rows from silver/{object_name}")
    return df


@task(name="create_fact_sales")
def create_fact_sales(purchases_df: pd.DataFrame, clients_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create fact table by joining purchases with client dimensions.
    
    Args:
        purchases_df: Cleaned purchases data
        clients_df: Cleaned clients data
        
    Returns:
        Fact table with denormalized data
    """
    # Join purchases with clients
    fact_sales = purchases_df.merge(
        clients_df[['id_client', 'name', 'email', 'country', 'subscription_date']],
        on='id_client',
        how='left'
    )
    
    # Convert dates to datetime
    fact_sales['purchase_date'] = pd.to_datetime(fact_sales['purchase_date'])
    fact_sales['subscription_date'] = pd.to_datetime(fact_sales['subscription_date'])
    
    # Add temporal dimensions
    fact_sales['purchase_year'] = fact_sales['purchase_date'].dt.year
    fact_sales['purchase_month'] = fact_sales['purchase_date'].dt.month
    fact_sales['purchase_quarter'] = fact_sales['purchase_date'].dt.quarter
    fact_sales['purchase_week'] = fact_sales['purchase_date'].dt.isocalendar().week
    fact_sales['purchase_day_of_week'] = fact_sales['purchase_date'].dt.dayofweek
    fact_sales['purchase_day_name'] = fact_sales['purchase_date'].dt.day_name()
    
    # Add year-month for easy grouping
    fact_sales['year_month'] = fact_sales['purchase_date'].dt.to_period('M').astype(str)
    fact_sales['year_week'] = fact_sales['purchase_date'].dt.to_period('W').astype(str)
    
    # Calculate customer tenure at purchase time
    fact_sales['customer_tenure_days'] = (
        fact_sales['purchase_date'] - fact_sales['subscription_date']
    ).dt.days
    
    print(f"Created fact_sales table with {len(fact_sales)} rows")
    return fact_sales


@task(name="calculate_client_kpis")
def calculate_client_kpis(fact_sales: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate KPIs per client.
    
    Metrics:
    - Total purchases
    - Total spent
    - Average order value
    - First/last purchase dates
    - Customer lifetime value
    - Purchase frequency
    
    Args:
        fact_sales: Fact table with sales data
        
    Returns:
        DataFrame with client KPIs
    """
    client_kpis = fact_sales.groupby('id_client').agg({
        'id_purchase': 'count',
        'total': ['sum', 'mean', 'min', 'max'],
        'quantity': 'sum',
        'purchase_date': ['min', 'max'],
        'name': 'first',
        'email': 'first',
        'country': 'first',
        'subscription_date': 'first'
    }).reset_index()
    
    # Flatten column names
    client_kpis.columns = [
        'id_client', 'total_purchases', 'total_spent', 'avg_order_value',
        'min_order_value', 'max_order_value', 'total_quantity',
        'first_purchase_date', 'last_purchase_date',
        'name', 'email', 'country', 'subscription_date'
    ]
    
    # Calculate customer lifetime (days since first purchase)
    client_kpis['first_purchase_date'] = pd.to_datetime(client_kpis['first_purchase_date'])
    client_kpis['last_purchase_date'] = pd.to_datetime(client_kpis['last_purchase_date'])
    client_kpis['subscription_date'] = pd.to_datetime(client_kpis['subscription_date'])
    
    today = pd.Timestamp.now()
    client_kpis['customer_lifetime_days'] = (
        today - client_kpis['first_purchase_date']
    ).dt.days
    
    # Purchase frequency (purchases per month)
    client_kpis['purchase_frequency_per_month'] = (
        client_kpis['total_purchases'] / 
        (client_kpis['customer_lifetime_days'] / 30).clip(lower=1)
    ).round(2)
    
    # Days since last purchase (recency)
    client_kpis['days_since_last_purchase'] = (
        today - client_kpis['last_purchase_date']
    ).dt.days
    
    # Customer segment based on RFM (simple)
    client_kpis['is_active'] = client_kpis['days_since_last_purchase'] <= 90
    client_kpis['is_high_value'] = client_kpis['total_spent'] > client_kpis['total_spent'].median()
    
    # Round numeric columns
    client_kpis['total_spent'] = client_kpis['total_spent'].round(2)
    client_kpis['avg_order_value'] = client_kpis['avg_order_value'].round(2)
    client_kpis['min_order_value'] = client_kpis['min_order_value'].round(2)
    client_kpis['max_order_value'] = client_kpis['max_order_value'].round(2)
    
    # Convert dates to string for CSV
    client_kpis['first_purchase_date'] = client_kpis['first_purchase_date'].dt.strftime('%Y-%m-%d')
    client_kpis['last_purchase_date'] = client_kpis['last_purchase_date'].dt.strftime('%Y-%m-%d')
    client_kpis['subscription_date'] = client_kpis['subscription_date'].dt.strftime('%Y-%m-%d')
    
    print(f"Calculated KPIs for {len(client_kpis)} clients")
    return client_kpis


@task(name="calculate_product_analytics")
def calculate_product_analytics(fact_sales: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate analytics per product.
    
    Metrics:
    - Total quantity sold
    - Total revenue
    - Average price
    - Number of orders
    - Top countries
    
    Args:
        fact_sales: Fact table with sales data
        
    Returns:
        DataFrame with product analytics
    """
    product_analytics = fact_sales.groupby('product').agg({
        'quantity': 'sum',
        'total': 'sum',
        'price': ['mean', 'min', 'max'],
        'id_purchase': 'count',
        'id_client': 'nunique'
    }).reset_index()
    
    # Flatten column names
    product_analytics.columns = [
        'product', 'total_quantity_sold', 'total_revenue',
        'avg_price', 'min_price', 'max_price',
        'total_orders', 'unique_customers'
    ]
    
    # Calculate average quantity per order
    product_analytics['avg_quantity_per_order'] = (
        product_analytics['total_quantity_sold'] / product_analytics['total_orders']
    ).round(2)
    
    # Round numeric columns
    product_analytics['total_revenue'] = product_analytics['total_revenue'].round(2)
    product_analytics['avg_price'] = product_analytics['avg_price'].round(2)
    product_analytics['min_price'] = product_analytics['min_price'].round(2)
    product_analytics['max_price'] = product_analytics['max_price'].round(2)
    
    # Sort by revenue
    product_analytics = product_analytics.sort_values('total_revenue', ascending=False)
    
    # Add top countries per product
    top_countries = fact_sales.groupby(['product', 'country'])['total'].sum().reset_index()
    top_countries = top_countries.sort_values(['product', 'total'], ascending=[True, False])
    top_countries = top_countries.groupby('product').head(3)
    top_countries_str = top_countries.groupby('product')['country'].apply(
        lambda x: ', '.join(x)
    ).reset_index()
    top_countries_str.columns = ['product', 'top_countries']
    
    product_analytics = product_analytics.merge(top_countries_str, on='product', how='left')
    
    print(f"Calculated analytics for {len(product_analytics)} products")
    return product_analytics


@task(name="calculate_country_analytics")
def calculate_country_analytics(fact_sales: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate analytics per country.
    
    Metrics:
    - Total revenue (CA)
    - Number of customers
    - Number of orders
    - Average order value
    
    Args:
        fact_sales: Fact table with sales data
        
    Returns:
        DataFrame with country analytics
    """
    country_analytics = fact_sales.groupby('country').agg({
        'total': 'sum',
        'id_client': 'nunique',
        'id_purchase': 'count',
        'quantity': 'sum'
    }).reset_index()
    
    # Flatten column names
    country_analytics.columns = [
        'country', 'total_revenue', 'unique_customers',
        'total_orders', 'total_quantity'
    ]
    
    # Calculate metrics
    country_analytics['avg_order_value'] = (
        country_analytics['total_revenue'] / country_analytics['total_orders']
    ).round(2)
    
    country_analytics['avg_revenue_per_customer'] = (
        country_analytics['total_revenue'] / country_analytics['unique_customers']
    ).round(2)
    
    # Calculate market share
    total_revenue = country_analytics['total_revenue'].sum()
    country_analytics['market_share_pct'] = (
        country_analytics['total_revenue'] / total_revenue * 100
    ).round(2)
    
    # Round numeric columns
    country_analytics['total_revenue'] = country_analytics['total_revenue'].round(2)
    
    # Sort by revenue
    country_analytics = country_analytics.sort_values('total_revenue', ascending=False)
    
    print(f"Calculated analytics for {len(country_analytics)} countries")
    return country_analytics


@task(name="calculate_daily_aggregations")
def calculate_daily_aggregations(fact_sales: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily aggregations.
    
    Args:
        fact_sales: Fact table with sales data
        
    Returns:
        DataFrame with daily metrics
    """
    daily_agg = fact_sales.groupby('purchase_date').agg({
        'total': 'sum',
        'id_purchase': 'count',
        'id_client': 'nunique',
        'quantity': 'sum'
    }).reset_index()
    
    daily_agg.columns = [
        'date', 'total_revenue', 'total_orders',
        'unique_customers', 'total_quantity'
    ]
    
    # Calculate rolling averages (7-day)
    daily_agg = daily_agg.sort_values('date')
    daily_agg['revenue_7day_avg'] = daily_agg['total_revenue'].rolling(7, min_periods=1).mean().round(2)
    daily_agg['orders_7day_avg'] = daily_agg['total_orders'].rolling(7, min_periods=1).mean().round(2)
    
    # Calculate day-over-day growth
    daily_agg['revenue_growth_pct'] = (
        daily_agg['total_revenue'].pct_change() * 100
    ).round(2)
    
    # Round numeric columns
    daily_agg['total_revenue'] = daily_agg['total_revenue'].round(2)
    
    # Convert date to string
    daily_agg['date'] = daily_agg['date'].dt.strftime('%Y-%m-%d')
    
    print(f"Calculated daily aggregations for {len(daily_agg)} days")
    return daily_agg


@task(name="calculate_weekly_aggregations")
def calculate_weekly_aggregations(fact_sales: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate weekly aggregations.
    
    Args:
        fact_sales: Fact table with sales data
        
    Returns:
        DataFrame with weekly metrics
    """
    weekly_agg = fact_sales.groupby('year_week').agg({
        'total': 'sum',
        'id_purchase': 'count',
        'id_client': 'nunique',
        'quantity': 'sum'
    }).reset_index()
    
    weekly_agg.columns = [
        'year_week', 'total_revenue', 'total_orders',
        'unique_customers', 'total_quantity'
    ]
    
    # Calculate average order value
    weekly_agg['avg_order_value'] = (
        weekly_agg['total_revenue'] / weekly_agg['total_orders']
    ).round(2)
    
    # Calculate week-over-week growth
    weekly_agg = weekly_agg.sort_values('year_week')
    weekly_agg['revenue_growth_pct'] = (
        weekly_agg['total_revenue'].pct_change() * 100
    ).round(2)
    
    # Round numeric columns
    weekly_agg['total_revenue'] = weekly_agg['total_revenue'].round(2)
    
    print(f"Calculated weekly aggregations for {len(weekly_agg)} weeks")
    return weekly_agg


@task(name="calculate_monthly_aggregations")
def calculate_monthly_aggregations(fact_sales: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate monthly aggregations.
    
    Args:
        fact_sales: Fact table with sales data
        
    Returns:
        DataFrame with monthly metrics
    """
    monthly_agg = fact_sales.groupby('year_month').agg({
        'total': 'sum',
        'id_purchase': 'count',
        'id_client': 'nunique',
        'quantity': 'sum'
    }).reset_index()
    
    monthly_agg.columns = [
        'year_month', 'total_revenue', 'total_orders',
        'unique_customers', 'total_quantity'
    ]
    
    # Calculate metrics
    monthly_agg['avg_order_value'] = (
        monthly_agg['total_revenue'] / monthly_agg['total_orders']
    ).round(2)
    
    monthly_agg['avg_revenue_per_customer'] = (
        monthly_agg['total_revenue'] / monthly_agg['unique_customers']
    ).round(2)
    
    # Calculate month-over-month growth
    monthly_agg = monthly_agg.sort_values('year_month')
    monthly_agg['revenue_growth_pct'] = (
        monthly_agg['total_revenue'].pct_change() * 100
    ).round(2)
    
    monthly_agg['orders_growth_pct'] = (
        monthly_agg['total_orders'].pct_change() * 100
    ).round(2)
    
    monthly_agg['customers_growth_pct'] = (
        monthly_agg['unique_customers'].pct_change() * 100
    ).round(2)
    
    # Round numeric columns
    monthly_agg['total_revenue'] = monthly_agg['total_revenue'].round(2)
    
    print(f"Calculated monthly aggregations for {len(monthly_agg)} months")
    return monthly_agg


@task(name="calculate_statistical_distributions")
def calculate_statistical_distributions(fact_sales: pd.DataFrame) -> dict:
    """
    Calculate statistical distributions.
    
    Args:
        fact_sales: Fact table with sales data
        
    Returns:
        Dictionary with statistical metrics
    """
    stats = {
        'order_value_distribution': {
            'mean': float(fact_sales['total'].mean().round(2)),
            'median': float(fact_sales['total'].median().round(2)),
            'std': float(fact_sales['total'].std().round(2)),
            'min': float(fact_sales['total'].min().round(2)),
            'max': float(fact_sales['total'].max().round(2)),
            'q25': float(fact_sales['total'].quantile(0.25).round(2)),
            'q75': float(fact_sales['total'].quantile(0.75).round(2)),
            'q90': float(fact_sales['total'].quantile(0.90).round(2)),
            'q95': float(fact_sales['total'].quantile(0.95).round(2))
        },
        'quantity_distribution': {
            'mean': float(fact_sales['quantity'].mean().round(2)),
            'median': float(fact_sales['quantity'].median()),
            'mode': int(fact_sales['quantity'].mode()[0]) if len(fact_sales['quantity'].mode()) > 0 else 0,
            'min': int(fact_sales['quantity'].min()),
            'max': int(fact_sales['quantity'].max())
        },
        'price_distribution': {
            'mean': float(fact_sales['price'].mean().round(2)),
            'median': float(fact_sales['price'].median().round(2)),
            'std': float(fact_sales['price'].std().round(2)),
            'min': float(fact_sales['price'].min().round(2)),
            'max': float(fact_sales['price'].max().round(2))
        },
        'overall_metrics': {
            'total_revenue': float(fact_sales['total'].sum().round(2)),
            'total_orders': int(len(fact_sales)),
            'total_customers': int(fact_sales['id_client'].nunique()),
            'total_products': int(fact_sales['product'].nunique()),
            'total_countries': int(fact_sales['country'].nunique()),
            'avg_order_value': float(fact_sales['total'].mean().round(2)),
            'avg_items_per_order': float(fact_sales['quantity'].mean().round(2))
        }
    }
    
    print("Calculated statistical distributions")
    return stats


@task(name="write_to_gold", retries=2)
def write_csv_to_gold(df: pd.DataFrame, object_name: str) -> str:
    """
    Write DataFrame to gold bucket as CSV.
    
    Args:
        df: DataFrame to write
        object_name: Name of object in gold bucket
        
    Returns:
        Object name in gold bucket
    """
    client = get_minio_client()
    
    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)
    
    # Convert DataFrame to CSV bytes
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode('utf-8')
    
    client.put_object(
        BUCKET_GOLD,
        object_name,
        BytesIO(csv_bytes),
        length=len(csv_bytes)
    )
    
    print(f"Wrote {len(df)} rows to {BUCKET_GOLD}/{object_name}")
    return object_name


@task(name="write_parquet_to_gold", retries=2)
def write_parquet_to_gold(df: pd.DataFrame, object_name: str) -> str:
    """
    Write DataFrame to gold bucket as Parquet.
    
    Args:
        df: DataFrame to write
        object_name: Name of object in gold bucket
        
    Returns:
        Object name in gold bucket
    """
    client = get_minio_client()
    
    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)
    
    # Convert DataFrame to Parquet bytes
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_bytes = parquet_buffer.getvalue()
    
    client.put_object(
        BUCKET_GOLD,
        object_name,
        BytesIO(parquet_bytes),
        length=len(parquet_bytes)
    )
    
    print(f"Wrote {len(df)} rows to {BUCKET_GOLD}/{object_name}")
    return object_name


@task(name="write_json_to_gold", retries=2)
def write_json_to_gold(data: dict, object_name: str) -> str:
    """
    Write dictionary to gold bucket as JSON.
    
    Args:
        data: Dictionary to write
        object_name: Name of object in gold bucket
        
    Returns:
        Object name in gold bucket
    """
    client = get_minio_client()
    
    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)
    
    # Add timestamp
    data['generated_at'] = datetime.now().isoformat()
    
    # Convert to JSON bytes
    json_bytes = json.dumps(data, indent=2).encode('utf-8')
    
    client.put_object(
        BUCKET_GOLD,
        object_name,
        BytesIO(json_bytes),
        length=len(json_bytes)
    )
    
    print(f"Wrote JSON to {BUCKET_GOLD}/{object_name}")
    return object_name


@flow(name="Gold Aggregation Flow")
def gold_aggregation_flow() -> dict:
    """
    Main flow: Create gold layer with business aggregations and KPIs.
    
    Creates:
    - Fact table (denormalized sales)
    - Client KPIs
    - Product analytics
    - Country analytics
    - Temporal aggregations (daily, weekly, monthly)
    - Statistical distributions
    
    Returns:
        Dictionary with created objects
    """
    print("=" * 60)
    print("Starting Gold Aggregation")
    print("=" * 60)
    
    # Read silver data
    clients_df = read_csv_from_silver("clients.csv")
    purchases_df = read_csv_from_silver("purchases.csv")
    
    # Create fact table
    print("\n[1/9] Creating fact table...")
    fact_sales = create_fact_sales(purchases_df, clients_df)
    fact_sales_file = write_csv_to_gold(fact_sales, "fact_sales.csv")
    write_parquet_to_gold(fact_sales, "fact_sales.parquet")
    
    # Calculate client KPIs
    print("\n[2/9] Calculating client KPIs...")
    client_kpis = calculate_client_kpis(fact_sales)
    client_kpis_file = write_csv_to_gold(client_kpis, "client_kpis.csv")
    write_parquet_to_gold(client_kpis, "client_kpis.parquet")
    
    # Calculate product analytics
    print("\n[3/9] Calculating product analytics...")
    product_analytics = calculate_product_analytics(fact_sales)
    product_analytics_file = write_csv_to_gold(product_analytics, "product_analytics.csv")
    write_parquet_to_gold(product_analytics, "product_analytics.parquet")
    
    # Calculate country analytics
    print("\n[4/9] Calculating country analytics...")
    country_analytics = calculate_country_analytics(fact_sales)
    country_analytics_file = write_csv_to_gold(country_analytics, "country_analytics.csv")
    write_parquet_to_gold(country_analytics, "country_analytics.parquet")
    
    # Calculate daily aggregations
    print("\n[5/9] Calculating daily aggregations...")
    daily_agg = calculate_daily_aggregations(fact_sales)
    daily_agg_file = write_csv_to_gold(daily_agg, "daily_sales.csv")
    write_parquet_to_gold(daily_agg, "daily_sales.parquet")
    
    # Calculate weekly aggregations
    print("\n[6/9] Calculating weekly aggregations...")
    weekly_agg = calculate_weekly_aggregations(fact_sales)
    weekly_agg_file = write_csv_to_gold(weekly_agg, "weekly_sales.csv")
    write_parquet_to_gold(weekly_agg, "weekly_sales.parquet")
    
    # Calculate monthly aggregations
    print("\n[7/9] Calculating monthly aggregations...")
    monthly_agg = calculate_monthly_aggregations(fact_sales)
    monthly_agg_file = write_csv_to_gold(monthly_agg, "monthly_sales.csv")
    write_parquet_to_gold(monthly_agg, "monthly_sales.parquet")
    
    # Calculate statistical distributions
    print("\n[8/9] Calculating statistical distributions...")
    stats = calculate_statistical_distributions(fact_sales)
    stats_file = write_json_to_gold(stats, "statistical_distributions.json")
    
    # Create summary report
    print("\n[9/9] Creating summary report...")
    summary = {
        "fact_table": {
            "total_rows": len(fact_sales),
            "date_range": {
                "min": fact_sales['purchase_date'].min().strftime('%Y-%m-%d'),
                "max": fact_sales['purchase_date'].max().strftime('%Y-%m-%d')
            }
        },
        "kpis": {
            "total_clients": len(client_kpis),
            "total_products": len(product_analytics),
            "total_countries": len(country_analytics),
            "active_clients": int(client_kpis['is_active'].sum()),
            "high_value_clients": int(client_kpis['is_high_value'].sum())
        },
        "temporal_aggregations": {
            "daily_records": len(daily_agg),
            "weekly_records": len(weekly_agg),
            "monthly_records": len(monthly_agg)
        },
        "top_performers": {
            "top_product_by_revenue": product_analytics.iloc[0]['product'] if len(product_analytics) > 0 else None,
            "top_country_by_revenue": country_analytics.iloc[0]['country'] if len(country_analytics) > 0 else None,
            "top_client_by_spending": client_kpis.nlargest(1, 'total_spent').iloc[0]['name'] if len(client_kpis) > 0 else None
        },
        "files_created": {
            "fact_sales": fact_sales_file,
            "client_kpis": client_kpis_file,
            "product_analytics": product_analytics_file,
            "country_analytics": country_analytics_file,
            "daily_sales": daily_agg_file,
            "weekly_sales": weekly_agg_file,
            "monthly_sales": monthly_agg_file,
            "statistical_distributions": stats_file
        }
    }
    
    summary_file = write_json_to_gold(summary, "gold_summary.json")
    
    print("\n" + "=" * 60)
    print("Gold Aggregation Complete!")
    print("=" * 60)
    print(f"\nFiles created in gold bucket:")
    print(f"  - {fact_sales_file} ({len(fact_sales)} rows)")
    print(f"  - {client_kpis_file} ({len(client_kpis)} clients)")
    print(f"  - {product_analytics_file} ({len(product_analytics)} products)")
    print(f"  - {country_analytics_file} ({len(country_analytics)} countries)")
    print(f"  - {daily_agg_file} ({len(daily_agg)} days)")
    print(f"  - {weekly_agg_file} ({len(weekly_agg)} weeks)")
    print(f"  - {monthly_agg_file} ({len(monthly_agg)} months)")
    print(f"  - {stats_file}")
    print(f"  - {summary_file}")
    
    print(f"\nKey Metrics:")
    print(f"  - Total Revenue: ${stats['overall_metrics']['total_revenue']:,.2f}")
    print(f"  - Total Orders: {stats['overall_metrics']['total_orders']:,}")
    print(f"  - Unique Customers: {stats['overall_metrics']['total_customers']}")
    print(f"  - Average Order Value: ${stats['overall_metrics']['avg_order_value']:.2f}")
    print(f"  - Active Clients: {summary['kpis']['active_clients']}")
    print(f"  - High Value Clients: {summary['kpis']['high_value_clients']}")
    
    return summary


if __name__ == "__main__":
    result = gold_aggregation_flow()
    print(f"\nGold aggregation complete: {result}")
