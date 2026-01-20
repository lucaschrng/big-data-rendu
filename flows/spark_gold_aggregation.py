"""
Gold Aggregation using PySpark.
Equivalent to gold_aggregation.py but using Spark DataFrames.
"""
import json
from datetime import datetime
from io import BytesIO, StringIO

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

from config import BUCKET_GOLD, BUCKET_SILVER, get_minio_client


def get_spark_session(master_url: str = "local[*]") -> SparkSession:
    """
    Create or get SparkSession.
    
    Args:
        master_url: Spark master URL (default: local mode)
        
    Returns:
        SparkSession instance
    """
    return SparkSession.builder \
        .appName("ELT-Gold-Aggregation") \
        .master(master_url) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "8") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.driver.maxResultSize", "2g") \
        .getOrCreate()


def read_csv_from_silver_spark(spark: SparkSession, object_name: str) -> DataFrame:
    """
    Read CSV data from silver bucket into Spark DataFrame.
    
    Args:
        spark: SparkSession
        object_name: Name of object in silver bucket
        
    Returns:
        Spark DataFrame with cleaned data
    """
    client = get_minio_client()
    
    # Download CSV to temporary file to use Spark's native CSV reader
    import tempfile
    import os
    
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as tmp_file:
        response = client.get_object(BUCKET_SILVER, object_name)
        tmp_file.write(response.read())
        tmp_path = tmp_file.name
        response.close()
        response.release_conn()
    
    # Use Spark's native CSV reader with proper partitioning
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(tmp_path)
    
    # Repartition for better parallelism
    df = df.repartition(8)
    
    # Cache the DataFrame to force loading before we delete the temp file
    df = df.cache()
    df.count()  # Trigger the cache
    
    # Now we can safely delete the temp file
    if os.path.exists(tmp_path):
        os.unlink(tmp_path)
    
    print(f"Read data from silver/{object_name}")
    return df


def create_fact_sales_spark(purchases_df: DataFrame, clients_df: DataFrame) -> DataFrame:
    """
    Create fact table by joining purchases with client dimensions using Spark.
    
    Args:
        purchases_df: Cleaned purchases data
        clients_df: Cleaned clients data
        
    Returns:
        Fact table with denormalized data
    """
    # Select relevant columns from clients
    clients_subset = clients_df.select(
        F.col('id_client').cast(IntegerType()).alias('client_id'),
        F.col('name').alias('client_name'),
        'email',
        'country',
        'subscription_date'
    )
    
    # Cast purchases columns
    purchases_df = purchases_df.withColumn('id_client', F.col('id_client').cast(IntegerType()))
    purchases_df = purchases_df.withColumn('price', F.col('price').cast(DoubleType()))
    purchases_df = purchases_df.withColumn('quantity', F.col('quantity').cast(DoubleType()))
    purchases_df = purchases_df.withColumn('total', F.col('total').cast(DoubleType()))
    
    # Join purchases with clients
    # Use broadcast join since clients is smaller than purchases
    fact_sales = purchases_df.join(
        F.broadcast(clients_subset),
        purchases_df['id_client'] == clients_subset['client_id'],
        'left'
    ).drop('client_id')
    
    # Convert dates to date type
    fact_sales = fact_sales.withColumn('purchase_date', F.to_date(F.col('purchase_date')))
    fact_sales = fact_sales.withColumn('subscription_date', F.to_date(F.col('subscription_date')))
    
    # Add temporal dimensions
    fact_sales = fact_sales.withColumn('purchase_year', F.year('purchase_date'))
    fact_sales = fact_sales.withColumn('purchase_month', F.month('purchase_date'))
    fact_sales = fact_sales.withColumn('purchase_quarter', F.quarter('purchase_date'))
    fact_sales = fact_sales.withColumn('purchase_week', F.weekofyear('purchase_date'))
    fact_sales = fact_sales.withColumn('purchase_day_of_week', F.dayofweek('purchase_date'))
    fact_sales = fact_sales.withColumn('purchase_day_name', F.date_format('purchase_date', 'EEEE'))
    
    # Add year-month for easy grouping
    fact_sales = fact_sales.withColumn('year_month', F.date_format('purchase_date', 'yyyy-MM'))
    fact_sales = fact_sales.withColumn('year_week', F.concat(
        F.year('purchase_date').cast('string'),
        F.lit('-W'),
        F.lpad(F.weekofyear('purchase_date').cast('string'), 2, '0')
    ))
    
    # Calculate customer tenure at purchase time
    fact_sales = fact_sales.withColumn(
        'customer_tenure_days',
        F.datediff('purchase_date', 'subscription_date')
    )
    
    print(f"Created fact_sales table with {fact_sales.count()} rows")
    return fact_sales


def calculate_client_kpis_spark(fact_sales: DataFrame) -> DataFrame:
    """
    Calculate KPIs per client using Spark.
    
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
    client_kpis = fact_sales.groupBy('id_client').agg(
        F.count('id_purchase').alias('total_purchases'),
        F.round(F.sum('total'), 2).alias('total_spent'),
        F.round(F.avg('total'), 2).alias('avg_order_value'),
        F.round(F.min('total'), 2).alias('min_order_value'),
        F.round(F.max('total'), 2).alias('max_order_value'),
        F.sum('quantity').alias('total_quantity'),
        F.min('purchase_date').alias('first_purchase_date'),
        F.max('purchase_date').alias('last_purchase_date'),
        F.first('client_name').alias('name'),
        F.first('email').alias('email'),
        F.first('country').alias('country'),
        F.first('subscription_date').alias('subscription_date')
    )
    
    # Calculate customer lifetime (days since first purchase)
    today = datetime.now().date()
    today_str = today.strftime('%Y-%m-%d')
    
    client_kpis = client_kpis.withColumn(
        'customer_lifetime_days',
        F.datediff(F.lit(today_str), 'first_purchase_date')
    )
    
    # Purchase frequency (purchases per month)
    client_kpis = client_kpis.withColumn(
        'purchase_frequency_per_month',
        F.round(
            F.col('total_purchases') / F.greatest(F.col('customer_lifetime_days') / 30, F.lit(1)),
            2
        )
    )
    
    # Days since last purchase (recency)
    client_kpis = client_kpis.withColumn(
        'days_since_last_purchase',
        F.datediff(F.lit(today_str), 'last_purchase_date')
    )
    
    # Customer segment based on RFM (simple)
    client_kpis = client_kpis.withColumn('is_active', F.col('days_since_last_purchase') <= 90)
    
    # Calculate median for high value segmentation
    median_spent = client_kpis.approxQuantile('total_spent', [0.5], 0.01)[0]
    client_kpis = client_kpis.withColumn('is_high_value', F.col('total_spent') > F.lit(median_spent))
    
    # Convert dates to string for CSV
    client_kpis = client_kpis.withColumn('first_purchase_date', F.date_format('first_purchase_date', 'yyyy-MM-dd'))
    client_kpis = client_kpis.withColumn('last_purchase_date', F.date_format('last_purchase_date', 'yyyy-MM-dd'))
    client_kpis = client_kpis.withColumn('subscription_date', F.date_format('subscription_date', 'yyyy-MM-dd'))
    
    print(f"Calculated KPIs for {client_kpis.count()} clients")
    return client_kpis


def calculate_product_analytics_spark(fact_sales: DataFrame) -> DataFrame:
    """
    Calculate analytics per product using Spark.
    
    Args:
        fact_sales: Fact table with sales data
        
    Returns:
        DataFrame with product analytics
    """
    product_analytics = fact_sales.groupBy('product').agg(
        F.sum('quantity').alias('total_quantity_sold'),
        F.round(F.sum('total'), 2).alias('total_revenue'),
        F.round(F.avg('price'), 2).alias('avg_price'),
        F.round(F.min('price'), 2).alias('min_price'),
        F.round(F.max('price'), 2).alias('max_price'),
        F.count('id_purchase').alias('total_orders'),
        F.countDistinct('id_client').alias('unique_customers')
    )
    
    # Calculate average quantity per order
    product_analytics = product_analytics.withColumn(
        'avg_quantity_per_order',
        F.round(F.col('total_quantity_sold') / F.col('total_orders'), 2)
    )
    
    # Sort by revenue
    product_analytics = product_analytics.orderBy(F.desc('total_revenue'))
    
    # Add top countries per product
    # Partition by product for window function
    window_spec = Window.partitionBy('product').orderBy(F.desc('country_total'))
    
    top_countries = fact_sales.groupBy('product', 'country').agg(
        F.sum('total').alias('country_total')
    ).withColumn('rank', F.row_number().over(window_spec)).filter(F.col('rank') <= 3)
    
    top_countries_str = top_countries.groupBy('product').agg(
        F.concat_ws(', ', F.collect_list('country')).alias('top_countries')
    )
    
    product_analytics = product_analytics.join(top_countries_str, 'product', 'left')
    
    print(f"Calculated analytics for {product_analytics.count()} products")
    return product_analytics


def calculate_country_analytics_spark(fact_sales: DataFrame) -> DataFrame:
    """
    Calculate analytics per country using Spark.
    
    Args:
        fact_sales: Fact table with sales data
        
    Returns:
        DataFrame with country analytics
    """
    country_analytics = fact_sales.groupBy('country').agg(
        F.round(F.sum('total'), 2).alias('total_revenue'),
        F.countDistinct('id_client').alias('unique_customers'),
        F.count('id_purchase').alias('total_orders'),
        F.sum('quantity').alias('total_quantity')
    )
    
    # Calculate metrics
    country_analytics = country_analytics.withColumn(
        'avg_order_value',
        F.round(F.col('total_revenue') / F.col('total_orders'), 2)
    )
    
    country_analytics = country_analytics.withColumn(
        'avg_revenue_per_customer',
        F.round(F.col('total_revenue') / F.col('unique_customers'), 2)
    )
    
    # Calculate market share
    total_revenue = country_analytics.agg(F.sum('total_revenue')).collect()[0][0]
    country_analytics = country_analytics.withColumn(
        'market_share_pct',
        F.round(F.col('total_revenue') / F.lit(total_revenue) * 100, 2)
    )
    
    # Sort by revenue
    country_analytics = country_analytics.orderBy(F.desc('total_revenue'))
    
    print(f"Calculated analytics for {country_analytics.count()} countries")
    return country_analytics


def calculate_daily_aggregations_spark(fact_sales: DataFrame) -> DataFrame:
    """
    Calculate daily aggregations using Spark.
    
    Args:
        fact_sales: Fact table with sales data
        
    Returns:
        DataFrame with daily metrics
    """
    daily_agg = fact_sales.groupBy('purchase_date').agg(
        F.round(F.sum('total'), 2).alias('total_revenue'),
        F.count('id_purchase').alias('total_orders'),
        F.countDistinct('id_client').alias('unique_customers'),
        F.sum('quantity').alias('total_quantity')
    ).withColumnRenamed('purchase_date', 'date')
    
    # Sort by date
    daily_agg = daily_agg.orderBy('date').coalesce(1)
    
    # Calculate rolling averages (7-day)
    window_7day = Window.orderBy('date').rowsBetween(-6, 0)
    daily_agg = daily_agg.withColumn(
        'revenue_7day_avg',
        F.round(F.avg('total_revenue').over(window_7day), 2)
    )
    daily_agg = daily_agg.withColumn(
        'orders_7day_avg',
        F.round(F.avg('total_orders').over(window_7day), 2)
    )
    
    # Calculate day-over-day growth
    window_lag = Window.orderBy('date')
    daily_agg = daily_agg.withColumn(
        'prev_revenue',
        F.lag('total_revenue').over(window_lag)
    )
    daily_agg = daily_agg.withColumn(
        'revenue_growth_pct',
        F.round((F.col('total_revenue') - F.col('prev_revenue')) / F.col('prev_revenue') * 100, 2)
    ).drop('prev_revenue')
    
    # Convert date to string
    daily_agg = daily_agg.withColumn('date', F.date_format('date', 'yyyy-MM-dd'))
    
    print(f"Calculated daily aggregations for {daily_agg.count()} days")
    return daily_agg


def calculate_weekly_aggregations_spark(fact_sales: DataFrame) -> DataFrame:
    """
    Calculate weekly aggregations using Spark.
    
    Args:
        fact_sales: Fact table with sales data
        
    Returns:
        DataFrame with weekly metrics
    """
    weekly_agg = fact_sales.groupBy('year_week').agg(
        F.round(F.sum('total'), 2).alias('total_revenue'),
        F.count('id_purchase').alias('total_orders'),
        F.countDistinct('id_client').alias('unique_customers'),
        F.sum('quantity').alias('total_quantity')
    )
    
    # Calculate average order value
    weekly_agg = weekly_agg.withColumn(
        'avg_order_value',
        F.round(F.col('total_revenue') / F.col('total_orders'), 2)
    )
    
    # Sort by week
    weekly_agg = weekly_agg.orderBy('year_week').coalesce(1)
    
    # Calculate week-over-week growth
    window_lag = Window.orderBy('year_week')
    weekly_agg = weekly_agg.withColumn(
        'prev_revenue',
        F.lag('total_revenue').over(window_lag)
    )
    weekly_agg = weekly_agg.withColumn(
        'revenue_growth_pct',
        F.round((F.col('total_revenue') - F.col('prev_revenue')) / F.col('prev_revenue') * 100, 2)
    ).drop('prev_revenue')
    
    print(f"Calculated weekly aggregations for {weekly_agg.count()} weeks")
    return weekly_agg


def calculate_monthly_aggregations_spark(fact_sales: DataFrame) -> DataFrame:
    """
    Calculate monthly aggregations using Spark.
    
    Args:
        fact_sales: Fact table with sales data
        
    Returns:
        DataFrame with monthly metrics
    """
    monthly_agg = fact_sales.groupBy('year_month').agg(
        F.round(F.sum('total'), 2).alias('total_revenue'),
        F.count('id_purchase').alias('total_orders'),
        F.countDistinct('id_client').alias('unique_customers'),
        F.sum('quantity').alias('total_quantity')
    )
    
    # Calculate metrics
    monthly_agg = monthly_agg.withColumn(
        'avg_order_value',
        F.round(F.col('total_revenue') / F.col('total_orders'), 2)
    )
    monthly_agg = monthly_agg.withColumn(
        'avg_revenue_per_customer',
        F.round(F.col('total_revenue') / F.col('unique_customers'), 2)
    )
    
    # Sort by month
    monthly_agg = monthly_agg.orderBy('year_month').coalesce(1)
    
    # Calculate month-over-month growth
    window_lag = Window.orderBy('year_month')
    
    monthly_agg = monthly_agg.withColumn('prev_revenue', F.lag('total_revenue').over(window_lag))
    monthly_agg = monthly_agg.withColumn('prev_orders', F.lag('total_orders').over(window_lag))
    monthly_agg = monthly_agg.withColumn('prev_customers', F.lag('unique_customers').over(window_lag))
    
    monthly_agg = monthly_agg.withColumn(
        'revenue_growth_pct',
        F.round((F.col('total_revenue') - F.col('prev_revenue')) / F.col('prev_revenue') * 100, 2)
    )
    monthly_agg = monthly_agg.withColumn(
        'orders_growth_pct',
        F.round((F.col('total_orders') - F.col('prev_orders')) / F.col('prev_orders') * 100, 2)
    )
    monthly_agg = monthly_agg.withColumn(
        'customers_growth_pct',
        F.round((F.col('unique_customers') - F.col('prev_customers')) / F.col('prev_customers') * 100, 2)
    )
    
    monthly_agg = monthly_agg.drop('prev_revenue', 'prev_orders', 'prev_customers')
    
    print(f"Calculated monthly aggregations for {monthly_agg.count()} months")
    return monthly_agg


def calculate_statistical_distributions_spark(fact_sales: DataFrame) -> dict:
    """
    Calculate statistical distributions using Spark.
    
    Args:
        fact_sales: Fact table with sales data
        
    Returns:
        Dictionary with statistical metrics
    """
    # Order value distribution
    order_stats = fact_sales.agg(
        F.round(F.mean('total'), 2).alias('mean'),
        F.round(F.stddev('total'), 2).alias('std'),
        F.round(F.min('total'), 2).alias('min'),
        F.round(F.max('total'), 2).alias('max')
    ).collect()[0]
    
    order_quantiles = fact_sales.approxQuantile('total', [0.25, 0.5, 0.75, 0.90, 0.95], 0.01)
    
    # Quantity distribution
    quantity_stats = fact_sales.agg(
        F.round(F.mean('quantity'), 2).alias('mean'),
        F.min('quantity').alias('min'),
        F.max('quantity').alias('max')
    ).collect()[0]
    
    quantity_median = fact_sales.approxQuantile('quantity', [0.5], 0.01)[0]
    
    # Price distribution
    price_stats = fact_sales.agg(
        F.round(F.mean('price'), 2).alias('mean'),
        F.round(F.stddev('price'), 2).alias('std'),
        F.round(F.min('price'), 2).alias('min'),
        F.round(F.max('price'), 2).alias('max')
    ).collect()[0]
    
    price_median = fact_sales.approxQuantile('price', [0.5], 0.01)[0]
    
    # Overall metrics
    overall = fact_sales.agg(
        F.round(F.sum('total'), 2).alias('total_revenue'),
        F.count('id_purchase').alias('total_orders'),
        F.countDistinct('id_client').alias('total_customers'),
        F.countDistinct('product').alias('total_products'),
        F.countDistinct('country').alias('total_countries'),
        F.round(F.mean('total'), 2).alias('avg_order_value'),
        F.round(F.mean('quantity'), 2).alias('avg_items_per_order')
    ).collect()[0]
    
    stats = {
        'order_value_distribution': {
            'mean': float(order_stats['mean']),
            'median': round(order_quantiles[1], 2),
            'std': float(order_stats['std']) if order_stats['std'] else 0,
            'min': float(order_stats['min']),
            'max': float(order_stats['max']),
            'q25': round(order_quantiles[0], 2),
            'q75': round(order_quantiles[2], 2),
            'q90': round(order_quantiles[3], 2),
            'q95': round(order_quantiles[4], 2)
        },
        'quantity_distribution': {
            'mean': float(quantity_stats['mean']),
            'median': round(quantity_median, 2),
            'min': int(quantity_stats['min']),
            'max': int(quantity_stats['max'])
        },
        'price_distribution': {
            'mean': float(price_stats['mean']),
            'median': round(price_median, 2),
            'std': float(price_stats['std']) if price_stats['std'] else 0,
            'min': float(price_stats['min']),
            'max': float(price_stats['max'])
        },
        'overall_metrics': {
            'total_revenue': float(overall['total_revenue']),
            'total_orders': int(overall['total_orders']),
            'total_customers': int(overall['total_customers']),
            'total_products': int(overall['total_products']),
            'total_countries': int(overall['total_countries']),
            'avg_order_value': float(overall['avg_order_value']),
            'avg_items_per_order': float(overall['avg_items_per_order'])
        }
    }
    
    print("Calculated statistical distributions")
    return stats


def write_spark_df_to_gold(df: DataFrame, object_name: str) -> str:
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
    
    # Collect to pandas for CSV output
    pdf = df.toPandas()
    csv_buffer = StringIO()
    pdf.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode('utf-8')
    
    client.put_object(
        BUCKET_GOLD,
        object_name,
        BytesIO(csv_bytes),
        length=len(csv_bytes)
    )
    
    print(f"Wrote {df.count()} rows to {BUCKET_GOLD}/{object_name}")
    return object_name


def write_spark_parquet_to_gold(df: DataFrame, object_name: str) -> str:
    """
    Write Spark DataFrame to gold bucket as Parquet.
    
    Args:
        df: DataFrame to write
        object_name: Name of object in gold bucket
        
    Returns:
        Object name in gold bucket
    """
    client = get_minio_client()
    
    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)
    
    # Download to temp file and upload (simplest way with MinIO client)
    # Alternatively could configure Hadoop/S3A but that requires more jars
    import tempfile
    import os
    import shutil
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Write parquet to local temp dir
        local_path = os.path.join(temp_dir, "data.parquet")
        df.write.mode("overwrite").parquet(local_path)
        
        # In Spark, writing to a directory creates part- files. 
        # We want to upload the folder structure or a single file?
        # MinIO/S3 structure usually expects a "prefix" (folder) which contains the part files.
        # So object_name should be treated as a prefix if it doesn't end in an extension, 
        # or we upload all files under that prefix.
        
        # However, for simplicity and compatibility with the rest of the pipeline which treats objects as files,
        # we might want to try to coalesce(1) before write if data is small, to get a single part file,
        # but Spark still writes a directory.
        
        # Strategy: Upload all files in the directory to MinIO under the object_name prefix
        # If object_name is "fact_sales.parquet", we upload files to gold/fact_sales.parquet/part-000...
        
        for root, dirs, files in os.walk(local_path):
            for file in files:
                if file.startswith(".") or file.startswith("_SUCCESS"):
                    continue
                    
                local_file_path = os.path.join(root, file)
                # Calculate relative path for object storage
                # If structure is temp/data.parquet/part-001.parquet
                # We want gold/fact_sales.parquet/part-001.parquet
                
                # If we just want to support reading later by Spark/Pandas, they can read directories.
                # So uploading the directory content is correct.
                
                # Construct object key
                # object_name is e.g. "fact_sales.parquet"
                minio_key = f"{object_name}/{file}"
                
                client.fput_object(
                    BUCKET_GOLD,
                    minio_key,
                    local_file_path
                )
    
    print(f"Wrote Parquet to {BUCKET_GOLD}/{object_name}")
    return object_name


def write_json_to_gold_spark(data: dict, object_name: str) -> str:
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
    data['processing_engine'] = 'spark'
    
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


def spark_gold_aggregation_flow(master_url: str = "local[*]") -> dict:
    """
    Main flow: Create gold layer with business aggregations and KPIs using Spark.
    
    Creates:
    - Fact table (denormalized sales)
    - Client KPIs
    - Product analytics
    - Country analytics
    - Temporal aggregations (daily, weekly, monthly)
    - Statistical distributions
    
    Args:
        master_url: Spark master URL
        
    Returns:
        Dictionary with created objects
    """
    print("=" * 60)
    print("Starting Spark Gold Aggregation")
    print("=" * 60)
    
    spark = get_spark_session(master_url)
    
    try:
        # Read silver data
        clients_df = read_csv_from_silver_spark(spark, "clients_spark.csv")
        purchases_df = read_csv_from_silver_spark(spark, "purchases_spark.csv")
        
        # Create fact table
        print("\n[1/9] Creating fact table...")
        fact_sales = create_fact_sales_spark(purchases_df, clients_df)
        fact_sales.cache()  # Cache for reuse
        fact_sales_file = write_spark_df_to_gold(fact_sales, "fact_sales_spark.csv")
        write_spark_parquet_to_gold(fact_sales, "fact_sales_spark.parquet")
        
        # Calculate client KPIs
        print("\n[2/9] Calculating client KPIs...")
        client_kpis = calculate_client_kpis_spark(fact_sales)
        client_kpis_file = write_spark_df_to_gold(client_kpis, "client_kpis_spark.csv")
        write_spark_parquet_to_gold(client_kpis, "client_kpis_spark.parquet")
        
        # Calculate product analytics
        print("\n[3/9] Calculating product analytics...")
        product_analytics = calculate_product_analytics_spark(fact_sales)
        product_analytics_file = write_spark_df_to_gold(product_analytics, "product_analytics_spark.csv")
        write_spark_parquet_to_gold(product_analytics, "product_analytics_spark.parquet")
        
        # Calculate country analytics
        print("\n[4/9] Calculating country analytics...")
        country_analytics = calculate_country_analytics_spark(fact_sales)
        country_analytics_file = write_spark_df_to_gold(country_analytics, "country_analytics_spark.csv")
        write_spark_parquet_to_gold(country_analytics, "country_analytics_spark.parquet")
        
        # Calculate daily aggregations
        print("\n[5/9] Calculating daily aggregations...")
        daily_agg = calculate_daily_aggregations_spark(fact_sales)
        daily_agg_file = write_spark_df_to_gold(daily_agg, "daily_sales_spark.csv")
        write_spark_parquet_to_gold(daily_agg, "daily_sales_spark.parquet")
        
        # Calculate weekly aggregations
        print("\n[6/9] Calculating weekly aggregations...")
        weekly_agg = calculate_weekly_aggregations_spark(fact_sales)
        weekly_agg_file = write_spark_df_to_gold(weekly_agg, "weekly_sales_spark.csv")
        write_spark_parquet_to_gold(weekly_agg, "weekly_sales_spark.parquet")
        
        # Calculate monthly aggregations
        print("\n[7/9] Calculating monthly aggregations...")
        monthly_agg = calculate_monthly_aggregations_spark(fact_sales)
        monthly_agg_file = write_spark_df_to_gold(monthly_agg, "monthly_sales_spark.csv")
        write_spark_parquet_to_gold(monthly_agg, "monthly_sales_spark.parquet")
        
        # Calculate statistical distributions
        print("\n[8/9] Calculating statistical distributions...")
        stats = calculate_statistical_distributions_spark(fact_sales)
        stats_file = write_json_to_gold_spark(stats, "statistical_distributions_spark.json")
        
        # Create summary report
        print("\n[9/9] Creating summary report...")
        
        # Get date range
        date_range = fact_sales.agg(
            F.min('purchase_date').alias('min_date'),
            F.max('purchase_date').alias('max_date')
        ).collect()[0]
        
        summary = {
            "fact_table": {
                "total_rows": fact_sales.count(),
                "date_range": {
                    "min": str(date_range['min_date']),
                    "max": str(date_range['max_date'])
                }
            },
            "kpis": {
                "total_clients": client_kpis.count(),
                "total_products": product_analytics.count(),
                "total_countries": country_analytics.count(),
                "active_clients": client_kpis.filter(F.col('is_active') == True).count(),
                "high_value_clients": client_kpis.filter(F.col('is_high_value') == True).count()
            },
            "temporal_aggregations": {
                "daily_records": daily_agg.count(),
                "weekly_records": weekly_agg.count(),
                "monthly_records": monthly_agg.count()
            },
            "top_performers": {
                "top_product_by_revenue": product_analytics.first()['product'] if product_analytics.count() > 0 else None,
                "top_country_by_revenue": country_analytics.first()['country'] if country_analytics.count() > 0 else None,
                "top_client_by_spending": client_kpis.orderBy(F.desc('total_spent')).first()['name'] if client_kpis.count() > 0 else None
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
        
        summary_file = write_json_to_gold_spark(summary, "gold_summary_spark.json")
        
        print("\n" + "=" * 60)
        print("Spark Gold Aggregation Complete!")
        print("=" * 60)
        print(f"\nFiles created in gold bucket:")
        print(f"  - {fact_sales_file} ({summary['fact_table']['total_rows']} rows)")
        print(f"  - {client_kpis_file} ({summary['kpis']['total_clients']} clients)")
        print(f"  - {product_analytics_file} ({summary['kpis']['total_products']} products)")
        print(f"  - {country_analytics_file} ({summary['kpis']['total_countries']} countries)")
        print(f"  - {daily_agg_file} ({summary['temporal_aggregations']['daily_records']} days)")
        print(f"  - {weekly_agg_file} ({summary['temporal_aggregations']['weekly_records']} weeks)")
        print(f"  - {monthly_agg_file} ({summary['temporal_aggregations']['monthly_records']} months)")
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
        
    finally:
        spark.stop()


if __name__ == "__main__":
    result = spark_gold_aggregation_flow()
    print(f"\nSpark Gold aggregation complete: {result}")
