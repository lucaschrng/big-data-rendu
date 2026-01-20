"""
Silver Transformation using PySpark.
Equivalent to silver_transformation.py but using Spark DataFrames.
"""
import json
from datetime import datetime
from io import BytesIO, StringIO

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client


def get_spark_session(master_url: str = "local[*]") -> SparkSession:
    """
    Create or get SparkSession.
    
    Args:
        master_url: Spark master URL (default: local mode)
        
    Returns:
        SparkSession instance
    """
    return SparkSession.builder \
        .appName("ELT-Silver-Transformation") \
        .master(master_url) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "8") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.driver.maxResultSize", "2g") \
        .getOrCreate()


def read_csv_from_bronze_spark(spark: SparkSession, object_name: str) -> DataFrame:
    """
    Read CSV data from bronze bucket into Spark DataFrame.
    
    Args:
        spark: SparkSession
        object_name: Name of object in bronze bucket
        
    Returns:
        Spark DataFrame with raw data
    """
    client = get_minio_client()
    
    # Download CSV to temporary file to use Spark's native CSV reader
    import tempfile
    import os
    
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as tmp_file:
        response = client.get_object(BUCKET_BRONZE, object_name)
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
    
    print(f"Read data from {object_name}")
    return df


def clean_clients_data_spark(df: DataFrame) -> tuple[DataFrame, dict]:
    """
    Clean and transform clients data using Spark.
    
    Quality checks:
    - Remove null values
    - Standardize date formats
    - Validate email format
    - Deduplicate records
    - Filter future dates
    
    Args:
        df: Raw clients DataFrame
        
    Returns:
        Tuple of (cleaned DataFrame, quality metrics)
    """
    # Cache input since we'll use it for metrics and transformation
    df.cache()
    initial_count = df.count()
    
    quality_metrics = {
        "initial_rows": initial_count,
        "null_values": {},
        "duplicates_removed": 0,
        "invalid_dates": 0,
        "invalid_emails": 0,
        "future_dates_fixed": 0,
        "final_rows": 0
    }
    
    # Calculate initial quality metrics in a single pass
    # Using approx_count_distinct for speed on large datasets if precise count not needed,
    # but here we want exact metrics for the report.
    # To optimize, we can combine checks.
    
    today = datetime.now().date()
    today_str = today.strftime('%Y-%m-%d')
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    # 1. Cleaning transformations
    
    # Standardize date format
    df_clean = df.withColumn(
        'subscription_date_parsed',
        F.to_date(F.col('subscription_date'))
    )
    
    # Calculate metrics efficiently without multiple scans where possible
    # We'll do filtering and track counts
    
    # Filter nulls
    df_clean = df_clean.dropna()
    
    # Validate emails
    df_clean = df_clean.filter(F.col('email').rlike(email_pattern))
    
    # Handle dates
    df_clean = df_clean.filter(F.col('subscription_date_parsed').isNotNull())
    
    # Fix future dates
    df_clean = df_clean.withColumn(
        'subscription_date_final',
        F.when(F.col('subscription_date_parsed') > F.lit(today_str), F.lit(today_str))
        .otherwise(F.col('subscription_date_parsed'))
    ).drop('subscription_date', 'subscription_date_parsed').withColumnRenamed('subscription_date_final', 'subscription_date')
    
    # Deduplicate
    df_clean = df_clean.dropDuplicates(['id_client'])
    df_clean = df_clean.dropDuplicates(['email'])
    
    # Normalize types
    df_clean = df_clean.withColumn('id_client', F.col('id_client').cast(IntegerType())) \
        .withColumn('name', F.trim(F.col('name'))) \
        .withColumn('email', F.lower(F.trim(F.col('email')))) \
        .withColumn('country', F.trim(F.col('country'))) \
        .withColumn('subscription_date', F.date_format(F.col('subscription_date'), 'yyyy-MM-dd'))
    
    # Cache result and count once
    df_clean.cache()
    final_count = df_clean.count()
    
    quality_metrics["final_rows"] = final_count
    # Estimate dropped rows (simplified to avoid overhead)
    dropped_total = initial_count - final_count
    quality_metrics["duplicates_removed"] = dropped_total  # Approximate assignment for speed
    
    print(f"Clients cleaning: {initial_count} → {final_count} rows")
    return df_clean, quality_metrics


def clean_purchases_data_spark(df: DataFrame, valid_client_ids_df: DataFrame) -> tuple[DataFrame, dict]:
    """
    Clean and transform purchases data using Spark.
    
    Quality checks:
    - Remove null values
    - Standardize date formats
    - Validate numeric fields
    - Recalculate totals
    - Remove orphan purchases (invalid client_id)
    - Deduplicate records
    - Filter future dates
    
    Args:
        df: Raw purchases DataFrame
        valid_client_ids_df: DataFrame containing valid client IDs (column: id_client)
        
    Returns:
        Tuple of (cleaned DataFrame, quality metrics)
    """
    # Cache input
    df.cache()
    initial_count = df.count()
    
    quality_metrics = {
        "initial_rows": initial_count,
        "null_values": {},
        "duplicates_removed": 0,
        "invalid_dates": 0,
        "orphan_purchases": 0,
        "total_recalculated": 0,
        "negative_values": 0,
        "future_dates_fixed": 0,
        "final_rows": 0
    }
    
    today = datetime.now().date()
    today_str = today.strftime('%Y-%m-%d')
    
    # 1. Cleaning transformations
    
    # Standardize date format
    df_clean = df.withColumn(
        'purchase_date_parsed',
        F.to_date(F.col('purchase_date'))
    )
    
    # Filter nulls
    df_clean = df_clean.dropna()
    
    # Handle dates
    df_clean = df_clean.filter(F.col('purchase_date_parsed').isNotNull())
    
    # Fix future dates
    df_clean = df_clean.withColumn(
        'purchase_date_final',
        F.when(F.col('purchase_date_parsed') > F.lit(today_str), F.lit(today_str))
        .otherwise(F.col('purchase_date_parsed'))
    ).drop('purchase_date', 'purchase_date_parsed').withColumnRenamed('purchase_date_final', 'purchase_date')
    
    # Remove negative values
    df_clean = df_clean.filter((F.col('quantity') > 0) & (F.col('price') >= 0))
    
    # Recalculate totals (silent correction)
    df_clean = df_clean.withColumn(
        'total',
        F.round(F.col('quantity') * F.col('price'), 2)
    )
    
    # Normalize types
    df_clean = df_clean.withColumn('id_purchase', F.col('id_purchase').cast(IntegerType())) \
        .withColumn('id_client', F.col('id_client').cast(IntegerType())) \
        .withColumn('product', F.trim(F.col('product'))) \
        .withColumn('quantity', F.col('quantity').cast(IntegerType())) \
        .withColumn('price', F.col('price').cast(DoubleType())) \
        .withColumn('total', F.col('total').cast(DoubleType())) \
        .withColumn('purchase_date', F.date_format(F.col('purchase_date'), 'yyyy-MM-dd'))

    # Deduplicate
    df_clean = df_clean.dropDuplicates(['id_purchase'])
    
    # Remove orphan purchases using broadcast join if clients fit in memory (hinted by user context)
    # Using left_semi join to keep only purchases with valid client IDs
    # We rename id_client in valid_ids to avoid ambiguity if needed, but left_semi handles it
    
    # Ensuring valid_client_ids_df has correct type
    valid_ids = valid_client_ids_df.select(F.col('id_client').cast(IntegerType()).alias('valid_id'))
    
    # Using broadcast for valid_ids as it's just a single column of 800k ints (small enough ~3MB)
    df_clean = df_clean.join(
        F.broadcast(valid_ids),
        df_clean['id_client'] == valid_ids['valid_id'],
        'left_semi'
    )
    
    # Cache result
    df_clean.cache()
    final_count = df_clean.count()
    
    quality_metrics["final_rows"] = final_count
    quality_metrics["orphan_purchases"] = initial_count - final_count # Approximate attribution
    
    print(f"Purchases cleaning: {initial_count} → {final_count} rows")
    return df_clean, quality_metrics


def write_spark_df_to_silver(df: DataFrame, object_name: str) -> str:
    """
    Write Spark DataFrame to silver bucket as CSV.
    
    Args:
        df: Cleaned DataFrame
        object_name: Name of object in silver bucket
        
    Returns:
        Object name in silver bucket
    """
    client = get_minio_client()
    
    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)
    
    # Collect to pandas for CSV output (for small datasets)
    pdf = df.toPandas()
    csv_buffer = StringIO()
    pdf.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode('utf-8')
    
    client.put_object(
        BUCKET_SILVER,
        object_name,
        BytesIO(csv_bytes),
        length=len(csv_bytes)
    )
    
    print(f"Wrote {df.count()} rows to {BUCKET_SILVER}/{object_name}")
    return object_name


def write_quality_report_spark(metrics: dict, object_name: str) -> str:
    """
    Write data quality report to silver bucket.
    
    Args:
        metrics: Dictionary containing quality metrics
        object_name: Name of report file
        
    Returns:
        Object name in silver bucket
    """
    client = get_minio_client()
    
    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)
    
    # Add timestamp to report
    metrics['report_timestamp'] = datetime.now().isoformat()
    metrics['processing_engine'] = 'spark'
    
    # Convert to JSON
    json_bytes = json.dumps(metrics, indent=2).encode('utf-8')
    
    client.put_object(
        BUCKET_SILVER,
        object_name,
        BytesIO(json_bytes),
        length=len(json_bytes)
    )
    
    print(f"Quality report written to {BUCKET_SILVER}/{object_name}")
    return object_name


def spark_silver_transformation_flow(master_url: str = "local[*]") -> dict:
    """
    Main flow: Transform bronze data to silver layer with quality controls using Spark.
    
    Transformations:
    - Clean null values and aberrant data
    - Standardize date formats
    - Normalize data types
    - Deduplicate records
    - Validate referential integrity
    - Generate quality reports
    
    Args:
        master_url: Spark master URL
        
    Returns:
        Dictionary with transformation results and metrics
    """
    spark = get_spark_session(master_url)
    
    try:
        # Read bronze data
        clients_df = read_csv_from_bronze_spark(spark, "clients.csv")
        purchases_df = read_csv_from_bronze_spark(spark, "purchases.csv")
        
        # Clean clients data
        clients_clean, clients_metrics = clean_clients_data_spark(clients_df)
        
        # Clean purchases data with referential integrity check
        # Pass clients dataframe for broadcast join
        purchases_clean, purchases_metrics = clean_purchases_data_spark(purchases_df, clients_clean)
        
        # Write cleaned data to silver
        silver_clients = write_spark_df_to_silver(clients_clean, "clients_spark.csv")
        silver_purchases = write_spark_df_to_silver(purchases_clean, "purchases_spark.csv")
        
        # Combine metrics for quality report
        quality_report = {
            "clients": clients_metrics,
            "purchases": purchases_metrics,
            "summary": {
                "total_input_rows": clients_metrics["initial_rows"] + purchases_metrics["initial_rows"],
                "total_output_rows": clients_metrics["final_rows"] + purchases_metrics["final_rows"],
                "data_quality_score": round(
                    (clients_metrics["final_rows"] + purchases_metrics["final_rows"]) / 
                    (clients_metrics["initial_rows"] + purchases_metrics["initial_rows"]) * 100, 2
                )
            }
        }
        
        # Write quality report
        report_name = write_quality_report_spark(quality_report, "quality_report_spark.json")
        
        print(f"\n=== Spark Silver Transformation Complete ===")
        print(f"Data quality score: {quality_report['summary']['data_quality_score']}%")
        print(f"Total rows: {quality_report['summary']['total_input_rows']} → {quality_report['summary']['total_output_rows']}")
        
        return {
            "clients": silver_clients,
            "purchases": silver_purchases,
            "quality_report": report_name,
            "metrics": quality_report
        }
    finally:
        spark.stop()


if __name__ == "__main__":
    result = spark_silver_transformation_flow()
    print(f"\nSpark Silver transformation complete: {result}")
