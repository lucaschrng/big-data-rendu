import csv
import json
from datetime import datetime
from io import BytesIO, StringIO
from typing import Any

import pandas as pd
from prefect import flow, task

from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client


@task(name="read_from_bronze", retries=2)
def read_csv_from_bronze(object_name: str) -> pd.DataFrame:
    """
    Read CSV data from bronze bucket into pandas DataFrame.
    
    Args:
        object_name: Name of object in bronze bucket
        
    Returns:
        DataFrame with raw data
    """
    client = get_minio_client()
    
    response = client.get_object(BUCKET_BRONZE, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    
    df = pd.read_csv(BytesIO(data))
    print(f"Read {len(df)} rows from {object_name}")
    return df


@task(name="clean_clients_data")
def clean_clients_data(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Clean and transform clients data.
    
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
    initial_count = len(df)
    quality_metrics = {
        "initial_rows": initial_count,
        "null_values": {},
        "duplicates_removed": 0,
        "invalid_dates": 0,
        "invalid_emails": 0,
        "future_dates_fixed": 0,
        "final_rows": 0
    }
    
    # Check for null values per column
    for col in df.columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            quality_metrics["null_values"][col] = int(null_count)
    
    # Remove rows with any null values
    df_clean = df.dropna()
    rows_after_null = len(df_clean)
    
    # Standardize date format and handle future dates
    today = datetime.now().date()
    df_clean['subscription_date'] = pd.to_datetime(df_clean['subscription_date'], errors='coerce')
    
    # Count invalid dates
    invalid_dates = df_clean['subscription_date'].isnull().sum()
    quality_metrics["invalid_dates"] = int(invalid_dates)
    
    # Remove rows with invalid dates
    df_clean = df_clean.dropna(subset=['subscription_date'])
    
    # Fix future dates by capping at today
    future_mask = df_clean['subscription_date'].dt.date > today
    quality_metrics["future_dates_fixed"] = int(future_mask.sum())
    df_clean.loc[future_mask, 'subscription_date'] = pd.Timestamp(today)
    
    # Convert to standard date format (YYYY-MM-DD)
    df_clean['subscription_date'] = df_clean['subscription_date'].dt.strftime('%Y-%m-%d')
    
    # Validate email format (basic check)
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    invalid_emails = ~df_clean['email'].str.match(email_pattern, na=False)
    quality_metrics["invalid_emails"] = int(invalid_emails.sum())
    df_clean = df_clean[~invalid_emails]
    
    # Deduplicate based on id_client (keep first occurrence)
    duplicates_before = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=['id_client'], keep='first')
    quality_metrics["duplicates_removed"] = duplicates_before - len(df_clean)
    
    # Also check for duplicate emails
    email_duplicates = df_clean.duplicated(subset=['email'], keep='first').sum()
    if email_duplicates > 0:
        print(f"Warning: {email_duplicates} duplicate emails found (keeping first occurrence)")
        df_clean = df_clean.drop_duplicates(subset=['email'], keep='first')
        quality_metrics["duplicates_removed"] += email_duplicates
    
    # Normalize data types
    df_clean['id_client'] = df_clean['id_client'].astype(int)
    df_clean['name'] = df_clean['name'].str.strip()
    df_clean['email'] = df_clean['email'].str.lower().str.strip()
    df_clean['country'] = df_clean['country'].str.strip()
    
    quality_metrics["final_rows"] = len(df_clean)
    
    print(f"Clients cleaning: {initial_count} → {len(df_clean)} rows")
    print(f"  - Null values removed: {initial_count - rows_after_null}")
    print(f"  - Invalid dates: {quality_metrics['invalid_dates']}")
    print(f"  - Future dates fixed: {quality_metrics['future_dates_fixed']}")
    print(f"  - Invalid emails: {quality_metrics['invalid_emails']}")
    print(f"  - Duplicates removed: {quality_metrics['duplicates_removed']}")
    
    return df_clean, quality_metrics


@task(name="clean_purchases_data")
def clean_purchases_data(df: pd.DataFrame, valid_client_ids: set) -> tuple[pd.DataFrame, dict]:
    """
    Clean and transform purchases data.
    
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
        valid_client_ids: Set of valid client IDs from cleaned clients data
        
    Returns:
        Tuple of (cleaned DataFrame, quality metrics)
    """
    initial_count = len(df)
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
    
    # Check for null values per column
    for col in df.columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            quality_metrics["null_values"][col] = int(null_count)
    
    # Remove rows with any null values
    df_clean = df.dropna()
    rows_after_null = len(df_clean)
    
    # Standardize date format and handle future dates
    today = datetime.now().date()
    df_clean['purchase_date'] = pd.to_datetime(df_clean['purchase_date'], errors='coerce')
    
    # Count invalid dates
    invalid_dates = df_clean['purchase_date'].isnull().sum()
    quality_metrics["invalid_dates"] = int(invalid_dates)
    
    # Remove rows with invalid dates
    df_clean = df_clean.dropna(subset=['purchase_date'])
    
    # Fix future dates by capping at today
    future_mask = df_clean['purchase_date'].dt.date > today
    quality_metrics["future_dates_fixed"] = int(future_mask.sum())
    df_clean.loc[future_mask, 'purchase_date'] = pd.Timestamp(today)
    
    # Convert to standard date format
    df_clean['purchase_date'] = df_clean['purchase_date'].dt.strftime('%Y-%m-%d')
    
    # Normalize data types
    df_clean['id_purchase'] = df_clean['id_purchase'].astype(int)
    df_clean['id_client'] = df_clean['id_client'].astype(int)
    df_clean['product'] = df_clean['product'].str.strip()
    df_clean['price'] = pd.to_numeric(df_clean['price'], errors='coerce')
    df_clean['quantity'] = pd.to_numeric(df_clean['quantity'], errors='coerce')
    df_clean['total'] = pd.to_numeric(df_clean['total'], errors='coerce')
    
    # Remove rows with invalid numeric values
    df_clean = df_clean.dropna(subset=['price', 'quantity', 'total'])
    
    # Check for negative values
    negative_mask = (df_clean['price'] < 0) | (df_clean['quantity'] < 0)
    quality_metrics["negative_values"] = int(negative_mask.sum())
    df_clean = df_clean[~negative_mask]
    
    # Recalculate total and fix inconsistencies
    calculated_total = (df_clean['price'] * df_clean['quantity']).round(2)
    total_mismatch = (df_clean['total'] != calculated_total)
    quality_metrics["total_recalculated"] = int(total_mismatch.sum())
    df_clean['total'] = calculated_total
    
    # Remove orphan purchases (purchases with invalid client_id)
    orphan_mask = ~df_clean['id_client'].isin(valid_client_ids)
    quality_metrics["orphan_purchases"] = int(orphan_mask.sum())
    df_clean = df_clean[~orphan_mask]
    
    # Deduplicate based on id_purchase (keep first occurrence)
    duplicates_before = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=['id_purchase'], keep='first')
    quality_metrics["duplicates_removed"] = duplicates_before - len(df_clean)
    
    quality_metrics["final_rows"] = len(df_clean)
    
    print(f"Purchases cleaning: {initial_count} → {len(df_clean)} rows")
    print(f"  - Null values removed: {initial_count - rows_after_null}")
    print(f"  - Invalid dates: {quality_metrics['invalid_dates']}")
    print(f"  - Future dates fixed: {quality_metrics['future_dates_fixed']}")
    print(f"  - Negative values removed: {quality_metrics['negative_values']}")
    print(f"  - Totals recalculated: {quality_metrics['total_recalculated']}")
    print(f"  - Orphan purchases removed: {quality_metrics['orphan_purchases']}")
    print(f"  - Duplicates removed: {quality_metrics['duplicates_removed']}")
    
    return df_clean, quality_metrics


@task(name="write_to_silver", retries=2)
def write_csv_to_silver(df: pd.DataFrame, object_name: str) -> str:
    """
    Write cleaned DataFrame to silver bucket as CSV.
    
    Args:
        df: Cleaned DataFrame
        object_name: Name of object in silver bucket
        
    Returns:
        Object name in silver bucket
    """
    client = get_minio_client()
    
    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)
    
    # Convert DataFrame to CSV bytes
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue().encode('utf-8')
    
    client.put_object(
        BUCKET_SILVER,
        object_name,
        BytesIO(csv_bytes),
        length=len(csv_bytes)
    )
    
    print(f"Wrote {len(df)} rows to {BUCKET_SILVER}/{object_name}")
    return object_name


@task(name="write_quality_report")
def write_quality_report(metrics: dict, object_name: str) -> str:
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


@flow(name="Silver Transformation Flow")
def silver_transformation_flow() -> dict:
    """
    Main flow: Transform bronze data to silver layer with quality controls.
    
    Transformations:
    - Clean null values and aberrant data
    - Standardize date formats
    - Normalize data types
    - Deduplicate records
    - Validate referential integrity
    - Generate quality reports
    
    Returns:
        Dictionary with transformation results and metrics
    """
    # Read bronze data
    clients_df = read_csv_from_bronze("clients.csv")
    purchases_df = read_csv_from_bronze("purchases.csv")
    
    # Clean clients data
    clients_clean, clients_metrics = clean_clients_data(clients_df)
    
    # Get valid client IDs for referential integrity
    valid_client_ids = set(clients_clean['id_client'].unique())
    
    # Clean purchases data with referential integrity check
    purchases_clean, purchases_metrics = clean_purchases_data(purchases_df, valid_client_ids)
    
    # Write cleaned data to silver
    silver_clients = write_csv_to_silver(clients_clean, "clients.csv")
    silver_purchases = write_csv_to_silver(purchases_clean, "purchases.csv")
    
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
    report_name = write_quality_report(quality_report, "quality_report.json")
    
    print(f"\n=== Silver Transformation Complete ===")
    print(f"Data quality score: {quality_report['summary']['data_quality_score']}%")
    print(f"Total rows: {quality_report['summary']['total_input_rows']} → {quality_report['summary']['total_output_rows']}")
    
    return {
        "clients": silver_clients,
        "purchases": silver_purchases,
        "quality_report": report_name,
        "metrics": quality_report
    }


if __name__ == "__main__":
    result = silver_transformation_flow()
    print(f"\nSilver transformation complete: {result}")
