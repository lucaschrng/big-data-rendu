import os
import pandas as pd
from io import BytesIO
from pymongo import MongoClient
from prefect import flow, task
from datetime import datetime
import time

from config import (
    get_minio_client, 
    BUCKET_GOLD, 
    MONGO_URI, 
    MONGO_DB_NAME
)

@task(name="read_parquet_from_minio")
def read_parquet_from_minio(object_name: str) -> pd.DataFrame:
    """
    Read Parquet file from MinIO Gold bucket.
    """
    client = get_minio_client()
    
    # Check if object exists (Parquet might be a folder or a single file depending on writer)
    # Pandas writes single file. Spark writes folder.
    # We'll assume Pandas single file for now or handle both.
    
    try:
        response = client.get_object(BUCKET_GOLD, object_name)
        data = response.read()
        response.close()
        response.release_conn()
        
        df = pd.read_parquet(BytesIO(data))
        print(f"Read {len(df)} rows from {BUCKET_GOLD}/{object_name}")
        return df
    except Exception as e:
        print(f"Error reading {object_name}: {e}")
        # Try reading as directory/prefix if simple read failed (for Spark parquet folders)
        # This part is complex with MinIO client directly, usually requires listing objects
        # For this exercise, we focus on the single file parquet exported by Pandas pipeline
        raise e

@task(name="load_to_mongodb")
def load_to_mongodb(df: pd.DataFrame, collection_name: str, batch_size: int = 10000):
    """
    Load DataFrame to MongoDB collection.
    """
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    collection = db[collection_name]
    
    # Clear existing data
    collection.delete_many({})
    
    # Convert DataFrame to list of dicts
    # MongoDB doesn't accept NaNs, converting to None/null
    records = df.where(pd.notnull(df), None).to_dict(orient='records')
    
    # Insert in batches
    total_inserted = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        if batch:
            result = collection.insert_many(batch)
            total_inserted += len(result.inserted_ids)
            print(f"Inserted batch {i//batch_size + 1}: {len(batch)} records")
            
    # Create indexes for common lookup fields
    if "id_client" in df.columns:
        collection.create_index("id_client")
    if "id_purchase" in df.columns:
        collection.create_index("id_purchase")
    if "date" in df.columns:
        collection.create_index("date")
    if "product" in df.columns:
        collection.create_index("product")
    if "country" in df.columns:
        collection.create_index("country")
        
    print(f"Successfully loaded {total_inserted} documents into {collection_name}")
    client.close()
    return total_inserted

@flow(name="ETL Gold to MongoDB")
def etl_mongo_flow():
    """
    Pipeline to load Gold data (Parquet) from MinIO to MongoDB.
    """
    print("=" * 60)
    print("Starting ETL: Gold (MinIO) -> MongoDB")
    print("=" * 60)
    
    start_time = time.time()
    
    # List of files to load (Pandas versions)
    files_to_load = [
        ("fact_sales.parquet", "fact_sales"),
        ("client_kpis.parquet", "client_kpis"),
        ("product_analytics.parquet", "product_analytics"),
        ("country_analytics.parquet", "country_analytics"),
        ("daily_sales.parquet", "daily_sales"),
        ("weekly_sales.parquet", "weekly_sales"),
        ("monthly_sales.parquet", "monthly_sales")
    ]
    
    metrics = {}
    
    for filename, collection in files_to_load:
        try:
            print(f"\nProcessing {filename} -> {collection}...")
            df = read_parquet_from_minio(filename)
            count = load_to_mongodb(df, collection)
            metrics[collection] = count
        except Exception as e:
            print(f"Failed to process {filename}: {e}")
            
    end_time = time.time()
    duration = end_time - start_time
    
    print("\n" + "=" * 60)
    print(f"ETL Complete in {duration:.2f} seconds")
    print("=" * 60)
    print("Documents loaded:")
    for col, count in metrics.items():
        print(f"  - {col}: {count}")
        
    return {
        "duration_seconds": duration,
        "metrics": metrics
    }

if __name__ == "__main__":
    etl_mongo_flow()
