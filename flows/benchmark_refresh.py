import time
import requests
from prefect import flow, task
from gold_aggregation import gold_aggregation_flow
from etl_mongo import etl_mongo_flow

@flow(name="Benchmark Refresh Rate")
def benchmark_refresh_flow():
    """
    Benchmark the complete refresh cycle:
    1. Gold Aggregation (Pandas) -> Generates Parquet
    2. ETL Mongo -> Loads Parquet to MongoDB
    """
    print("=" * 60)
    print("🚀 Starting Refresh Benchmark: Gold -> MongoDB")
    print("=" * 60)
    
    # 1. Measure Gold Generation
    start_gold = time.time()
    gold_result = gold_aggregation_flow()
    end_gold = time.time()
    gold_duration = end_gold - start_gold
    print(f"✓ Gold Generation complete in {gold_duration:.2f}s")
    
    # 2. Measure ETL to MongoDB
    start_etl = time.time()
    etl_result = etl_mongo_flow()
    end_etl = time.time()
    etl_duration = end_etl - start_etl
    print(f"✓ ETL to MongoDB complete in {etl_duration:.2f}s")
    
    # Total Refresh Time
    total_time = gold_duration + etl_duration
    
    print("\n" + "=" * 60)
    print("⏱️  REFRESH PERFORMANCE RESULTS")
    print("=" * 60)
    print(f"1. Gold Generation (Pandas): {gold_duration:.4f}s")
    print(f"2. Data Loading (MinIO -> Mongo): {etl_duration:.4f}s")
    print("-" * 30)
    print(f"⚡ TOTAL REFRESH TIME: {total_time:.4f}s")
    print("=" * 60)
    
    return {
        "gold_duration": gold_duration,
        "etl_duration": etl_duration,
        "total_time": total_time
    }

if __name__ == "__main__":
    benchmark_refresh_flow()
