"""
Benchmark script to compare Pandas vs PySpark processing times.
Runs both pipelines and measures execution time for each step.
"""
import os
import time
import json
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Callable, Any

# Change to project root directory for consistent paths
os.chdir(Path(__file__).parent.parent)

from config import BUCKET_BRONZE, get_minio_client

# Import Pandas pipeline
from bronze_ingestion import bronze_ingestion_flow
from silver_transformation import silver_transformation_flow
from gold_aggregation import gold_aggregation_flow

# Spark imports are done lazily to avoid Java errors at startup


def measure_time(func: Callable, *args, **kwargs) -> tuple[Any, float]:
    """
    Measure execution time of a function.
    
    Args:
        func: Function to measure
        *args: Positional arguments
        **kwargs: Keyword arguments
        
    Returns:
        Tuple of (result, execution_time_seconds)
    """
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    end_time = time.perf_counter()
    execution_time = end_time - start_time
    return result, execution_time


def run_pandas_pipeline(data_dir: str = "./data") -> dict:
    """
    Run the complete Pandas pipeline and measure time for each step.
    
    Args:
        data_dir: Directory containing source CSV files
        
    Returns:
        Dictionary with results and timing
    """
    print("\n" + "=" * 80)
    print("🐼 PANDAS PIPELINE")
    print("=" * 80)
    
    timing = {
        "bronze_ingestion": 0,
        "silver_transformation": 0,
        "gold_aggregation": 0,
        "total": 0
    }
    
    total_start = time.perf_counter()
    
    # Bronze Ingestion
    print("\n[1/3] Bronze Ingestion...")
    bronze_result, timing["bronze_ingestion"] = measure_time(bronze_ingestion_flow, data_dir)
    print(f"✓ Bronze completed in {timing['bronze_ingestion']:.4f} seconds")
    
    # Silver Transformation
    print("\n[2/3] Silver Transformation...")
    silver_result, timing["silver_transformation"] = measure_time(silver_transformation_flow)
    print(f"✓ Silver completed in {timing['silver_transformation']:.4f} seconds")
    
    # Gold Aggregation
    print("\n[3/3] Gold Aggregation...")
    gold_result, timing["gold_aggregation"] = measure_time(gold_aggregation_flow)
    print(f"✓ Gold completed in {timing['gold_aggregation']:.4f} seconds")
    
    timing["total"] = time.perf_counter() - total_start
    
    print(f"\n📊 Pandas Total Time: {timing['total']:.4f} seconds")
    
    return {
        "engine": "pandas",
        "timing": timing,
        "results": {
            "bronze": bronze_result,
            "silver": silver_result,
            "gold": gold_result
        }
    }


def check_java_available() -> bool:
    """Check if Java is available for PySpark."""
    import subprocess
    import shutil
    
    # First check if java binary exists in PATH
    if shutil.which('java') is None:
        return False
    
    # Then verify it actually works
    try:
        result = subprocess.run(
            ['java', '-version'], 
            capture_output=True, 
            text=True,
            timeout=5
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired, Exception):
        return False


def run_spark_pipeline(data_dir: str = "./data", master_url: str = "local[*]") -> dict:
    """
    Run the complete Spark pipeline and measure time for each step.
    
    Note: Bronze ingestion is shared (same for both pipelines).
    
    Args:
        data_dir: Directory containing source CSV files
        master_url: Spark master URL
        
    Returns:
        Dictionary with results and timing
    """
    print("\n" + "=" * 80)
    print("⚡ SPARK PIPELINE")
    print("=" * 80)
    
    # Check Java availability
    if not check_java_available():
        print("\n⚠️  Java non disponible - PySpark nécessite Java pour fonctionner")
        print("\n💡 Options:")
        print("   1. Installer Java: brew install openjdk@17")
        print("   2. Utiliser le cluster Docker (voir README)")
        print("\n   Pour exécuter Spark dans Docker:")
        print("   docker-compose exec spark-master /opt/spark/bin/spark-submit \\")
        print("       --master spark://spark-master:7077 \\")
        print("       /opt/spark/data/spark_benchmark.py")
        
        return {
            "engine": "spark",
            "master_url": master_url,
            "timing": {
                "bronze_ingestion": 0,
                "silver_transformation": 0,
                "gold_aggregation": 0,
                "total": 0
            },
            "results": {
                "silver": {"metrics": {"summary": {"data_quality_score": "N/A"}}},
                "gold": None
            },
            "error": "Java not available"
        }
    
    timing = {
        "bronze_ingestion": 0,
        "silver_transformation": 0,
        "gold_aggregation": 0,
        "total": 0
    }
    
    total_start = time.perf_counter()
    
    # Bronze Ingestion (same as Pandas - just file upload)
    print("\n[1/3] Bronze Ingestion (shared with Pandas)...")
    # Check if bronze data exists, if not run bronze ingestion
    client = get_minio_client()
    try:
        client.stat_object(BUCKET_BRONZE, "clients.csv")
        print("✓ Bronze data already exists, skipping ingestion")
        timing["bronze_ingestion"] = 0
    except Exception:
        bronze_result, timing["bronze_ingestion"] = measure_time(bronze_ingestion_flow, data_dir)
        print(f"✓ Bronze completed in {timing['bronze_ingestion']:.4f} seconds")
    
    # Import Spark modules only when Java is available
    from spark_silver_transformation import spark_silver_transformation_flow
    from spark_gold_aggregation import spark_gold_aggregation_flow
    
    # Silver Transformation (Spark)
    print("\n[2/3] Silver Transformation (Spark)...")
    silver_result, timing["silver_transformation"] = measure_time(
        spark_silver_transformation_flow, master_url
    )
    print(f"✓ Spark Silver completed in {timing['silver_transformation']:.4f} seconds")
    
    # Gold Aggregation (Spark)
    print("\n[3/3] Gold Aggregation (Spark)...")
    gold_result, timing["gold_aggregation"] = measure_time(
        spark_gold_aggregation_flow, master_url
    )
    print(f"✓ Spark Gold completed in {timing['gold_aggregation']:.4f} seconds")
    
    timing["total"] = time.perf_counter() - total_start
    
    print(f"\n📊 Spark Total Time: {timing['total']:.4f} seconds")
    
    return {
        "engine": "spark",
        "master_url": master_url,
        "timing": timing,
        "results": {
            "silver": silver_result,
            "gold": gold_result
        }
    }


def run_benchmark(
    data_dir: str = "./data",
    spark_master_url: str = "local[*]",
    save_results: bool = True
) -> dict:
    """
    Run complete benchmark comparing Pandas vs Spark.
    
    Args:
        data_dir: Directory containing source CSV files
        spark_master_url: Spark master URL (local[*] for local mode, spark://master:7077 for cluster)
        save_results: Whether to save results to file
        
    Returns:
        Dictionary with benchmark results
    """
    print("\n" + "#" * 80)
    print("#" + " " * 78 + "#")
    print("#" + "  BENCHMARK: PANDAS vs SPARK  ".center(78) + "#")
    print("#" + " " * 78 + "#")
    print("#" * 80)
    
    benchmark_start = datetime.now()
    
    # Run Pandas pipeline
    pandas_results = run_pandas_pipeline(data_dir)
    
    # Run Spark pipeline
    spark_results = run_spark_pipeline(data_dir, spark_master_url)
    
    # Check if Spark failed (no Java)
    spark_failed = spark_results.get("error") is not None
    
    # Calculate comparison
    if spark_failed:
        comparison = {
            "silver_speedup": "N/A",
            "gold_speedup": "N/A", 
            "total_speedup": "N/A",
            "pandas_faster_by": "N/A",
            "winner": "pandas",
            "winner_reason": "Spark non disponible (Java manquant)"
        }
    else:
        comparison = {
            "silver_speedup": pandas_results["timing"]["silver_transformation"] / spark_results["timing"]["silver_transformation"] if spark_results["timing"]["silver_transformation"] > 0 else float('inf'),
            "gold_speedup": pandas_results["timing"]["gold_aggregation"] / spark_results["timing"]["gold_aggregation"] if spark_results["timing"]["gold_aggregation"] > 0 else float('inf'),
            "total_speedup": pandas_results["timing"]["total"] / spark_results["timing"]["total"] if spark_results["timing"]["total"] > 0 else float('inf'),
            "pandas_faster_by": spark_results["timing"]["total"] - pandas_results["timing"]["total"],
        }
        
        # Determine winner
        if pandas_results["timing"]["total"] < spark_results["timing"]["total"]:
            comparison["winner"] = "pandas"
            comparison["winner_reason"] = "Lower overhead for small datasets"
        else:
            comparison["winner"] = "spark"
            comparison["winner_reason"] = "Distributed processing advantage"
    
    # Build final results
    results = {
        "benchmark_timestamp": benchmark_start.isoformat(),
        "spark_master_url": spark_master_url,
        "pandas": {
            "timing": pandas_results["timing"],
            "data_quality_score": pandas_results["results"]["silver"]["metrics"]["summary"]["data_quality_score"]
        },
        "spark": {
            "timing": spark_results["timing"],
            "data_quality_score": spark_results["results"]["silver"]["metrics"]["summary"]["data_quality_score"]
        },
        "comparison": comparison
    }
    
    # Print summary
    print("\n" + "=" * 80)
    print("📊 BENCHMARK RESULTS SUMMARY")
    print("=" * 80)
    
    if spark_failed:
        print("\n┌─────────────────────────────────────────────────────────────────────────────┐")
        print("│                        TIMING COMPARISON (seconds)                          │")
        print("├───────────────────────┬───────────────────┬───────────────────┬─────────────┤")
        print("│ Step                  │ Pandas            │ Spark             │ Speedup     │")
        print("├───────────────────────┼───────────────────┼───────────────────┼─────────────┤")
        print(f"│ Bronze Ingestion      │ {pandas_results['timing']['bronze_ingestion']:>17.4f} │ {'N/A':>17} │ {'N/A':>11} │")
        print(f"│ Silver Transformation │ {pandas_results['timing']['silver_transformation']:>17.4f} │ {'N/A':>17} │ {'N/A':>11} │")
        print(f"│ Gold Aggregation      │ {pandas_results['timing']['gold_aggregation']:>17.4f} │ {'N/A':>17} │ {'N/A':>11} │")
        print("├───────────────────────┼───────────────────┼───────────────────┼─────────────┤")
        print(f"│ TOTAL                 │ {pandas_results['timing']['total']:>17.4f} │ {'N/A':>17} │ {'N/A':>11} │")
        print("└───────────────────────┴───────────────────┴───────────────────┴─────────────┘")
        print("\n⚠️  Spark non exécuté - Java requis")
    else:
        print("\n┌─────────────────────────────────────────────────────────────────────────────┐")
        print("│                        TIMING COMPARISON (seconds)                          │")
        print("├───────────────────────┬───────────────────┬───────────────────┬─────────────┤")
        print("│ Step                  │ Pandas            │ Spark             │ Speedup     │")
        print("├───────────────────────┼───────────────────┼───────────────────┼─────────────┤")
        print(f"│ Bronze Ingestion      │ {pandas_results['timing']['bronze_ingestion']:>17.4f} │ {spark_results['timing']['bronze_ingestion']:>17.4f} │ {'N/A':>11} │")
        print(f"│ Silver Transformation │ {pandas_results['timing']['silver_transformation']:>17.4f} │ {spark_results['timing']['silver_transformation']:>17.4f} │ {comparison['silver_speedup']:>10.2f}x │")
        print(f"│ Gold Aggregation      │ {pandas_results['timing']['gold_aggregation']:>17.4f} │ {spark_results['timing']['gold_aggregation']:>17.4f} │ {comparison['gold_speedup']:>10.2f}x │")
        print("├───────────────────────┼───────────────────┼───────────────────┼─────────────┤")
        print(f"│ TOTAL                 │ {pandas_results['timing']['total']:>17.4f} │ {spark_results['timing']['total']:>17.4f} │ {comparison['total_speedup']:>10.2f}x │")
        print("└───────────────────────┴───────────────────┴───────────────────┴─────────────┘")
        
        print(f"\n🏆 Winner: {comparison['winner'].upper()}")
        print(f"   Reason: {comparison['winner_reason']}")
        
        if comparison['pandas_faster_by'] > 0:
            print(f"   Pandas was {abs(comparison['pandas_faster_by']):.4f}s faster")
        else:
            print(f"   Spark was {abs(comparison['pandas_faster_by']):.4f}s faster")
    
    print("\n💡 Note: Spark has startup overhead. With larger datasets (millions of rows),")
    print("   Spark's distributed processing will show significant performance gains.")
    
    # Save results
    if save_results:
        results_file = Path(data_dir) / "benchmark_results.json"
        with open(results_file, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\n📁 Results saved to: {results_file}")
    
    return results


def run_pandas_only(data_dir: str = "./data") -> dict:
    """Run only the Pandas pipeline for benchmarking."""
    return run_pandas_pipeline(data_dir)


def run_spark_only(data_dir: str = "./data", master_url: str = "local[*]") -> dict:
    """Run only the Spark pipeline for benchmarking."""
    return run_spark_pipeline(data_dir, master_url)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Benchmark Pandas vs Spark ELT Pipeline")
    parser.add_argument(
        "--data-dir",
        type=str,
        default="./data",
        help="Directory containing source CSV files"
    )
    parser.add_argument(
        "--spark-master",
        type=str,
        default="local[*]",
        help="Spark master URL (default: local[*], use spark://spark-master:7077 for cluster)"
    )
    parser.add_argument(
        "--pandas-only",
        action="store_true",
        help="Run only Pandas pipeline"
    )
    parser.add_argument(
        "--spark-only",
        action="store_true",
        help="Run only Spark pipeline"
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save results to file"
    )
    
    args = parser.parse_args()
    
    if args.pandas_only:
        run_pandas_only(args.data_dir)
    elif args.spark_only:
        run_spark_only(args.data_dir, args.spark_master)
    else:
        run_benchmark(
            data_dir=args.data_dir,
            spark_master_url=args.spark_master,
            save_results=not args.no_save
        )
