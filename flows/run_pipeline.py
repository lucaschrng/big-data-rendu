from bronze_ingestion import bronze_ingestion_flow
from gold_aggregation import gold_aggregation_flow
from silver_transformation import silver_transformation_flow


def run_complete_pipeline(data_dir: str = "./data", include_gold: bool = True):
    """
    Run the complete ELT pipeline: Bronze → Silver → Gold.
    
    Args:
        data_dir: Directory containing source CSV files
        include_gold: Whether to run gold aggregation (default: True)
    """
    print("=" * 80)
    print("Starting Complete ELT Pipeline (Bronze → Silver → Gold)")
    print("=" * 80)
    
    # Step 1: Bronze Ingestion
    print("\n[STEP 1/3] Bronze Ingestion - Raw Data Lake")
    print("-" * 80)
    bronze_result = bronze_ingestion_flow(data_dir)
    print(f"✓ Bronze ingestion complete: {bronze_result}")
    
    # Step 2: Silver Transformation
    print("\n[STEP 2/3] Silver Transformation - Data Quality & Cleaning")
    print("-" * 80)
    silver_result = silver_transformation_flow()
    print(f"✓ Silver transformation complete")
    
    # Step 3: Gold Aggregation
    gold_result = None
    if include_gold:
        print("\n[STEP 3/3] Gold Aggregation - Business Metrics & KPIs")
        print("-" * 80)
        gold_result = gold_aggregation_flow()
        print(f"✓ Gold aggregation complete")
    else:
        print("\n[STEP 3/3] Gold Aggregation - Skipped")
    
    # Final Summary
    print("\n" + "=" * 80)
    print("🎉 Pipeline Complete!")
    print("=" * 80)
    
    print(f"\n📊 Data Quality Metrics (Silver Layer):")
    print(f"  - Data Quality Score: {silver_result['metrics']['summary']['data_quality_score']}%")
    print(f"  - Input Rows: {silver_result['metrics']['summary']['total_input_rows']}")
    print(f"  - Output Rows: {silver_result['metrics']['summary']['total_output_rows']}")
    
    print(f"\n👥 Clients:")
    print(f"  - Initial: {silver_result['metrics']['clients']['initial_rows']}")
    print(f"  - Final: {silver_result['metrics']['clients']['final_rows']}")
    print(f"  - Duplicates removed: {silver_result['metrics']['clients']['duplicates_removed']}")
    print(f"  - Future dates fixed: {silver_result['metrics']['clients']['future_dates_fixed']}")
    
    print(f"\n🛒 Purchases:")
    print(f"  - Initial: {silver_result['metrics']['purchases']['initial_rows']}")
    print(f"  - Final: {silver_result['metrics']['purchases']['final_rows']}")
    print(f"  - Duplicates removed: {silver_result['metrics']['purchases']['duplicates_removed']}")
    print(f"  - Future dates fixed: {silver_result['metrics']['purchases']['future_dates_fixed']}")
    print(f"  - Orphan purchases removed: {silver_result['metrics']['purchases']['orphan_purchases']}")
    print(f"  - Totals recalculated: {silver_result['metrics']['purchases']['total_recalculated']}")
    
    if gold_result:
        print(f"\n💰 Business Metrics (Gold Layer):")
        print(f"  - Total Clients: {gold_result['kpis']['total_clients']}")
        print(f"  - Active Clients: {gold_result['kpis']['active_clients']}")
        print(f"  - High Value Clients: {gold_result['kpis']['high_value_clients']}")
        print(f"  - Total Products: {gold_result['kpis']['total_products']}")
        print(f"  - Total Countries: {gold_result['kpis']['total_countries']}")
        
        print(f"\n🏆 Top Performers:")
        print(f"  - Best Product: {gold_result['top_performers']['top_product_by_revenue']}")
        print(f"  - Best Country: {gold_result['top_performers']['top_country_by_revenue']}")
        print(f"  - Top Client: {gold_result['top_performers']['top_client_by_spending']}")
        
        print(f"\n📈 Aggregations Created:")
        print(f"  - Daily records: {gold_result['temporal_aggregations']['daily_records']}")
        print(f"  - Weekly records: {gold_result['temporal_aggregations']['weekly_records']}")
        print(f"  - Monthly records: {gold_result['temporal_aggregations']['monthly_records']}")
    
    print("\n" + "=" * 80)
    
    return {
        "bronze": bronze_result,
        "silver": silver_result,
        "gold": gold_result
    }


if __name__ == "__main__":
    result = run_complete_pipeline()
