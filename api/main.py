from fastapi import FastAPI, HTTPException, Query
from pymongo import MongoClient
from typing import List, Optional, Dict, Any
import os
from datetime import datetime

import math

app = FastAPI(title="Analytics API", description="API exposing Gold layer analytics from MongoDB")

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:password@localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "analytics_gold")

# Database Connection
def get_db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB_NAME]

def sanitize_for_json(obj):
    """Recursively replace NaN/Infinity with None for JSON compliance."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    return obj

@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": datetime.now()}

@app.get("/kpis/global")
def get_global_kpis():
    """Get overall metrics calculated during Gold aggregation."""
    db = get_db()
    # We can calculate these on the fly from fact_sales or store them in a summary collection.
    # The ETL stores summary in gold_summary.json but we didn't load that to Mongo yet.
    # We can aggregate from fact_sales or other collections.
    
    # Let's aggregate from fact_sales for real-time feel or read from pre-aggregated collections
    
    total_revenue = 0
    total_orders = 0
    
    # Try reading from daily_sales for faster aggregation
    pipeline = [
        {"$group": {
            "_id": None,
            "total_revenue": {"$sum": "$total_revenue"},
            "total_orders": {"$sum": "$total_orders"},
            "total_quantity": {"$sum": "$total_quantity"}
        }}
    ]
    
    result = list(db.daily_sales.aggregate(pipeline))
    
    if result:
        data = result[0]
        # Get unique customers count from client_kpis
        total_customers = db.client_kpis.count_documents({})
        
        response_data = {
            "total_revenue": round(data.get("total_revenue", 0), 2),
            "total_orders": data.get("total_orders", 0),
            "total_quantity": data.get("total_quantity", 0),
            "total_customers": total_customers,
            "avg_order_value": round(data.get("total_revenue", 0) / data.get("total_orders", 1), 2)
        }
        return sanitize_for_json(response_data)
    return {"error": "No data available"}

@app.get("/sales/temporal")
def get_temporal_sales(period: str = Query("daily", enum=["daily", "weekly", "monthly"])):
    """Get sales evolution over time."""
    db = get_db()
    collection_map = {
        "daily": "daily_sales",
        "weekly": "weekly_sales",
        "monthly": "monthly_sales"
    }
    
    col_name = collection_map[period]
    sort_field = "date" if period == "daily" else "year_week" if period == "weekly" else "year_month"
    
    data = list(db[col_name].find({}, {"_id": 0}).sort(sort_field, 1))
    return sanitize_for_json(data)

@app.get("/products/analytics")
def get_product_analytics(limit: int = 10):
    """Get product performance analytics."""
    db = get_db()
    data = list(db.product_analytics.find({}, {"_id": 0}).sort("total_revenue", -1).limit(limit))
    return sanitize_for_json(data)

@app.get("/countries/analytics")
def get_country_analytics():
    """Get country performance analytics."""
    db = get_db()
    data = list(db.country_analytics.find({}, {"_id": 0}).sort("total_revenue", -1))
    return sanitize_for_json(data)

@app.get("/clients/top")
def get_top_clients(limit: int = 10):
    """Get top spending clients."""
    db = get_db()
    data = list(db.client_kpis.find(
        {}, 
        {"_id": 0, "name": 1, "country": 1, "total_spent": 1, "total_purchases": 1, "last_purchase_date": 1}
    ).sort("total_spent", -1).limit(limit))
    return sanitize_for_json(data)

@app.get("/clients/distribution")
def get_clients_distribution():
    """Get active vs inactive and high value clients counts."""
    db = get_db()
    
    total = db.client_kpis.count_documents({})
    active = db.client_kpis.count_documents({"is_active": True})
    high_value = db.client_kpis.count_documents({"is_high_value": True})
    
    return sanitize_for_json({
        "total": total,
        "active": active,
        "inactive": total - active,
        "high_value": high_value,
        "standard_value": total - high_value
    })
