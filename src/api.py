"""
FastAPI Service for E-Commerce Analytics
=========================================

REST API endpoints for:
- Customer segmentation queries
- Revenue analytics
- Real-time insights

Usage:
    uvicorn api:app --reload
    # API docs: http://localhost:8000/docs
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
from typing import List, Optional, Dict, Any
import logging

from src.config import DatabaseConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="E-Commerce Analytics API",
    description="Real-time customer intelligence and business insights",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database engine
engine = sa.create_engine(DatabaseConfig.get_connection_string())

@app.get("/")
async def root():
    """API health check"""
    return {"message": "E-Commerce Analytics API", "status": "healthy"}

@app.get("/customers/top/{limit}")
async def get_top_customers(limit: int = Query(10, ge=1, le=100)):
    """Get top customers by revenue"""
    try:
        query = text("""
            SELECT
                c.customer_id,
                c.customer_city,
                c.customer_state,
                COUNT(DISTINCT f.order_id) as total_orders,
                COUNT(f.order_item_id) as total_items,
                SUM(f.total_value) as total_revenue,
                AVG(f.total_value) as avg_item_value
            FROM fact_order_items f
            JOIN dim_customers c ON f.customer_id = c.customer_id
            WHERE f.order_status = 'delivered'
            GROUP BY c.customer_id, c.customer_city, c.customer_state
            ORDER BY total_revenue DESC
            LIMIT :limit
        """)

        with engine.connect() as conn:
            result = conn.execute(query, {"limit": limit})
            rows = result.fetchall()

        customers = []
        for row in rows:
            customers.append({
                "customer_id": row[0],
                "city": row[1],
                "state": row[2],
                "total_orders": row[3],
                "total_items": row[4],
                "total_revenue": float(row[5]),
                "avg_item_value": float(row[6])
            })

        return {"customers": customers, "count": len(customers)}

    except Exception as e:
        logger.error(f"Error fetching top customers: {str(e)}")
        raise HTTPException(status_code=500, detail="Database query failed")

@app.get("/revenue/monthly")
async def get_monthly_revenue():
    """Get monthly revenue trends"""
    try:
        query = text("""
            SELECT
                EXTRACT(YEAR FROM f.order_purchase_date) as year,
                EXTRACT(MONTH FROM f.order_purchase_date) as month,
                TO_CHAR(f.order_purchase_date, 'YYYY-MM') as year_month,
                SUM(f.total_value) as revenue,
                COUNT(DISTINCT f.order_id) as orders,
                COUNT(DISTINCT f.customer_id) as customers
            FROM fact_order_items f
            WHERE f.order_status = 'delivered'
            GROUP BY year, month, year_month
            ORDER BY year, month
        """)

        with engine.connect() as conn:
            result = conn.execute(query)
            rows = result.fetchall()

        monthly_data = []
        for row in rows:
            monthly_data.append({
                "year": int(row[0]),
                "month": int(row[1]),
                "year_month": row[2],
                "revenue": float(row[3]),
                "orders": row[4],
                "customers": row[5]
            })

        return {"monthly_revenue": monthly_data}

    except Exception as e:
        logger.error(f"Error fetching monthly revenue: {str(e)}")
        raise HTTPException(status_code=500, detail="Database query failed")

@app.get("/customers/segment/{segment}")
async def get_customers_by_segment(segment: str):
    """Get customers in specific RFM segment"""
    valid_segments = ["Champions", "Loyal Customers", "Potential Loyalists",
                     "New Customers", "At Risk", "Need Attention", "Lost"]

    if segment not in valid_segments:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid segment. Valid segments: {', '.join(valid_segments)}"
        )

    try:
        # This would require RFM segmentation table
        # For now, return placeholder
        return {
            "segment": segment,
            "customer_count": 0,
            "message": "RFM segmentation data not yet implemented in database"
        }

    except Exception as e:
        logger.error(f"Error fetching segment {segment}: {str(e)}")
        raise HTTPException(status_code=500, detail="Database query failed")

@app.get("/products/top/{limit}")
async def get_top_products(limit: int = Query(10, ge=1, le=100)):
    """Get top products by revenue"""
    try:
        query = text("""
            SELECT
                p.product_id,
                p.product_category_name,
                COUNT(f.order_item_id) as items_sold,
                SUM(f.total_value) as total_revenue,
                AVG(f.price) as avg_price
            FROM fact_order_items f
            JOIN dim_products p ON f.product_id = p.product_id
            WHERE f.order_status = 'delivered'
            GROUP BY p.product_id, p.product_category_name
            ORDER BY total_revenue DESC
            LIMIT :limit
        """)

        with engine.connect() as conn:
            result = conn.execute(query, {"limit": limit})
            rows = result.fetchall()

        products = []
        for row in rows:
            products.append({
                "product_id": row[0],
                "category": row[1],
                "items_sold": row[2],
                "total_revenue": float(row[3]),
                "avg_price": float(row[4])
            })

        return {"products": products, "count": len(products)}

    except Exception as e:
        logger.error(f"Error fetching top products: {str(e)}")
        raise HTTPException(status_code=500, detail="Database query failed")

@app.get("/health")
async def health_check():
    """Database connectivity check"""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {"status": "unhealthy", "database": "disconnected", "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)