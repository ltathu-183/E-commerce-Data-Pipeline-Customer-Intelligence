# E-Commerce Data Pipeline & Customer Intelligence Platform

**Project Objective**: Build a production-grade data warehouse and analytics platform for an e-commerce business (Olist dataset), with focus on customer intelligence, retention, and revenue optimization.

## 📊 Project Overview

This project demonstrates a **professional data engineering & analytics pipeline** with:
- ✅ Structured ETL pipeline (not messy notebooks)
- ✅ Star schema data modeling with clear grain definition
- ✅ Advanced SQL analytics with JOINs, GROUP BY, WINDOW FUNCTIONS
- ✅ Customer segmentation (RFM + Clustering)
- ✅ Business metrics and KPIs
- ✅ Production-ready architecture

---

## 🏗️ Architecture

### Data Flow
```
Raw CSV Files
    ↓
[EXTRACT] - DataExtractor
    ↓
Staging Tables (raw_*)
    ↓
[TRANSFORM] - DataCleaner + DataTransformer
    ↓
Cleaned & Feature-Engineered Data
    ↓
[LOAD] - DataLoader
    ↓
Analytical Warehouse (Star Schema)
    ├─ Fact Table: fact_order_items
    ├─ Dimensions: dim_customers, dim_products, dim_time, dim_sellers
    └─ Aggregates: agg_customer_metrics, agg_product_metrics, agg_monthly_revenue
    ↓
[ANALYZE]
    ├─ SQL Analytics Queries
    ├─ RFM Segmentation
    ├─ Customer Clustering
    └─ Business Dashboards
```

---

## 📁 Project Structure

```
E-commerce-Data-Pipeline-Customer-Intelligence/
├── README.md                           # This file
├── data/
│   ├── raw/                           # Original CSV files
│   │   ├── olist_customers_dataset.csv
│   │   ├── olist_orders_dataset.csv
│   │   ├── olist_order_items_dataset.csv
│   │   ├── olist_products_dataset.csv
│   │   ├── olist_payments_dataset.csv
│   │   ├── olist_order_reviews_dataset.csv
│   │   ├── olist_sellers_dataset.csv
│   │   ├── olist_geolocation_dataset.csv
│   │   └── product_category_name_translation.csv
│   └── processed/                     # Output from pipeline
│       ├── staging/                   # Stage layer (raw_* tables)
│       ├── dwh/                       # Analytical warehouse
│       └── rfm_segmentation.csv       # RFM results
├── src/
│   ├── etl_pipeline.py               # CORE: Extract-Transform-Load
│   ├── data_modeling.py              # Star schema SQL definitions
│   ├── analytics_queries.py          # Business analytics SQL queries
│   └── clustering_model.py           # (optional) ML clustering
├── notebooks/
│   ├── 1_EDA_schema_exploration.ipynb          # Data understanding
│   ├── 2_RFM_Segmentation.ipynb                # Customer segmentation
│   ├── 3_Customer_Clustering.ipynb             # (optional) Clustering
│   └── 4_Dashboard_and_Insights.ipynb          # (optional) Visualizations
└── reports/
    ├── rfm_segmentation_analysis.png
    ├── metrics.json                  # Key metrics
    └── insights_and_recommendations.md
```

---

## 🔑 Key Design Decisions

### 1. **Fact Table Grain: ITEM-LEVEL** ⭐
- **Why?**: 1 row = 1 product in 1 order (not aggregated)
- **Benefit**: Enables product analysis + flexible aggregation
- **Example**: Order #1001 with 3 items → 3 rows in fact_order_items

```sql
-- Fact table grain
ORDER_ID | ORDER_ITEM_ID | PRODUCT_ID | CUSTOMER_ID | PRICE | FREIGHT_VALUE | TOTAL_VALUE
1001     | 1             | P123       | C456        | 100   | 10            | 110
1001     | 2             | P789       | C456        | 200   | 10            | 210
1001     | 3             | P111       | C456        | 50    | 10            | 60
```

### 2. **Star Schema with Dimensions**
- **Fact Table**: `fact_order_items` (transactional detail)
- **Dimensions**:
  - `dim_customers` (customer demographics)
  - `dim_products` (product catalog)
  - `dim_time` (date hierarchy)
  - `dim_sellers` (merchant info)
- **Aggregates**: Pre-calculated metrics for dashboard performance

### 3. **Metric Definitions** (Critical!)
Clear, consistent definitions prevent ambiguity:

| Metric | Definition | Formula |
|--------|-----------|---------|
| **Revenue** | Total monetary value | SUM(price + freight) |
| **AOV** | Average per transaction | SUM(total_value) / COUNT(orders) |
| **Repeat Rate** | % of repeat customers | COUNT(cust >1 order) / COUNT(unique cust) |
| **CLV** | Total customer spending | SUM(total_value) per customer |

---

## 🚀 How to Run

### Step 1: Run ETL Pipeline
```python
from src.etl_pipeline import ETLPipeline

# Execute complete pipeline
datasets, fact_table = ETLPipeline.run()

# Output:
# - data/processed/staging/raw_*.csv (staging layer)
# - data/processed/dwh/fact_order_items.csv (analytical layer)
```

### Step 2: Load to Database (SQL)
```sql
-- Use data_modeling.py SQL statements to:
-- 1. Create dimension tables
-- 2. Create fact tables
-- 3. Create aggregates and views
```

### Step 3: Run Analytics Queries
```python
from src.analytics_queries import (
    QUERY_TOP_CUSTOMERS_BY_REVENUE,
    QUERY_MONTHLY_REVENUE_TREND,
    QUERY_RFM_SEGMENTATION,
    # ... more queries
)
```

### Step 4: Generate Insights
```bash
# Run RFM segmentation
jupyter notebook notebooks/2_RFM_Segmentation.ipynb

# Optional: Customer clustering
jupyter notebook notebooks/3_Customer_Clustering.ipynb

# View dashboards and insights
jupyter notebook notebooks/4_Dashboard_and_Insights.ipynb
```

---

## 📊 Data Quality Checks

Before loading, the pipeline validates:

✅ **Schema Compliance**
- All critical fields present (order_id, customer_id, price, etc.)
- No unexpected null values

✅ **Referential Integrity**
- All order_ids exist in orders table
- All customer_ids exist in customers table
- All product_ids exist in products table

✅ **Data Consistency**
- Prices are non-negative
- Timestamps are valid ISO format
- Delivery date > purchase date

✅ **Duplicates**
- No duplicate order_ids
- No duplicate customer_ids
- Properly handled multi-item orders

---

## 🎯 Key Metrics & KPIs

### Business Metrics
```
Total Revenue:          R$ X,XXX,XXX
Total Orders:           XX,XXX
Average Order Value:    R$ XXX
Repeat Purchase Rate:   XX%
Unique Customers:       XX,XXX
```

### Segmentation Metrics (RFM)
- **Champions**: VIP customers (High R, F, M)
- **Loyal Customers**: Regular repeat buyers
- **Potential Loyalists**: Recent customers, growing frequency
- **New Customers**: First purchases
- **At Risk**: High-value customers not buying recently
- **Lost**: Haven't purchased in long time

---

## � Data Dictionary

### Fact Table: fact_order_items
**Grain**: One row per order item (product in order).  
**Purpose**: Transactional detail for revenue, delivery, and product analysis.

| Column | Type | Description | Business Meaning | Example |
|--------|------|-------------|------------------|---------|
| order_id | string | Unique order identifier | Links to orders table | 'e481f51cbdc54678b7cc49136f2d6af7' |
| order_item_id | int | Sequential item number in order | Identifies multiple items per order | 1, 2, 3 |
| product_id | string | Unique product identifier | Links to products dimension | '87285b34884572647811a353c7ac498a' |
| seller_id | string | Unique seller identifier | Links to sellers dimension | '3504c0cb71d7fa48d967e0e4c94d59d9' |
| shipping_limit_date | datetime | Latest shipping deadline | Business rule for fulfillment | '2017-09-19 09:45:35' |
| price | float | Product price (BRL) | Revenue component | 29.99 |
| freight_value | float | Shipping cost (BRL) | Revenue component | 8.72 |
| customer_id | string | Unique customer identifier | Links to customers dimension | '9ef432eb6251297304e76186b10a928d' |
| order_status | string | Order lifecycle status | Delivered = revenue realized | 'delivered' |
| order_purchase_timestamp | datetime | When order was placed | Transaction date | '2017-10-02 10:56:33' |
| order_approved_at | datetime | When payment approved | Start of fulfillment | '2017-10-02 10:56:33' |
| order_delivered_carrier_date | datetime | When shipped to carrier | Logistics milestone | '2017-10-04 19:55:00' |
| order_delivered_customer_date | datetime | When delivered to customer | Completion date | '2017-10-10 21:25:13' |
| order_estimated_delivery_date | datetime | Promised delivery date | Customer expectation | '2017-10-18 00:00:00' |
| is_approved | int | Flag: order approved (1=yes, 0=no) | Business state signal | 1 |
| is_shipped | int | Flag: shipped to carrier (1=yes, 0=no) | Business state signal | 1 |
| is_delivered | int | Flag: delivered to customer (1=yes, 0=no) | Business state signal | 1 |
| total_value | float | price + freight_value | Total revenue per item | 38.71 |
| delivery_time_days | float | Days from purchase to delivery | Fulfillment speed (null if not delivered) | 8.44 |
| estimated_delivery_days | float | Days from purchase to estimated delivery | Promised timeline | 15.77 |
| is_delayed | int | 1 if delivery_time_days > estimated_delivery_days (only for delivered) | On-time performance | 0 |
| order_purchase_ym | period | Year-month of purchase | Time aggregation key | '2017-10' |
| etl_loaded_at | datetime | When record was processed | Data lineage | '2024-05-06 12:00:00' |
| data_source | string | Source system | Metadata | 'olist_csv' |

### Dimension: dim_customers
**Grain**: One row per customer.  
**Purpose**: Customer demographics and geography.

| Column | Type | Description | Business Meaning |
|--------|------|-------------|------------------|
| customer_id | string | Unique customer identifier | Primary key |
| customer_unique_id | string | Unique customer ID (anonymized) | De-duplication across orders |
| customer_zip_code_prefix | string | Zip code prefix | Geography |
| customer_city | string | City name | Geography |
| customer_state | string | State code (BR) | Geography |
| etl_loaded_at | datetime | Processing timestamp | Data lineage |
| data_source | string | Source system | Metadata |

### Dimension: dim_products
**Grain**: One row per product.  
**Purpose**: Product catalog with attributes.

| Column | Type | Description | Business Meaning |
|--------|------|-------------|------------------|
| product_id | string | Unique product identifier | Primary key |
| product_category_name | string | Category name (Portuguese) | Product classification |
| category_english | string | Category name (English) | Translated for analysis |
| product_name_lenght | float | Product name length | Text feature |
| product_description_lenght | float | Description length | Text feature |
| product_photos_qty | float | Number of photos | Product richness |
| product_weight_g | float | Weight in grams | Shipping cost factor |
| product_length_cm | float | Length in cm | Packaging |
| product_height_cm | float | Height in cm | Packaging |
| product_width_cm | float | Width in cm | Packaging |
| has_product_info | int | Flag: has category info (1=yes, 0=no) | Missing data signal |
| etl_loaded_at | datetime | Processing timestamp | Data lineage |
| data_source | string | Source system | Metadata |

### Dimension: dim_sellers
**Grain**: One row per seller.  
**Purpose**: Seller information.

| Column | Type | Description | Business Meaning |
|--------|------|-------------|------------------|
| seller_id | string | Unique seller identifier | Primary key |
| seller_zip_code_prefix | string | Zip code prefix | Geography |
| seller_city | string | City name | Geography |
| seller_state | string | State code (BR) | Geography |
| etl_loaded_at | datetime | Processing timestamp | Data lineage |
| data_source | string | Source system | Metadata |

### Dimension: dim_time
**Grain**: One row per date.  
**Purpose**: Time hierarchy for analysis.

| Column | Type | Description | Business Meaning |
|--------|------|-------------|------------------|
| date_id | datetime | Date | Primary key |
| year | int | Year | Time dimension |
| month | int | Month (1-12) | Time dimension |
| day | int | Day of month | Time dimension |
| day_of_week | int | Day of week (0=Mon, 6=Sun) | Time dimension |
| day_name | string | Day name | Time dimension |
| month_name | string | Month name | Time dimension |
| quarter | int | Quarter (1-4) | Time dimension |
| week_of_year | int | ISO week number | Time dimension |
| is_weekend | int | 1 if weekend, 0 otherwise | Time dimension |
| date_key | int | YYYYMMDD format | Alternative key |

### Aggregate: agg_customer_metrics
**Grain**: One row per customer (delivered orders only).  
**Purpose**: Pre-calculated customer KPIs.

| Column | Type | Description | Business Meaning |
|--------|------|-------------|------------------|
| customer_id | string | Customer identifier | Primary key |
| total_orders | int | Number of orders | Purchase frequency |
| total_revenue | float | Sum of total_value | Customer lifetime value |
| avg_order_value | float | Average total_value per order | Spending pattern |
| min_order_value | float | Minimum order value | Range of spending |
| max_order_value | float | Maximum order value | Range of spending |
| avg_delivery_days | float | Average delivery time | Service quality |
| delayed_rate | float | Fraction of delayed orders | On-time performance |
| first_order_date | datetime | Date of first order | Customer tenure start |
| last_order_date | datetime | Date of last order | Recency |
| days_as_customer | int | Days between first and last order | Tenure length |
| etl_loaded_at | datetime | Processing timestamp | Data lineage |

### Aggregate: agg_product_metrics
**Grain**: One row per product (delivered orders only).  
**Purpose**: Pre-calculated product performance.

| Column | Type | Description | Business Meaning |
|--------|------|-------------|------------------|
| product_id | string | Product identifier | Primary key |
| orders_count | int | Number of orders containing product | Popularity |
| units_sold | int | Total quantity sold | Demand |
| total_revenue | float | Sum of total_value | Revenue contribution |
| avg_price | float | Average price | Pricing |
| avg_delivery_days | float | Average delivery time | Supply chain |
| delayed_rate | float | Fraction of delayed deliveries | Performance |
| first_sale_date | datetime | Date of first sale | Product lifecycle |
| last_sale_date | datetime | Date of last sale | Recency |
| etl_loaded_at | datetime | Processing timestamp | Data lineage |

### Aggregate: agg_monthly_revenue
**Grain**: One row per year-month (delivered orders only).  
**Purpose**: Time-series revenue metrics.

| Column | Type | Description | Business Meaning |
|--------|------|-------------|------------------|
| year_month | period | Year-month period | Primary key |
| total_revenue | float | Sum of total_value | Monthly revenue |
| order_count | int | Number of orders | Transaction volume |
| unique_customers | int | Distinct customers | Customer acquisition |
| item_count | int | Total order items | Product movement |
| year | int | Year | Time dimension |
| month | int | Month | Time dimension |
| etl_loaded_at | datetime | Processing timestamp | Data lineage |

---

## 🔍 Decision Log

### Missing Value Handling Strategy
**Decision**: Flag-first approach - create binary flags (e.g., `is_delivered`) before any imputation to preserve business-state signals.  
**Why**: Prevents data leakage in ML models and maintains analytical integrity (e.g., undelivered orders should not be treated as "zero delivery time").  
**Alternatives Considered**: Blind imputation (e.g., mean fill) or drop rows.  
**Impact**: Allows downstream queries to filter by business state (e.g., analyze only delivered orders for revenue metrics).

### Imputation Choices
- **product_weight_g**: Median fill - preserves distribution without outliers affecting shipping cost calculations.
- **order_approved_at**: Fill with `order_purchase_timestamp` - assumes instant approval for missing values, reasonable business assumption.
- **Text fields (review_comment_*)**: Keep null - represents no engagement, no fake text generation.

### Fact Table Grain
**Decision**: Item-level (one row per product in order) instead of order-level aggregation.  
**Why**: Enables product-level analysis and flexible aggregation without losing detail.  
**Trade-off**: Larger table size vs. analytical power.

### Aggregation Tables
**Decision**: Pre-compute customer/product/monthly metrics for dashboard performance.  
**Why**: Avoids expensive GROUP BY queries on large fact tables in real-time dashboards.  
**Implementation**: Only include delivered orders to focus on realized revenue.

---

## �🔍 Recruiter Talking Points

This project demonstrates:

### ✅ Data Understanding
- Schema analysis, relationships, referential integrity
- Data quality validation, missing values handling
- Metric definitions (clear and consistent)

### ✅ ETL & Pipeline Design
- Professional structure (not messy notebooks)
- Clear stages: Extract → Transform → Load
- Error handling and logging

### ✅ Data Modeling
- Star schema with proper grain definition
- Dimension and fact table design
- Primary/foreign keys and constraints

### ✅ Advanced SQL
- Complex JOINs across multiple tables
- GROUP BY aggregations at different levels
- WINDOW FUNCTIONS (ROW_NUMBER, LAG, RANK, NTILE)
- CTEs (Common Table Expressions) for complex logic

### ✅ Business Analytics
- RFM segmentation (customer lifecycle)
- Cohort analysis (retention tracking)
- Revenue trends and forecasting
- Product performance analysis

### ✅ Production Thinking
- Scalable architecture
- Performance optimization (indexes, aggregates)
- Metadata tracking (created_at, updated_at)
- Version control and reproducibility

---

## 🛠️ Technologies Used

| Component | Technology |
|-----------|------------|
| **Data Processing** | Python (pandas, numpy) |
| **Warehousing** | PostgreSQL / MySQL |
| **ETL Orchestration** | Python (production-ready) |
| **Analytics** | SQL (advanced queries) |
| **ML/Segmentation** | scikit-learn, KMeans |
| **Visualization** | matplotlib, seaborn, Plotly |
| **Notebooks** | Jupyter |

---

## 📈 Expected Outputs

### 1. EDA Report (Notebook)
- Schema understanding
- Data quality metrics
- Relationship analysis
- Metric definitions

### 2. RFM Segmentation (CSV)
- Customer segments with R, F, M scores
- Business recommendations per segment
- Visualizations showing segment characteristics

### 3. Analytics Dashboard (SQL + Visuals)
- Revenue trends (monthly, YoY)
- Top products and categories
- Customer retention cohorts
- Delivery performance metrics

### 4. Insights Document
- Key findings with business context
- Actionable recommendations
- Investment priorities

---

## 🎓 Learning Outcomes

This project covers production data engineering concepts:

1. **ETL Best Practices**
   - Staging layer for debugging
   - Data validation and quality checks
   - Feature engineering for analytics

2. **Data Warehouse Design**
   - Star schema benefits
   - Grain definition importance
   - Aggregate tables for performance

3. **SQL Mastery**
   - Multi-table JOINs
   - Window functions for ranking/trends
   - CTEs for complex aggregations

4. **Analytics Thinking**
   - KPI definition and tracking
   - Customer segmentation strategies
   - Business storytelling with data

5. **Python for Data**
   - Pandas for data transformation
   - Logging for production code
   - Modular, reusable design

---

## 📝 Notes for Production

To scale this to production:

1. **Database**: Use PostgreSQL/Snowflake instead of CSVs
2. **Orchestration**: Use Airflow/dbt for scheduling
3. **Monitoring**: Add data quality tests (Great Expectations)
4. **Scaling**: Partition fact table by date, use columnar storage
5. **Version Control**: SQL with version tags, Python with git
6. **Documentation**: Add docstrings, maintain data dictionary

---

## 📞 Contact & Questions

For questions about architecture, approach, or specific implementations, refer to:
- `notebooks/` for exploratory analysis
- `src/` for production code structure
- `reports/` for final outputs and insights

---

**Status**: ✅ Production-Ready Architecture
**Last Updated**: 2024
**Owner**: Data Engineering Team
