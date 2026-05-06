"""
E-Commerce Data Warehouse: COMPLETE SQL Layer
==============================================

✅ UPGRADES:
- Dimension tables with metadata
- Fact table with data lineage
- Aggregation tables for performance
- Analytical views
- Data quality checks
- Production-ready schema

USAGE:
1. Use CREATE_SCHEMA_SQL to set up database
2. Use POPULATE_TABLES_SQL to load data
3. Query ANALYTICAL_VIEWS for business insights
"""

# ============================================================================
# PRODUCTION SCHEMA WITH METADATA TRACKING
# ============================================================================

CREATE_SCHEMA_SQL = """

-- Create schema for clean organization
CREATE SCHEMA IF NOT EXISTS ecommerce;
SET search_path TO ecommerce;

-- ============================================================================
-- DIMENSION TABLES (Complete Star Schema)
-- ============================================================================

-- DIM_CUSTOMERS: Customer master data
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100) NOT NULL,
    customer_state VARCHAR(2) NOT NULL,
    
    -- Metadata (data lineage)
    etl_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    data_source VARCHAR(50),
    
    CONSTRAINT check_state_length CHECK (LENGTH(customer_state) = 2)
);

CREATE INDEX idx_dim_customers_city ON dim_customers(customer_city);
CREATE INDEX idx_dim_customers_state ON dim_customers(customer_state);

COMMENT ON TABLE dim_customers IS 'Customer dimension: demographics and location';
COMMENT ON COLUMN dim_customers.etl_loaded_at IS 'When record was loaded (data lineage)';


-- DIM_PRODUCTS: Product catalog with category
CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    category_english VARCHAR(100),
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g INT DEFAULT 0,
    product_length_cm INT DEFAULT 0,
    product_height_cm INT DEFAULT 0,
    product_width_cm INT DEFAULT 0,
    
    -- Metadata
    etl_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    data_source VARCHAR(50),
    
    CONSTRAINT check_weight CHECK (product_weight_g >= 0),
    CONSTRAINT check_photos CHECK (product_photos_qty >= 0)
);

CREATE INDEX idx_dim_products_category ON dim_products(product_category_name);

COMMENT ON TABLE dim_products IS 'Product dimension: catalog and characteristics';


-- DIM_SELLERS: Merchant data
CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(2),
    
    -- Metadata
    etl_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    data_source VARCHAR(50),
    
    CONSTRAINT check_seller_state CHECK (LENGTH(seller_state) = 2)
);

CREATE INDEX idx_dim_sellers_state ON dim_sellers(seller_state);

COMMENT ON TABLE dim_sellers IS 'Seller dimension: merchant information';


-- DIM_TIME: Date hierarchy for time-based analysis
CREATE TABLE IF NOT EXISTS dim_time (
    date_id DATE PRIMARY KEY,
    date_key INT UNIQUE NOT NULL,  -- YYYYMMDD format for easy sorting
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10),
    month_name VARCHAR(10),
    quarter INT,
    week_of_year INT,
    is_weekend INT,
    
    CONSTRAINT check_month CHECK (month >= 1 AND month <= 12),
    CONSTRAINT check_quarter CHECK (quarter >= 1 AND quarter <= 4)
);

CREATE INDEX idx_dim_time_year_month ON dim_time(year, month);
CREATE INDEX idx_dim_time_date_key ON dim_time(date_key);

COMMENT ON TABLE dim_time IS 'Time dimension: date hierarchy';


-- ============================================================================
-- FACT TABLE (ITEM-LEVEL GRAIN)
-- ============================================================================

/*
⭐ GRAIN: ITEM-LEVEL
  1 row = 1 product in 1 order

WHY?
  ✓ Enables product-level analysis
  ✓ Flexible aggregation
  ✓ Natural for multi-item orders
  ✓ Industry standard approach

EXAMPLE:
  Order #1001 has 3 items → 3 fact rows
*/

CREATE TABLE IF NOT EXISTS fact_order_items (
    -- Business Keys (must relate to dimensions)
    order_id VARCHAR(50) NOT NULL,
    order_item_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    seller_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    order_purchase_date DATE NOT NULL,
    
    -- Financial Measures
    price DECIMAL(10, 2) NOT NULL,
    freight_value DECIMAL(10, 2) NOT NULL DEFAULT 0,
    total_value DECIMAL(10, 2) NOT NULL,  -- price + freight
    
    -- Temporal Measures
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_delivered_timestamp TIMESTAMP,
    order_estimated_delivery_date DATE,
    
    -- Calculated Features
    delivery_time_days INT,
    estimated_delivery_days INT,
    
    -- Flags (instead of nulls)
    is_delayed INT,           -- 1 if late, 0 if on-time
    has_delivery_date INT,    -- 1 if delivered, 0 if null
    is_delivered INT,
    
    -- Dimension References
    order_status VARCHAR(20),
    
    -- Metadata (data lineage)
    etl_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    data_source VARCHAR(50),
    
    -- Constraints
    PRIMARY KEY (order_id, order_item_id),
    CONSTRAINT fk_fact_customer FOREIGN KEY (customer_id) 
        REFERENCES dim_customers(customer_id),
    CONSTRAINT fk_fact_product FOREIGN KEY (product_id) 
        REFERENCES dim_products(product_id),
    CONSTRAINT fk_fact_seller FOREIGN KEY (seller_id) 
        REFERENCES dim_sellers(seller_id),
    CONSTRAINT fk_fact_date FOREIGN KEY (order_purchase_date) 
        REFERENCES dim_time(date_id),
    CONSTRAINT check_price CHECK (price >= 0),
    CONSTRAINT check_freight CHECK (freight_value >= 0),
    CONSTRAINT check_delayed CHECK (is_delayed IN (0, 1)),
    CONSTRAINT check_delivery_flag CHECK (has_delivery_date IN (0, 1))
);

-- Performance indexes
CREATE INDEX idx_fact_customer ON fact_order_items(customer_id);
CREATE INDEX idx_fact_product ON fact_order_items(product_id);
CREATE INDEX idx_fact_seller ON fact_order_items(seller_id);
CREATE INDEX idx_fact_order_date ON fact_order_items(order_purchase_date);
CREATE INDEX idx_fact_order_status ON fact_order_items(order_status);
CREATE INDEX idx_fact_delayed ON fact_order_items(is_delayed);

COMMENT ON TABLE fact_order_items IS 
    'Fact table: Item-level e-commerce transactions. Grain: 1 row = 1 product in 1 order.';
COMMENT ON COLUMN fact_order_items.total_value IS 'Measure: price + freight (revenue)';
COMMENT ON COLUMN fact_order_items.has_delivery_date IS 
    'Flag: 1 if delivered, 0 if null. Use instead of NULL for cleaner queries.';
COMMENT ON COLUMN fact_order_items.etl_loaded_at IS 'Data lineage: when loaded';


-- ============================================================================
-- AGGREGATION TABLES (Performance Layer)
-- ============================================================================

-- AGG_CUSTOMER_METRICS: Pre-calculated customer KPIs
CREATE TABLE IF NOT EXISTS agg_customer_metrics (
    customer_id VARCHAR(50) PRIMARY KEY,
    
    total_orders INT,
    total_revenue DECIMAL(12, 2),
    avg_order_value DECIMAL(10, 2),
    min_order_value DECIMAL(10, 2),
    max_order_value DECIMAL(10, 2),
    
    avg_delivery_days INT,
    delayed_rate DECIMAL(5, 4),
    
    first_order_date DATE,
    last_order_date DATE,
    days_as_customer INT,
    
    etl_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_agg_customer FOREIGN KEY (customer_id) 
        REFERENCES dim_customers(customer_id)
);

CREATE INDEX idx_agg_customer_revenue ON agg_customer_metrics(total_revenue DESC);
CREATE INDEX idx_agg_customer_orders ON agg_customer_metrics(total_orders DESC);

COMMENT ON TABLE agg_customer_metrics IS 
    'Aggregation: Pre-calculated customer-level metrics for dashboard performance';


-- AGG_PRODUCT_METRICS: Pre-calculated product performance
CREATE TABLE IF NOT EXISTS agg_product_metrics (
    product_id VARCHAR(50) PRIMARY KEY,
    
    orders_count INT,
    units_sold INT,
    
    total_revenue DECIMAL(12, 2),
    avg_price DECIMAL(10, 2),
    
    avg_delivery_days INT,
    delayed_rate DECIMAL(5, 4),
    
    first_sale_date DATE,
    last_sale_date DATE,
    
    etl_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_agg_product FOREIGN KEY (product_id) 
        REFERENCES dim_products(product_id)
);

CREATE INDEX idx_agg_product_revenue ON agg_product_metrics(total_revenue DESC);
CREATE INDEX idx_agg_product_units ON agg_product_metrics(units_sold DESC);

COMMENT ON TABLE agg_product_metrics IS 
    'Aggregation: Pre-calculated product performance metrics';


-- AGG_MONTHLY_REVENUE: Monthly aggregation for trends
CREATE TABLE IF NOT EXISTS agg_monthly_revenue (
    year INT NOT NULL,
    month INT NOT NULL,
    
    total_revenue DECIMAL(12, 2),
    order_count INT,
    item_count INT,
    unique_customers INT,
    avg_order_value DECIMAL(10, 2),
    
    etl_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (year, month),
    CONSTRAINT check_month_valid CHECK (month >= 1 AND month <= 12)
);

COMMENT ON TABLE agg_monthly_revenue IS 
    'Aggregation: Monthly revenue trends for time-series analysis';


-- ============================================================================
-- DATA QUALITY MONITORING TABLES
-- ============================================================================

CREATE TABLE IF NOT EXISTS data_quality_log (
    check_id SERIAL PRIMARY KEY,
    check_name VARCHAR(100),
    table_name VARCHAR(50),
    records_checked INT,
    records_failed INT,
    check_result VARCHAR(20),  -- 'PASS', 'WARNING', 'FAIL'
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    details TEXT
);

COMMENT ON TABLE data_quality_log IS 
    'Data quality monitoring: track validation results over time';

"""

# ============================================================================
# ANALYTICAL VIEWS (Easy Access to Common Queries)
# ============================================================================

CREATE_ANALYTICAL_VIEWS_SQL = """

SET search_path TO ecommerce;

-- VIEW 1: Order metrics (aggregated to order level from items)
CREATE OR REPLACE VIEW v_order_metrics AS
SELECT
    f.order_id,
    f.customer_id,
    f.order_purchase_date,
    COUNT(*) as item_count,
    SUM(f.total_value) as order_total,
    MAX(f.is_delayed) as order_delayed,
    MIN(f.order_status) as order_status,
    MIN(f.etl_loaded_at) as etl_loaded_at
FROM fact_order_items f
GROUP BY f.order_id, f.customer_id, f.order_purchase_date;

COMMENT ON VIEW v_order_metrics IS 
    'Order-level view: Aggregates item-level facts to order grain for easier querying';


-- VIEW 2: Customer purchase history (for RFM analysis)
CREATE OR REPLACE VIEW v_customer_history AS
SELECT
    c.customer_id,
    c.customer_city,
    c.customer_state,
    COUNT(DISTINCT f.order_id) as total_purchases,
    SUM(f.total_value) as lifetime_value,
    MIN(f.order_purchase_date) as first_purchase,
    MAX(f.order_purchase_date) as last_purchase,
    CURRENT_DATE - MAX(f.order_purchase_date) as days_since_last_purchase,
    ROUND(AVG(f.total_value), 2) as avg_order_value
FROM fact_order_items f
JOIN dim_customers c ON f.customer_id = c.customer_id
WHERE f.is_delivered = 1
GROUP BY c.customer_id, c.customer_city, c.customer_state;

COMMENT ON VIEW v_customer_history IS 
    'Customer view: Summary for RFM segmentation and customer analysis';


-- VIEW 3: Product performance
CREATE OR REPLACE VIEW v_product_performance AS
SELECT
    p.product_id,
    p.product_category_name,
    COUNT(DISTINCT f.order_id) as orders_sold,
    SUM(CASE WHEN f.is_delivered = 1 THEN 1 ELSE 0 END) as delivered_count,
    SUM(f.total_value) as total_revenue,
    ROUND(AVG(f.total_value), 2) as avg_price,
    ROUND(AVG(f.delivery_time_days), 1) as avg_delivery_days,
    ROUND(AVG(f.is_delayed::numeric), 3) as delayed_rate
FROM fact_order_items f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.product_id, p.product_category_name;

COMMENT ON VIEW v_product_performance IS 
    'Product view: Sales and delivery performance by product';


-- VIEW 4: Monthly sales trend
CREATE OR REPLACE VIEW v_monthly_trend AS
SELECT
    dt.year,
    dt.month,
    dt.month_name,
    COUNT(DISTINCT f.order_id) as orders,
    SUM(f.total_value) as revenue,
    COUNT(DISTINCT f.customer_id) as unique_customers,
    ROUND(SUM(f.total_value) / COUNT(DISTINCT f.order_id), 2) as avg_order_value
FROM fact_order_items f
JOIN dim_time dt ON f.order_purchase_date = dt.date_id
WHERE f.is_delivered = 1
GROUP BY dt.year, dt.month, dt.month_name
ORDER BY dt.year DESC, dt.month DESC;

COMMENT ON VIEW v_monthly_trend IS 
    'Time series view: Monthly revenue and order trends';

"""

# ============================================================================
# DATA QUALITY CHECKS (Production Safety)
# ============================================================================

DATA_QUALITY_CHECKS_SQL = """

SET search_path TO ecommerce;

-- Function to log quality checks
CREATE OR REPLACE FUNCTION log_quality_check(
    p_check_name VARCHAR,
    p_table_name VARCHAR,
    p_total INT,
    p_failed INT,
    p_result VARCHAR,
    p_details TEXT
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO data_quality_log 
        (check_name, table_name, records_checked, records_failed, check_result, details)
    VALUES (p_check_name, p_table_name, p_total, p_failed, p_result, p_details);
END;
$$ LANGUAGE plpgsql;


-- Quality Check 1: Referential Integrity
-- ✅ PRODUCTION: Check that all foreign keys are valid
SELECT 
    'FK_INTEGRITY' as check_type,
    COUNT(*) as orphaned_records
FROM fact_order_items f
WHERE NOT EXISTS (SELECT 1 FROM dim_customers WHERE customer_id = f.customer_id)
   OR NOT EXISTS (SELECT 1 FROM dim_products WHERE product_id = f.product_id)
   OR NOT EXISTS (SELECT 1 FROM dim_sellers WHERE seller_id = f.seller_id);


-- Quality Check 2: NULL in Critical Fields
-- ✅ PRODUCTION: Should be zero
SELECT
    'NULL_CHECKS' as check_type,
    COALESCE(SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END), 0) as null_order_id,
    COALESCE(SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END), 0) as null_customer_id,
    COALESCE(SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END), 0) as null_product_id,
    COALESCE(SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END), 0) as null_price
FROM fact_order_items;


-- Quality Check 3: Negative or Invalid Values
-- ✅ PRODUCTION: Should be zero
SELECT
    'VALUE_VALIDATION' as check_type,
    COUNT(*) as negative_price
FROM fact_order_items
WHERE price < 0 OR freight_value < 0 OR total_value < 0;


-- Quality Check 4: Data Freshness
-- ✅ PRODUCTION: Check if data is recent
SELECT
    'DATA_FRESHNESS' as check_type,
    MAX(etl_loaded_at) as last_load,
    CURRENT_TIMESTAMP - MAX(etl_loaded_at) as hours_since_load
FROM fact_order_items;

"""

if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("E-COMMERCE DATA WAREHOUSE - PRODUCTION SQL")
    print("=" * 80)
    print("\n✅ UPGRADES:")
    print("  1. Complete dimension tables")
    print("  2. Fact table with metadata tracking")
    print("  3. Aggregation tables for performance")
    print("  4. Analytical views for easy querying")
    print("  5. Data quality monitoring")
    print("\n📋 USAGE:")
    print("  1. Run CREATE_SCHEMA_SQL to set up database")
    print("  2. Run etl_pipeline_upgraded.py to populate tables")
    print("  3. Run CREATE_ANALYTICAL_VIEWS_SQL for views")
    print("  4. Query views for business analytics")
    print("\n🔒 PRODUCTION FEATURES:")
    print("  • Foreign key constraints (referential integrity)")
    print("  • Check constraints (data validation)")
    print("  • Indexes for query performance")
    print("  • Metadata columns (etl_loaded_at, data_source)")
    print("  • Data quality monitoring tables")
