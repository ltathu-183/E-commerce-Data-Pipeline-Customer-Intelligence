"""
E-Commerce Data Warehouse: Star Schema Design
==============================================

This module creates the analytical layer of the data warehouse:
1. Dimension tables (dim_customers, dim_products, dim_time)
2. Fact table (fact_order_items) with proper grain definition
3. Aggregate tables for common queries

Key Design Decision - GRAIN: ITEM-LEVEL
- 1 row = 1 product in 1 order
- Enables product analysis and flexible aggregation
- More granular than order-level, allowing precise metrics

Generated SQL statements for database creation.
"""

# SQL statements for PostgreSQL/MySQL

CREATE_STAR_SCHEMA_SQL = """

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- 1. DIM_CUSTOMERS
-- Purpose: Customer master data with geography and demographics
-- Grain: One row per customer
-- Key: customer_id (primary key)

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state VARCHAR(2),
    
    -- Metadata
    record_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT check_state_length CHECK (LENGTH(customer_state) = 2)
);

CREATE INDEX idx_dim_customers_city ON dim_customers(customer_city);
CREATE INDEX idx_dim_customers_state ON dim_customers(customer_state);

COMMENT ON TABLE dim_customers IS 
    'Customer dimension: maps customer_id to demographics and location';
COMMENT ON COLUMN dim_customers.customer_id IS 
    'Primary key: unique customer identifier';
COMMENT ON COLUMN dim_customers.customer_state IS 
    'Brazilian state abbreviation (2 chars)';


-- 2. DIM_PRODUCTS
-- Purpose: Product master data with category hierarchy
-- Grain: One row per product
-- Key: product_id (primary key)

CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100),
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT,
    
    -- Metadata
    record_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT check_weight_positive CHECK (product_weight_g >= 0),
    CONSTRAINT check_photos_positive CHECK (product_photos_qty >= 0)
);

CREATE INDEX idx_dim_products_category ON dim_products(product_category_name);

COMMENT ON TABLE dim_products IS 
    'Product dimension: product characteristics and category';
COMMENT ON COLUMN dim_products.product_id IS 
    'Primary key: unique product identifier';
COMMENT ON COLUMN dim_products.product_category_name IS 
    'Product category in Portuguese';
COMMENT ON COLUMN dim_products.product_category_name_english IS 
    'Product category translated to English';


-- 3. DIM_TIME
-- Purpose: Date hierarchy for time-based analysis
-- Grain: One row per day
-- Key: date_key (integer), date_id (date)

CREATE TABLE IF NOT EXISTS dim_time (
    date_id DATE PRIMARY KEY,
    date_key INT UNIQUE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,  -- 0 = Monday, 6 = Sunday
    day_name VARCHAR(10),
    month_name VARCHAR(10),
    quarter INT,
    week_of_year INT,
    is_weekend BOOLEAN,
    
    CONSTRAINT check_month_range CHECK (month >= 1 AND month <= 12),
    CONSTRAINT check_day_range CHECK (day >= 1 AND day <= 31),
    CONSTRAINT check_dow_range CHECK (day_of_week >= 0 AND day_of_week <= 6),
    CONSTRAINT check_quarter_range CHECK (quarter >= 1 AND quarter <= 4)
);

CREATE INDEX idx_dim_time_year_month ON dim_time(year, month);
CREATE INDEX idx_dim_time_day_of_week ON dim_time(day_of_week);

COMMENT ON TABLE dim_time IS 
    'Time dimension: date hierarchy for temporal analysis';
COMMENT ON COLUMN dim_time.date_key IS 
    'Surrogate key for dates (format: YYYYMMDD)';


-- 4. DIM_SELLERS
-- Purpose: Seller/merchant master data
-- Grain: One row per seller
-- Key: seller_id (primary key)

CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state VARCHAR(2),
    
    -- Metadata
    record_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT check_state_len CHECK (LENGTH(seller_state) = 2)
);

CREATE INDEX idx_dim_sellers_state ON dim_sellers(seller_state);

COMMENT ON TABLE dim_sellers IS 
    'Seller dimension: merchant information and location';


-- ============================================================================
-- FACT TABLE
-- ============================================================================

-- FACT_ORDER_ITEMS
-- Purpose: Transaction-level facts for e-commerce analysis
-- Grain: ITEM-LEVEL (1 row = 1 product in 1 order)
-- 
-- Why item-level?
--   1. Enables product-level analysis (best sellers, category trends)
--   2. Flexible aggregation (can sum to order or customer level)
--   3. Handles multiple items per order naturally
--   4. No data loss from aggregation

CREATE TABLE IF NOT EXISTS fact_order_items (
    -- Business Keys (must relate to dimensions)
    order_id VARCHAR(50) NOT NULL,
    order_item_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    seller_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    order_purchase_date DATE NOT NULL,  -- Foreign key to dim_time
    
    -- Financial Facts (measures)
    price DECIMAL(10, 2) NOT NULL,
    freight_value DECIMAL(10, 2) NOT NULL,
    total_value DECIMAL(10, 2) NOT NULL,  -- price + freight
    
    -- Temporal Facts
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_delivered_timestamp TIMESTAMP,
    order_estimated_delivery_date DATE,
    
    -- Calculated Facts
    delivery_time_days INT,  -- days from purchase to delivery
    estimated_delivery_days INT,  -- estimated delivery time
    is_delayed INT,  -- 1 if late, 0 if on-time
    is_delivered INT,  -- 1 if delivered, 0 otherwise
    
    -- Dimension References (for analysis)
    order_status VARCHAR(20),  -- 'delivered', 'cancelled', etc.
    
    -- Metadata
    record_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
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
    
    CONSTRAINT check_price_positive CHECK (price >= 0),
    CONSTRAINT check_freight_positive CHECK (freight_value >= 0),
    CONSTRAINT check_total_value CHECK (total_value = price + freight_value),
    CONSTRAINT check_is_delayed CHECK (is_delayed IN (0, 1)),
    CONSTRAINT check_is_delivered CHECK (is_delivered IN (0, 1))
);

-- Indexes for query performance
CREATE INDEX idx_fact_customer ON fact_order_items(customer_id);
CREATE INDEX idx_fact_product ON fact_order_items(product_id);
CREATE INDEX idx_fact_seller ON fact_order_items(seller_id);
CREATE INDEX idx_fact_order_date ON fact_order_items(order_purchase_date);
CREATE INDEX idx_fact_order_status ON fact_order_items(order_status);
CREATE INDEX idx_fact_delayed ON fact_order_items(is_delayed);

COMMENT ON TABLE fact_order_items IS 
    'Fact table: E-commerce transactions at item-level. Grain: 1 row = 1 product in 1 order.';
COMMENT ON COLUMN fact_order_items.order_id IS 
    'Order identifier (dimension reference to orders)';
COMMENT ON COLUMN fact_order_items.order_item_id IS 
    'Line item number within order (unique per order)';
COMMENT ON COLUMN fact_order_items.total_value IS 
    'Measure: price + freight (revenue per item)';
COMMENT ON COLUMN fact_order_items.is_delayed IS 
    'Flag: 1 if delivery_time > estimated_delivery_days, else 0';


-- ============================================================================
-- AGGREGATE TABLES (for performance optimization)
-- ============================================================================

-- AGG_CUSTOMER_METRICS
-- Pre-aggregated customer-level metrics for fast querying
-- Updated daily in production

CREATE TABLE IF NOT EXISTS agg_customer_metrics (
    customer_id VARCHAR(50) PRIMARY KEY,
    
    -- Count metrics
    total_orders INT,
    total_items INT,
    
    -- Monetary metrics
    total_revenue DECIMAL(12, 2),
    avg_order_value DECIMAL(10, 2),
    min_order_value DECIMAL(10, 2),
    max_order_value DECIMAL(10, 2),
    
    -- Behavioral metrics
    first_order_date DATE,
    last_order_date DATE,
    days_as_customer INT,
    
    -- Quality metrics
    pct_delivered DECIMAL(5, 2),
    pct_delayed DECIMAL(5, 2),
    avg_delivery_time_days INT,
    
    -- Metadata
    record_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_agg_customer FOREIGN KEY (customer_id) 
        REFERENCES dim_customers(customer_id)
);

CREATE INDEX idx_agg_customer_revenue ON agg_customer_metrics(total_revenue DESC);
CREATE INDEX idx_agg_customer_orders ON agg_customer_metrics(total_orders DESC);

COMMENT ON TABLE agg_customer_metrics IS 
    'Aggregate: Customer-level metrics for dashboard and RFM analysis';


-- AGG_PRODUCT_METRICS
-- Pre-aggregated product performance metrics

CREATE TABLE IF NOT EXISTS agg_product_metrics (
    product_id VARCHAR(50) PRIMARY KEY,
    
    -- Volume metrics
    units_sold INT,
    orders_count INT,
    
    -- Revenue metrics
    total_revenue DECIMAL(12, 2),
    avg_price DECIMAL(10, 2),
    avg_freight_value DECIMAL(8, 2),
    
    -- Performance metrics
    avg_delivery_time_days INT,
    pct_delayed DECIMAL(5, 2),

    -- Temporal
    first_sale_date DATE,
    last_sale_date DATE,

    -- Metadata
    record_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_agg_product FOREIGN KEY (product_id) 
        REFERENCES dim_products(product_id)
);

CREATE INDEX idx_agg_product_revenue ON agg_product_metrics(total_revenue DESC);
CREATE INDEX idx_agg_product_units ON agg_product_metrics(units_sold DESC);

COMMENT ON TABLE agg_product_metrics IS 
    'Aggregate: Product performance metrics for product analytics';


-- AGG_MONTHLY_REVENUE
-- Monthly aggregated revenue for trend analysis

CREATE TABLE IF NOT EXISTS agg_monthly_revenue (
    year INT NOT NULL,
    month INT NOT NULL,
    
    total_revenue DECIMAL(12, 2),
    order_count INT,
    item_count INT,
    unique_customers INT,
    avg_order_value DECIMAL(10, 2),
    
    record_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (year, month),
    CONSTRAINT check_month CHECK (month >= 1 AND month <= 12)
);

COMMENT ON TABLE agg_monthly_revenue IS 
    'Aggregate: Monthly revenue trends for time-series analysis';


-- ============================================================================
-- VIEWS FOR ANALYTICS
-- ============================================================================

-- V_ORDER_METRICS: Order-level aggregation
CREATE OR REPLACE VIEW v_order_metrics AS
SELECT
    order_id,
    customer_id,
    MIN(order_purchase_date) as order_date,
    COUNT(*) as item_count,
    SUM(total_value) as order_total,
    MAX(is_delayed) as is_delayed,
    MIN(order_status) as order_status
FROM fact_order_items
GROUP BY order_id, customer_id;

COMMENT ON VIEW v_order_metrics IS 
    'Order-level aggregation from item-level fact table';


-- V_CUSTOMER_ORDER_HISTORY: Customer purchase pattern
CREATE OR REPLACE VIEW v_customer_order_history AS
SELECT
    customer_id,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(total_value) as lifetime_value,
    MIN(order_purchase_date) as first_order_date,
    MAX(order_purchase_date) as last_order_date,
    AVG(total_value) as avg_order_value
FROM fact_order_items
GROUP BY customer_id;

COMMENT ON VIEW v_customer_order_history IS 
    'Customer summary for RFM analysis and segmentation';

"""

# ============================================================================
# DIMENSION TABLE DATA LOADING SQL
# ============================================================================

POPULATE_DIMENSIONS_SQL = """

-- Load DIM_CUSTOMERS from raw data
INSERT INTO dim_customers (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
)
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM raw_customers
ON CONFLICT (customer_id) DO UPDATE SET
    customer_city = EXCLUDED.customer_city,
    customer_state = EXCLUDED.customer_state,
    record_updated_at = CURRENT_TIMESTAMP;


-- Load DIM_PRODUCTS from raw data
INSERT INTO dim_products (
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
)
SELECT
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    COALESCE(product_weight_g, 0) as product_weight_g,
    COALESCE(product_length_cm, 0) as product_length_cm,
    COALESCE(product_height_cm, 0) as product_height_cm,
    COALESCE(product_width_cm, 0) as product_width_cm
FROM raw_products
WHERE product_category_name IS NOT NULL
ON CONFLICT (product_id) DO UPDATE SET
    product_category_name = EXCLUDED.product_category_name,
    record_created_at = CURRENT_TIMESTAMP;


-- Load DIM_SELLERS from raw data
INSERT INTO dim_sellers (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
)
SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM raw_sellers
ON CONFLICT (seller_id) DO UPDATE SET
    seller_city = EXCLUDED.seller_city,
    seller_state = EXCLUDED.seller_state;

"""

# ============================================================================
# FACT TABLE LOADING SQL
# ============================================================================

POPULATE_FACT_TABLE_SQL = """

-- Load FACT_ORDER_ITEMS from processed data
INSERT INTO fact_order_items (
    order_id,
    order_item_id,
    product_id,
    seller_id,
    customer_id,
    order_purchase_date,
    price,
    freight_value,
    total_value,
    order_purchase_timestamp,
    order_delivered_timestamp,
    order_estimated_delivery_date,
    delivery_time_days,
    estimated_delivery_days,
    is_delayed,
    is_delivered,
    order_status
)
SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    o.customer_id,
    DATE(o.order_purchase_timestamp),
    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value as total_value,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date as order_delivered_timestamp,
    o.order_estimated_delivery_date,
    DATEDIFF(day, o.order_purchase_timestamp, o.order_delivered_customer_date) as delivery_time_days,
    DATEDIFF(day, o.order_purchase_timestamp, o.order_estimated_delivery_date) as estimated_delivery_days,
    CASE 
        WHEN DATEDIFF(day, o.order_purchase_timestamp, o.order_delivered_customer_date) > 
             DATEDIFF(day, o.order_purchase_timestamp, o.order_estimated_delivery_date) 
        THEN 1 
        ELSE 0 
    END as is_delayed,
    CASE WHEN o.order_status = 'delivered' THEN 1 ELSE 0 END as is_delivered,
    o.order_status
FROM raw_order_items oi
JOIN raw_orders o ON oi.order_id = o.order_id;

"""

# ============================================================================
# MODULE EXPORTS
# ============================================================================

if __name__ == "__main__":
    print("E-Commerce Data Warehouse - Star Schema Definition")
    print("=" * 70)
    print("\nSchema Design Summary:")
    print("  - Grain: ITEM-LEVEL (1 row = 1 product in 1 order)")
    print("  - Fact Table: fact_order_items")
    print("  - Dimensions: dim_customers, dim_products, dim_time, dim_sellers")
    print(
        "  - Aggregates: agg_customer_metrics, agg_product_metrics, agg_monthly_revenue"
    )
    print("  - Views: v_order_metrics, v_customer_order_history")
    print("\n✓ Use CREATE_STAR_SCHEMA_SQL to initialize database")
    print("✓ Use POPULATE_DIMENSIONS_SQL to load dimension tables")
    print("✓ Use POPULATE_FACT_TABLE_SQL to load fact table")
