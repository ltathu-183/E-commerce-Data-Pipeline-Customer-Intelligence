"""
E-Commerce Analytics: SQL Business Queries
============================================

Advanced SQL queries demonstrating:
✓ Complex JOINs across multiple tables
✓ GROUP BY aggregations at different grains
✓ WINDOW FUNCTIONS for ranking and trends
✓ Subqueries and CTEs for complex logic
✓ Business metric calculations

These queries answer key business questions for dashboards and reports.
"""

# ==============================================================================
# 1. REVENUE & SALES METRICS
# ==============================================================================

# Query 1.1: Top 10 Customers by Revenue
QUERY_TOP_CUSTOMERS_BY_REVENUE = """
SELECT
    c.customer_id,
    c.customer_city,
    c.customer_state,
    COUNT(DISTINCT f.order_id) as total_orders,
    COUNT(f.order_item_id) as total_items,
    SUM(f.total_value) as total_revenue,
    AVG(f.total_value) as avg_item_value,
    MIN(f.order_purchase_date) as first_order_date,
    MAX(f.order_purchase_date) as last_order_date,
    (MAX(f.order_purchase_date) - MIN(f.order_purchase_date)) as customer_lifetime_days
FROM fact_order_items f
JOIN dim_customers c ON f.customer_id = c.customer_id
WHERE f.order_status = 'delivered'
GROUP BY c.customer_id, c.customer_city, c.customer_state
ORDER BY total_revenue DESC
LIMIT 10;

-- Business Insight: Identifies VIP customers for retention strategy
-- Metric: Total Revenue (sum of price + freight)
"""

# Query 1.2: Monthly Revenue Trend with YoY Comparison
QUERY_MONTHLY_REVENUE_TREND = """
WITH monthly_revenue AS (
    SELECT
        EXTRACT(YEAR FROM f.order_purchase_date) as year,
        EXTRACT(MONTH FROM f.order_purchase_date) as month,
        TO_CHAR(f.order_purchase_date, 'YYYY-MM') as year_month,
        SUM(f.total_value) as revenue,
        COUNT(DISTINCT f.order_id) as order_count,
        COUNT(DISTINCT f.customer_id) as unique_customers,
        AVG(f.total_value) as avg_item_value,
        COUNT(f.order_item_id) as item_count
    FROM fact_order_items f
    WHERE f.order_status = 'delivered'
    GROUP BY EXTRACT(YEAR FROM f.order_purchase_date), EXTRACT(MONTH FROM f.order_purchase_date), TO_CHAR(f.order_purchase_date, 'YYYY-MM')
)
SELECT
    year_month,
    year,
    month,
    revenue,
    order_count,
    unique_customers,
    item_count,
    avg_item_value,
    -- WINDOW FUNCTION: Calculate MoM growth
    LAG(revenue) OVER (ORDER BY year, month) as prev_month_revenue,
    ROUND(
        ((revenue - LAG(revenue) OVER (ORDER BY year, month)) /
         NULLIF(LAG(revenue) OVER (ORDER BY year, month), 0) * 100), 2
    ) as mom_growth_pct,
    -- WINDOW FUNCTION: Calculate YoY growth
    LAG(revenue) OVER (PARTITION BY month ORDER BY year) as prev_year_same_month,
    ROUND(
        ((revenue - LAG(revenue) OVER (PARTITION BY month ORDER BY year)) /
         NULLIF(LAG(revenue) OVER (PARTITION BY month ORDER BY year), 0) * 100), 2
    ) as yoy_growth_pct,
    -- WINDOW FUNCTION: Running total
    SUM(revenue) OVER (ORDER BY year, month) as cumulative_revenue
FROM monthly_revenue
ORDER BY year DESC, month DESC;

-- Business Insight: Identifies seasonal patterns and growth trends
-- Metrics: Revenue, Order Count, YoY/MoM Growth %, Cumulative Revenue
"""

# Query 1.3: Average Order Value (AOV) Analysis
QUERY_AOV_ANALYSIS = """
WITH order_totals AS (
    SELECT
        f.order_id,
        f.customer_id,
        f.order_purchase_date,
        MONTH(f.order_purchase_date) as month,
        YEAR(f.order_purchase_date) as year,
        SUM(f.total_value) as order_value,
        COUNT(f.order_item_id) as item_count
    FROM fact_order_items f
    WHERE f.order_status = 'delivered'
    GROUP BY f.order_id, f.customer_id, f.order_purchase_date, MONTH(f.order_purchase_date), YEAR(f.order_purchase_date)
)
SELECT
    year,
    month,
    COUNT(DISTINCT order_id) as total_orders,
    COUNT(DISTINCT customer_id) as unique_customers,
    ROUND(AVG(order_value), 2) as aov,
    ROUND(STDEV(order_value), 2) as aov_stddev,
    MIN(order_value) as min_order,
    MAX(order_value) as max_order,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY order_value) OVER (PARTITION BY year, month), 2) as median_aov,
    -- WINDOW FUNCTION: Rank months by AOV
    RANK() OVER (ORDER BY AVG(order_value) DESC) as aov_rank,
    -- WINDOW FUNCTION: Compare to average
    ROUND(AVG(order_value) - AVG(AVG(order_value)) OVER (), 2) as aov_vs_average
FROM order_totals
GROUP BY year, month
ORDER BY year DESC, month DESC;

-- Business Insight: Tracks AOV trends to identify pricing and bundling opportunities
-- Metric: AOV = Total Revenue / Number of Orders
"""

# ==============================================================================
# 2. CUSTOMER SEGMENTATION & RETENTION
# ==============================================================================

# Query 2.1: Repeat Purchase Rate and Customer Classification
QUERY_REPEAT_PURCHASE_RATE = """
WITH customer_purchases AS (
    SELECT
        f.customer_id,
        COUNT(DISTINCT f.order_id) as purchase_count,
        SUM(f.total_value) as total_spent,
        MIN(f.order_purchase_date) as first_purchase_date,
        MAX(f.order_purchase_date) as last_purchase_date,
        DATEDIFF(day, MIN(f.order_purchase_date), MAX(f.order_purchase_date)) as customer_lifetime_days
    FROM fact_order_items f
    WHERE f.order_status = 'delivered'
    GROUP BY f.customer_id
)
SELECT
    COUNT(*) as total_customers,
    SUM(CASE WHEN purchase_count = 1 THEN 1 ELSE 0 END) as one_time_buyers,
    SUM(CASE WHEN purchase_count > 1 THEN 1 ELSE 0 END) as repeat_customers,
    ROUND(100.0 * SUM(CASE WHEN purchase_count > 1 THEN 1 ELSE 0 END) / COUNT(*), 2) as repeat_purchase_rate_pct,
    ROUND(AVG(purchase_count), 2) as avg_purchases_per_customer,
    ROUND(AVG(total_spent), 2) as avg_customer_lifetime_value,
    -- WINDOW FUNCTION: Customer segments
    SUM(CASE
        WHEN purchase_count > 5 THEN total_spent
        ELSE 0
    END) as revenue_from_high_frequency_customers,
    MAX(purchase_count) as max_purchases_by_single_customer
FROM customer_purchases;

-- Business Insight: Repeat Purchase Rate is key retention metric (target >30%)
-- Metric: Repeat Purchase Rate % = Customers with >1 order / Total Customers * 100
"""

# Query 2.2: Customer Cohort Analysis (Cohort Retention)
QUERY_COHORT_ANALYSIS = """
WITH customer_cohorts AS (
    -- Assign customers to cohorts based on first purchase month
    SELECT
        f.customer_id,
        FORMAT(MIN(f.order_purchase_date), 'yyyy-MM') as cohort_month,
        FORMAT(f.order_purchase_date, 'yyyy-MM') as order_month,
        DATEDIFF(month, MIN(f.order_purchase_date) OVER (PARTITION BY f.customer_id), f.order_purchase_date) as months_since_first_purchase
    FROM fact_order_items f
    WHERE f.order_status = 'delivered'
)
SELECT
    cohort_month,
    months_since_first_purchase,
    COUNT(DISTINCT customer_id) as customers_in_cohort,
    ROUND(
        100.0 * COUNT(DISTINCT customer_id) /
        (SELECT COUNT(DISTINCT customer_id) FROM customer_cohorts WHERE cohort_month = c.cohort_month),
        2
    ) as cohort_retention_pct
FROM customer_cohorts c
GROUP BY cohort_month, months_since_first_purchase
HAVING DATEDIFF(month, MIN(DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)),
                FORMAT(GETDATE(), 'yyyy-MM')) >= months_since_first_purchase
ORDER BY cohort_month, months_since_first_purchase;

-- Business Insight: Tracks how customer retention evolves over time
-- Shows which cohorts have best long-term retention (product-market fit indicator)
"""

# Query 2.3: RFM Segmentation Base Data
QUERY_RFM_SEGMENTATION = """
WITH rfm_base AS (
    SELECT
        f.customer_id,
        c.customer_city,
        c.customer_state,

        -- RECENCY: Days since last purchase
        DATEDIFF(day, MAX(f.order_purchase_date), CAST(GETDATE() AS DATE)) as recency_days,

        -- FREQUENCY: Number of purchases
        COUNT(DISTINCT f.order_id) as frequency,

        -- MONETARY: Total revenue from customer
        SUM(f.total_value) as monetary
    FROM fact_order_items f
    JOIN dim_customers c ON f.customer_id = c.customer_id
    WHERE f.order_status = 'delivered'
    GROUP BY f.customer_id, c.customer_city, c.customer_state
),
rfm_scores AS (
    SELECT
        customer_id,
        customer_city,
        customer_state,
        recency_days,
        frequency,
        monetary,

        -- WINDOW FUNCTIONS: Quartile ranking (lower recency is better)
        NTILE(4) OVER (ORDER BY recency_days DESC) as r_score,

        -- WINDOW FUNCTIONS: Quartile ranking (higher frequency is better)
        NTILE(4) OVER (ORDER BY frequency ASC) as f_score,

        -- WINDOW FUNCTIONS: Quartile ranking (higher monetary is better)
        NTILE(4) OVER (ORDER BY monetary ASC) as m_score
    FROM rfm_base
)
SELECT
    customer_id,
    customer_city,
    customer_state,
    recency_days,
    frequency,
    ROUND(monetary, 2) as monetary,
    r_score,
    f_score,
    m_score,
    -- RFM Segment Classification
    CASE
        WHEN r_score = 4 AND f_score = 4 AND m_score = 4 THEN 'Champions'
        WHEN r_score = 3 AND f_score = 4 AND m_score = 4 THEN 'Loyal Customers'
        WHEN r_score = 4 AND f_score = 3 AND m_score = 3 THEN 'Potential Loyalists'
        WHEN r_score = 4 AND f_score = 1 AND m_score = 1 THEN 'New Customers'
        WHEN r_score = 1 AND f_score = 4 AND m_score = 4 THEN 'At Risk'
        WHEN r_score = 1 AND f_score = 3 AND m_score = 3 THEN 'Need Attention'
        WHEN r_score = 1 AND f_score = 1 AND m_score = 1 THEN 'Lost'
        ELSE 'Other'
    END as rfm_segment
FROM rfm_scores;

-- Business Insight: RFM segments guide customer retention strategy
-- Metrics: R (Recency), F (Frequency), M (Monetary value)
"""

# ==============================================================================
# 3. PRODUCT & CATEGORY ANALYTICS
# ==============================================================================

# Query 3.1: Top Products by Revenue with Category Breakdown
QUERY_TOP_PRODUCTS = """
WITH product_stats AS (
    SELECT TOP 20
        p.product_id,
        p.product_category_name,
        COUNT(DISTINCT f.order_id) as order_count,
        COUNT(f.order_item_id) as units_sold,
        SUM(f.price) as total_price_revenue,
        SUM(f.freight_value) as total_freight,
        SUM(f.total_value) as total_revenue,
        ROUND(AVG(f.price), 2) as avg_price,
        ROUND(AVG(f.total_value), 2) as avg_item_value,
        COUNT(DISTINCT f.customer_id) as unique_customers,
        ROUND(AVG(CAST(f.is_delayed AS FLOAT)), 4) as delayed_rate,
        MIN(f.order_purchase_date) as first_sale_date,
        MAX(f.order_purchase_date) as last_sale_date
    FROM fact_order_items f
    JOIN dim_products p ON f.product_id = p.product_id
    WHERE f.order_status = 'delivered'
    GROUP BY p.product_id, p.product_category_name
    ORDER BY total_revenue DESC
)
SELECT
    product_id,
    product_category_name,
    order_count,
    units_sold,
    total_revenue,
    avg_price,
    unique_customers,
    delayed_rate,
    -- WINDOW FUNCTIONS: Rank products
    RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank,
    RANK() OVER (ORDER BY units_sold DESC) as volume_rank,
    -- WINDOW FUNCTIONS: % of total revenue
    ROUND(100.0 * total_revenue / SUM(total_revenue) OVER (), 2) as pct_of_total_revenue,
    -- WINDOW FUNCTIONS: Cumulative %
    ROUND(100.0 * SUM(total_revenue) OVER (ORDER BY total_revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) /
          SUM(total_revenue) OVER (), 2) as cumulative_pct_of_revenue
FROM product_stats
ORDER BY total_revenue DESC;

-- Business Insight: Identifies bestsellers and portfolio concentration
-- Uses Pareto principle: 20% of products drive 80% of revenue
"""

# Query 3.2: Category Performance Analysis
QUERY_CATEGORY_PERFORMANCE = """
SELECT
    p.product_category_name,
    COUNT(DISTINCT f.order_id) as order_count,
    COUNT(f.order_item_id) as units_sold,
    COUNT(DISTINCT f.customer_id) as unique_customers,
    SUM(f.total_value) as total_revenue,
    ROUND(AVG(f.total_value), 2) as avg_item_value,
    ROUND(AVG(CAST(f.is_delayed AS FLOAT)), 4) as delayed_rate,
    ROUND(100.0 * SUM(CAST(f.is_delivered AS INT)) / COUNT(*), 2) as delivery_rate_pct,
    -- WINDOW FUNCTIONS: Category performance ranking
    RANK() OVER (ORDER BY SUM(f.total_value) DESC) as revenue_rank,
    -- WINDOW FUNCTIONS: % contribution
    ROUND(100.0 * SUM(f.total_value) / SUM(SUM(f.total_value)) OVER (), 2) as revenue_contribution_pct,
    -- WINDOW FUNCTIONS: Growth comparison
    COUNT(DISTINCT f.order_id) as current_orders
FROM fact_order_items f
JOIN dim_products p ON f.product_id = p.product_id
WHERE f.order_status IN ('delivered', 'shipped')
GROUP BY p.product_category_name
ORDER BY total_revenue DESC;

-- Business Insight: Portfolio management - which categories are growing/declining
"""

# ==============================================================================
# 4. OPERATIONAL METRICS
# ==============================================================================

# Query 4.1: Delivery Performance by Seller
QUERY_DELIVERY_PERFORMANCE = """
WITH seller_performance AS (
    SELECT
        s.seller_id,
        s.seller_city,
        s.seller_state,
        COUNT(DISTINCT f.order_id) as total_orders,
        COUNT(f.order_item_id) as total_items,
        SUM(f.total_value) as total_revenue,
        COUNT(CASE WHEN f.order_status = 'delivered' THEN 1 END) as delivered_orders,
        COUNT(CASE WHEN f.is_delayed = 1 THEN 1 END) as delayed_orders,
        ROUND(AVG(CAST(f.delivery_time_days AS FLOAT)), 1) as avg_delivery_time_days,
        ROUND(AVG(CAST(f.is_delayed AS FLOAT)), 4) as delayed_rate
    FROM fact_order_items f
    JOIN dim_sellers s ON f.seller_id = s.seller_id
    GROUP BY s.seller_id, s.seller_city, s.seller_state
    HAVING COUNT(DISTINCT f.order_id) >= 10  -- Minimum 10 orders
)
SELECT
    seller_id,
    seller_city,
    seller_state,
    total_orders,
    total_items,
    total_revenue,
    delivered_orders,
    delayed_orders,
    avg_delivery_time_days,
    delayed_rate,
    -- WINDOW FUNCTIONS: Seller ranking
    RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank,
    RANK() OVER (ORDER BY delayed_rate ASC) as performance_rank,
    -- WINDOW FUNCTIONS: Performance tier
    CASE
        WHEN delayed_rate <= 0.05 THEN 'Excellent'
        WHEN delayed_rate <= 0.10 THEN 'Good'
        WHEN delayed_rate <= 0.20 THEN 'Fair'
        ELSE 'Poor'
    END as performance_tier
FROM seller_performance
ORDER BY total_revenue DESC;

-- Business Insight: Identify top-performing sellers and those needing support
"""

# Query 4.2: Delayed Orders Analysis
QUERY_DELAYED_ORDERS = """
SELECT
    FORMAT(f.order_purchase_date, 'yyyy-MM') as order_month,
    COUNT(DISTINCT f.order_id) as total_orders,
    SUM(CASE WHEN f.is_delayed = 1 THEN 1 ELSE 0 END) as delayed_orders,
    ROUND(100.0 * SUM(CASE WHEN f.is_delayed = 1 THEN 1 ELSE 0 END) / COUNT(DISTINCT f.order_id), 2) as delayed_rate_pct,
    ROUND(AVG(CAST(f.delivery_time_days AS FLOAT)), 1) as avg_delivery_time_days,
    ROUND(AVG(CAST(f.estimated_delivery_days AS FLOAT)), 1) as avg_estimated_delivery_days,
    -- Delayed orders impact on revenue
    SUM(CASE WHEN f.is_delayed = 1 THEN f.total_value ELSE 0 END) as revenue_from_delayed_orders,
    -- WINDOW FUNCTIONS: Trend analysis
    LAG(SUM(CASE WHEN f.is_delayed = 1 THEN 1 ELSE 0 END)) OVER (ORDER BY FORMAT(f.order_purchase_date, 'yyyy-MM')) as prev_month_delayed_orders
FROM fact_order_items f
WHERE f.order_status = 'delivered'
GROUP BY FORMAT(f.order_purchase_date, 'yyyy-MM')
ORDER BY order_month DESC;

-- Business Insight: On-time delivery is key customer satisfaction metric
"""

# ==============================================================================
# 5. PAYMENT & FINANCIAL ANALYSIS
# ==============================================================================

# Query 5.1: Revenue by Payment Method
QUERY_PAYMENT_METHOD_ANALYSIS = """
SELECT
    p.payment_type,
    COUNT(DISTINCT f.order_id) as order_count,
    SUM(p.payment_value) as total_payment_value,
    ROUND(AVG(p.payment_value), 2) as avg_payment_value,
    COUNT(DISTINCT f.customer_id) as unique_customers,
    ROUND(100.0 * SUM(p.payment_value) / SUM(SUM(p.payment_value)) OVER (), 2) as revenue_contribution_pct,
    -- WINDOW FUNCTIONS: Payment method ranking
    RANK() OVER (ORDER BY SUM(p.payment_value) DESC) as payment_method_rank,
    -- Payment split within orders (installments)
    COUNT(*) as total_payment_records,
    ROUND(AVG(CAST(p.payment_installments AS FLOAT)), 1) as avg_installments
FROM fact_order_items f
JOIN payments p ON f.order_id = p.order_id
WHERE f.order_status = 'delivered'
GROUP BY p.payment_type
ORDER BY total_payment_value DESC;

-- Business Insight: Payment method preferences and financing trends
"""

print("\\n✓ Analytics Queries Loaded Successfully")
print("Total queries defined: 11")
print("\\nQuery Categories:")
print("  1. Revenue & Sales Metrics (3 queries)")
print("  2. Customer Segmentation & Retention (3 queries)")
print("  3. Product & Category Analytics (2 queries)")
print("  4. Operational Metrics (2 queries)")
print("  5. Payment & Financial Analysis (1 query)")
