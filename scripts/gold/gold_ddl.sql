/*
===============================================================================
DDL Script: Create Gold Layer Views (Star Schema)
===============================================================================
Database      : RETAIL_DATA_ANALYTICS_DB
Schema        : gold
Layer         : Gold (Business-Ready Layer)

Script Purpose:
    - Create Dimesion and Fact views from Silver layer tables
    - Apply business logic and KPI calculations
    - Provide analytics-ready Star Schema

Usage:
    SELECT * FROM gold.fact_sales;
    SELECT * FROM gold.fact_inventory;
===============================================================================
*/

-- ----------------------------------------------------------------------------
-- Context Setup
-- ----------------------------------------------------------------------------
USE ROLE ANALYST_ROLE;
USE DATABASE RETAIL_DATA_ANALYTICS_DB;
CREATE SCHEMA IF NOT EXISTS gold;
USE SCHEMA gold;

-- =============================================================================
-- DIMENSION: CUSTOMERS
-- =============================================================================
CREATE OR REPLACE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cd.customer_id)     AS customer_key,
    cd.customer_id                                  AS customer_id,
    cd.age                                          AS age,
    cd.gender                                       AS gender,
    cd.income_bracket                               AS income_bracket,
    cd.purchase_frequency                           AS purchase_frequency,
    cd.average_spend                                AS average_spend
FROM silver.customer_demographics cd;


-- =============================================================================
-- DIMENSION: PRODUCTS
-- =============================================================================
CREATE OR REPLACE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pi.product_id)      AS product_key,
    pi.product_id                                   AS product_id,
    pi.product_name                                 AS product_name,
    pi.category                                     AS category,
    pi.subcategory                                  AS subcategory,
    pi.supplier                                     AS supplier,
    pi.unit_cost                                    AS unit_cost,
    pi.unit_price                                   AS unit_price,
    pi.shelf_life                                   AS shelf_life,
    (pi.unit_price - pi.unit_cost)                  AS unit_margin,
    CASE 
        WHEN pi.unit_cost = 0 THEN NULL
        ELSE ((pi.unit_price - pi.unit_cost) / pi.unit_cost)
    END                                             AS margin_pct
FROM silver.product_information pi;


-- =============================================================================
-- DIMENSION: SITES
-- =============================================================================
CREATE OR REPLACE VIEW gold.dim_sites AS
SELECT
    ROW_NUMBER() OVER (ORDER BY sd.site_id)         AS site_key,
    sd.site_id                                      AS site_id,
    sd.site_name                                    AS site_name,
    sd.site_format                                  AS site_format,
    sd.region                                       AS region,
    sd.city                                         AS city,
    sd.state                                        AS state,
    sd.store_size                                   AS store_size,
    sd.open_date                                    AS open_date,
    sd.status                                       AS status,
    DATEDIFF('year', sd.open_date, CURRENT_DATE())  AS years_operational
FROM silver.site_details sd;


-- =============================================================================
-- DIMENSION: DATE (Derived from Sales)
-- =============================================================================
CREATE OR REPLACE VIEW gold.dim_date AS
SELECT DISTINCT
    sd.sales_date                                   AS date_key,
    YEAR(sd.sales_date)                             AS year,
    QUARTER(sd.sales_date)                          AS quarter,
    MONTH(sd.sales_date)                            AS month,
    MONTHNAME(sd.sales_date)                        AS month_name,
    WEEK(sd.sales_date)                             AS week_of_year,
    DAY(sd.sales_date)                              AS day,
    DAYOFWEEK(sd.sales_date)                        AS day_of_week
FROM silver.sales_data sd;


-- =============================================================================
-- FACT: SALES
-- =============================================================================
CREATE OR REPLACE VIEW gold.fact_sales AS
SELECT
    sd.sales_date                                   AS sales_date,
    dp.product_key                                  AS product_key,
    dc.customer_key                                 AS customer_key,
    ds.site_key                                     AS site_key,

    sd.units_sold                                   AS units_sold,
    sd.revenue                                      AS gross_revenue,
    sd.discounts                                    AS total_discounts,
    sd.returns                                      AS units_returned,

    (sd.revenue - sd.discounts)                     AS net_revenue,
    (sd.units_sold - COALESCE(sd.returns,0))        AS net_units_sold,

    CASE 
        WHEN sd.units_sold = 0 THEN NULL
        ELSE sd.revenue / sd.units_sold
    END                                             AS avg_selling_price

FROM silver.sales_data sd
LEFT JOIN gold.dim_products dp
       ON sd.product_id = dp.product_id
LEFT JOIN gold.dim_customers dc
       ON sd.customer_id = dc.customer_id
LEFT JOIN gold.dim_sites ds
       ON sd.site_id = ds.site_id;


-- =============================================================================
-- FACT: INVENTORY
-- =============================================================================
CREATE OR REPLACE VIEW gold.fact_inventory AS
SELECT
    di.site_id                                      AS site_id,
    dp.product_key                                  AS product_key,
    ds.site_key                                     AS site_key,

    di.beginning_inventory                          AS beginning_inventory,
    di.ending_inventory                             AS ending_inventory,
    di.replenishment                                AS replenishment_qty,

    (di.ending_inventory - di.beginning_inventory)  AS inventory_change,

    CASE 
        WHEN di.stockout_flag = 'YES' THEN 1
        ELSE 0
    END                                             AS stockout_indicator

FROM silver.inventory_data di
LEFT JOIN gold.dim_products dp
       ON di.product_id = dp.product_id
LEFT JOIN gold.dim_sites ds
       ON di.site_id = ds.site_id;


-- =============================================================================
-- FACT: LOGISTICS
-- =============================================================================
CREATE OR REPLACE VIEW gold.fact_logistics AS
SELECT
    ld.shipment_id                                  AS shipment_id,
    ld.shipment_date                                AS shipment_date,
    dp.product_key                                  AS product_key,
    ds.site_key                                     AS site_key,

    ld.quantity                                     AS shipment_quantity,
    ld.transportation_type                          AS transportation_type,
    ld.delivery_status                              AS delivery_status,

    CASE 
        WHEN ld.delivery_status = 'DELIVERED' THEN 1
        ELSE 0
    END                                             AS delivered_flag

FROM silver.logistics_data ld
LEFT JOIN gold.dim_products dp
       ON ld.product_id = dp.product_id
LEFT JOIN gold.dim_sites ds
       ON ld.site_id = ds.site_id;


-- =============================================================================
-- FACT: SEASONAL PLANNING
-- =============================================================================
CREATE OR REPLACE VIEW gold.fact_seasonal_planning AS
SELECT
    msp.month                                       AS month_label,
    ds.site_key                                     AS site_key,

    msp.product_category                            AS product_category,
    msp.forecasted_sales                            AS forecasted_sales,
    msp.actual_sales                                AS actual_sales,

    (msp.actual_sales - msp.forecasted_sales)       AS sales_variance,

    CASE 
        WHEN msp.forecasted_sales = 0 THEN NULL
        ELSE (msp.actual_sales - msp.forecasted_sales)
             / msp.forecasted_sales
    END                                             AS forecast_accuracy_pct,

    msp.seasonal_adjustment                         AS seasonal_adjustment

FROM silver.monthly_seasonal_planning msp
LEFT JOIN gold.dim_sites ds
       ON msp.site_id = ds.site_id;

-- =============================================================================
-- VIEW: SALES SUMMARY
-- PURPOSE:
--     Provides aggregated sales performance by year, quarter, and month.
--     Used for executive reporting and time-based trend analysis.
-- GRAIN:
--     One row per Year / Quarter / Month.
-- =============================================================================
CREATE OR REPLACE VIEW gold.v_sales_summary AS
SELECT
    dd.year                               AS year,
    dd.quarter                            AS quarter,
    dd.month                              AS month,
    SUM(fs.net_revenue)                   AS total_revenue,
    SUM(fs.units_sold)                    AS total_units,
    SUM(fs.total_discounts)               AS total_discounts,
    AVG(fs.avg_selling_price)             AS avg_selling_price
FROM gold.fact_sales fs
JOIN gold.dim_date dd
    ON fs.sales_date = dd.date_key
GROUP BY dd.year, dd.quarter, dd.month;



-- =============================================================================
-- VIEW: PRODUCT PERFORMANCE
-- PURPOSE:
--     Analyzes revenue and sales volume by product and category.
--     Includes realized selling price and margin insights.
-- GRAIN:
--     One row per Product / Category.
-- =============================================================================
CREATE OR REPLACE VIEW gold.v_product_performance AS
SELECT
    dp.product_name,
    dp.category,
    SUM(fs.net_revenue)                   AS total_revenue,
    SUM(fs.units_sold)                    AS total_units,
    AVG(dp.unit_margin)                   AS unit_margin,
    SUM(fs.net_revenue) / NULLIF(SUM(fs.units_sold),0) AS realized_price
FROM gold.fact_sales fs
JOIN gold.dim_products dp
    ON fs.product_key = dp.product_key
GROUP BY dp.product_name, dp.category;



-- =============================================================================
-- VIEW: STORE PERFORMANCE
-- PURPOSE:
--     Evaluates store-level revenue productivity.
--     Includes revenue per square foot and revenue per year operational.
-- GRAIN:
--     One row per Site / Region.
-- =============================================================================
CREATE OR REPLACE VIEW gold.v_store_performance AS
SELECT
    ds.site_name,
    ds.region,
    SUM(fs.net_revenue)                         AS total_revenue,
    SUM(fs.units_sold)                          AS total_units,
    SUM(fs.net_revenue) / NULLIF(ds.store_size,0) AS revenue_per_sqft,
    SUM(fs.net_revenue) / NULLIF(ds.years_operational,0) AS revenue_per_year_open
FROM gold.fact_sales fs
JOIN gold.dim_sites ds
    ON fs.site_key = ds.site_key
GROUP BY ds.site_name, ds.region, ds.store_size, ds.years_operational;



-- =============================================================================
-- VIEW: INVENTORY EFFICIENCY
-- PURPOSE:
--     Measures sell-through performance by product and store.
--     Helps identify overstock and fast-moving inventory.
-- GRAIN:
--     One row per Product / Site.
-- =============================================================================
CREATE OR REPLACE VIEW gold.v_inventory_efficiency AS
SELECT
    dp.product_name,
    ds.site_name,
    fi.ending_inventory,
    SUM(fs.units_sold) AS total_units_sold,
    CASE 
        WHEN fi.ending_inventory = 0 THEN NULL
        ELSE SUM(fs.units_sold) / fi.ending_inventory
    END AS sell_through_rate
FROM gold.fact_inventory fi
LEFT JOIN gold.fact_sales fs
    ON fi.product_key = fs.product_key
LEFT JOIN gold.dim_products dp
    ON fi.product_key = dp.product_key
LEFT JOIN gold.dim_sites ds
    ON fi.site_key = ds.site_key
GROUP BY dp.product_name, ds.site_name, fi.ending_inventory;



-- =============================================================================
-- VIEW: CUSTOMER VALUE
-- PURPOSE:
--     Calculates customer lifetime value and engagement metrics.
--     Supports segmentation and retention analysis.
-- GRAIN:
--     One row per Customer.
-- =============================================================================
CREATE OR REPLACE VIEW gold.v_customer_value AS
SELECT
    dc.customer_id,
    dc.income_bracket,
    SUM(fs.net_revenue)           AS lifetime_value,
    COUNT(DISTINCT fs.sales_date) AS active_days,
    AVG(fs.avg_selling_price)     AS avg_spend_per_purchase
FROM gold.fact_sales fs
JOIN gold.dim_customers dc
    ON fs.customer_key = dc.customer_key
GROUP BY dc.customer_id, dc.income_bracket;



-- =============================================================================
-- VIEW: FORECAST ACCURACY
-- PURPOSE:
--     Compares forecasted vs actual sales performance.
--     Used to measure planning accuracy and variance.
-- GRAIN:
--     One row per Product Category.
-- =============================================================================
CREATE OR REPLACE VIEW gold.v_forecast_accuracy AS
SELECT
    product_category,
    SUM(forecasted_sales) AS total_forecast,
    SUM(actual_sales)     AS total_actual,
    SUM(actual_sales - forecasted_sales) AS variance
FROM gold.fact_seasonal_planning
GROUP BY product_category;



-- =============================================================================
-- VIEW: SALES ROLLING 7-DAY AVERAGE
-- PURPOSE:
--     Calculates daily revenue and rolling 7-day average revenue.
--     Used for short-term trend smoothing and volatility analysis.
-- GRAIN:
--     One row per Sales Date.
-- =============================================================================
CREATE OR REPLACE VIEW gold.v_sales_rolling_7d AS
SELECT
    fs.sales_date,
    SUM(fs.net_revenue) AS daily_revenue,
    AVG(SUM(fs.net_revenue)) OVER (
        ORDER BY fs.sales_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_avg_revenue
FROM gold.fact_sales fs
GROUP BY fs.sales_date;



-- =============================================================================
-- VIEW: SALES RUNNING TOTAL
-- PURPOSE:
--     Computes cumulative revenue over time.
--     Useful for tracking growth trajectory.
-- GRAIN:
--     One row per Sales Date.
-- =============================================================================
CREATE OR REPLACE VIEW gold.v_sales_running_total AS
SELECT
    fs.sales_date,
    SUM(fs.net_revenue) AS daily_revenue,
    SUM(SUM(fs.net_revenue)) OVER (
        ORDER BY fs.sales_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue
FROM gold.fact_sales fs
GROUP BY fs.sales_date;



-- =============================================================================
-- VIEW: MONTHLY GROWTH
-- PURPOSE:
--     Calculates month-over-month revenue growth percentage.
--     Identifies acceleration or slowdown in performance.
-- GRAIN:
--     One row per Month.
-- =============================================================================
CREATE OR REPLACE VIEW gold.v_monthly_growth AS
WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', fs.sales_date) AS month,
        SUM(fs.net_revenue)               AS total_revenue
    FROM gold.fact_sales fs
    GROUP BY 1
)
SELECT
    ms.month,
    ms.total_revenue,
    LAG(ms.total_revenue) OVER (ORDER BY ms.month) AS prev_month_revenue,
    CASE
        WHEN LAG(ms.total_revenue) OVER (ORDER BY ms.month) = 0
        THEN NULL
        ELSE
            (ms.total_revenue -
             LAG(ms.total_revenue) OVER (ORDER BY ms.month))
            / LAG(ms.total_revenue) OVER (ORDER BY ms.month)
    END AS mom_growth_pct
FROM monthly_sales ms;