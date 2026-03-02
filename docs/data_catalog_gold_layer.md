# Data Catalog - Gold Layer

## Overview
This document provides a comprehensive catalog of all objects in the **Gold Layer** of the Retail Data Analytics data warehouse. The Gold layer contains business-ready dimension tables, fact tables, and analytical views built on the medallion architecture.

## Schema Information
- **Database**: RETAIL_DATA_ANALYTICS_DB
- **Schema**: gold
- **Layer**: Gold (Business-Ready Analytics)

---

## Dimension Tables

### 1. dim_customers
Customer dimension table with demographic information.

| Column Name | Data Type | Description | Source |
|-------------|-----------|-------------|--------|
| customer_key | INT | Surrogate key for the customer | Auto-generated |
| customer_id | VARCHAR(50) | Original customer identifier | silver.customer_demographics |
| age | INT | Customer age | silver.customer_demographics |
| gender | VARCHAR(20) | Customer gender | silver.customer_demographics |
| income_bracket | VARCHAR(50) | Income category | silver.customer_demographics |
| purchase_frequency | INT | Number of purchases | silver.customer_demographics |
| average_spend | DECIMAL(10,2) | Average spend per transaction | silver.customer_demographics |

---

### 2. dim_products
Product dimension table with pricing and category information.

| Column Name | Data Type | Description | Source |
|-------------|-----------|-------------|--------|
| product_key | INT | Surrogate key for the product | Auto-generated |
| product_id | VARCHAR(20) | Original product identifier | silver.product_information |
| product_name | VARCHAR(100) | Product name | silver.product_information |
| category | VARCHAR(50) | Product category | silver.product_information |
| subcategory | VARCHAR(50) | Product subcategory | silver.product_information |
| supplier | VARCHAR(50) | Product supplier | silver.product_information |
| unit_cost | DECIMAL(10,2) | Cost per unit | silver.product_information |
| unit_price | DECIMAL(10,2) | Selling price per unit | silver.product_information |
| shelf_life | INT | Shelf life in days | silver.product_information |
| unit_margin | DECIMAL(10,2) | Calculated margin (price - cost) | Calculated |
| margin_pct | DECIMAL(10,2) | Margin percentage | Calculated |

---

### 3. dim_sites
Store/site dimension table with location information.

| Column Name | Data Type | Description | Source |
|-------------|-----------|-------------|--------|
| site_key | INT | Surrogate key for the site | Auto-generated |
| site_id | VARCHAR(10) | Original site identifier | silver.site_details |
| site_name | VARCHAR(100) | Store name | silver.site_details |
| site_format | VARCHAR(50) | Store format type | silver.site_details |
| region | VARCHAR(50) | Geographic region | silver.site_details |
| city | VARCHAR(50) | Store city | silver.site_details |
| state | VARCHAR(50) | Store state | silver.site_details |
| store_size | INT | Store size in sq ft | silver.site_details |
| open_date | DATE | Store opening date | silver.site_details |
| status | VARCHAR(20) | Store operational status | silver.site_details |
| years_operational | INT | Years since opening | Calculated |

---

### 4. dim_date
Date dimension table derived from sales data.

| Column Name | Data Type | Description | Source |
|-------------|-----------|-------------|--------|
| date_key | DATE | Full date | silver.sales_data |
| year | INT | Year component | Extracted |
| quarter | INT | Quarter (1-4) | Extracted |
| month | INT | Month (1-12) | Extracted |
| month_name | VARCHAR(20) | Month name | Extracted |
| week_of_year | INT | Week number | Extracted |
| day | INT | Day of month | Extracted |
| day_of_week | INT | Day of week (1-7) | Extracted |

---

## Fact Tables

### 5. fact_sales
Primary sales fact table with transaction-level details.

| Column Name | Data Type | Description | Source |
|-------------|-----------|-------------|--------|
| sales_date | DATE | Date of sale | silver.sales_data |
| product_key | INT | Foreign key to dim_products | Linked |
| customer_key | INT | Foreign key to dim_customers | Linked |
| site_key | INT | Foreign key to dim_sites | Linked |
| units_sold | INT | Units sold in transaction | silver.sales_data |
| gross_revenue | DECIMAL(12,2) | Total revenue before discounts | silver.sales_data |
| total_discounts | DECIMAL(12,2) | Total discount amount | silver.sales_data |
| units_returned | INT | Units returned | silver.sales_data |
| net_revenue | DECIMAL(12,2) | Revenue - discounts | Calculated |
| net_units_sold | INT | Units sold - returns | Calculated |
| avg_selling_price | DECIMAL(12,2) | Revenue / units sold | Calculated |

---

### 6. fact_inventory
Inventory fact table with stock levels.

| Column Name | Data Type | Description | Source |
|-------------|-----------|-------------|--------|
| site_id | VARCHAR(50) | Site identifier | silver.inventory_data |
| product_key | INT | Foreign key to dim_products | Linked |
| site_key | INT | Foreign key to dim_sites | Linked |
| beginning_inventory | INT | Inventory at period start | silver.inventory_data |
| ending_inventory | INT | Inventory at period end | silver.inventory_data |
| replenishment_qty | INT | Replenishment quantity | silver.inventory_data |
| inventory_change | INT | Change in inventory | Calculated |
| stockout_indicator | INT | 1 if stockout, 0 otherwise | Calculated |

---

### 7. fact_logistics
Logistics fact table with shipment information.

| Column Name | Data Type | Description | Source |
|-------------|-----------|-------------|--------|
| shipment_id | VARCHAR(50) | Shipment identifier | silver.logistics_data |
| shipment_date | DATE | Date of shipment | silver.logistics_data |
| product_key | INT | Foreign key to dim_products | Linked |
| site_key | INT | Foreign key to dim_sites | Linked |
| shipment_quantity | INT | Quantity shipped | silver.logistics_data |
| transportation_type | VARCHAR(30) | Mode of transport | silver.logistics_data |
| delivery_status | VARCHAR(30) | Delivery status | silver.logistics_data |
| delivered_flag | INT | 1 if delivered, 0 otherwise | Calculated |

---

### 8. fact_seasonal_planning
Seasonal planning fact table with forecast data.

| Column Name | Data Type | Description | Source |
|-------------|-----------|-------------|--------|
| month_label | VARCHAR(20) | Month name | silver.monthly_seasonal_planning |
| site_key | INT | Foreign key to dim_sites | Linked |
| product_category | VARCHAR(50) | Product category | silver.monthly_seasonal_planning |
| forecasted_sales | DECIMAL(12,2) | Forecasted sales amount | silver.monthly_seasonal_planning |
| actual_sales | DECIMAL(14,6) | Actual sales amount | silver.monthly_seasonal_planning |
| sales_variance | DECIMAL(14,6) | Actual - Forecasted | Calculated |
| forecast_accuracy_pct | DECIMAL(10,4) | Forecast accuracy percentage | Calculated |
| seasonal_adjustment | DECIMAL(6,4) | Seasonal adjustment factor | silver.monthly_seasonal_planning |

---

## Analytics Views

### 9. v_sales_summary
Aggregated sales performance by time period.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| year | INT | Year |
| quarter | INT | Quarter (1-4) |
| month | INT | Month (1-12) |
| total_revenue | DECIMAL(12,2) | Sum of net revenue |
| total_units | BIGINT | Sum of units sold |
| total_discounts | DECIMAL(12,2) | Sum of discounts |
| avg_selling_price | DECIMAL(12,2) | Average selling price |

**Grain**: One row per Year / Quarter / Month  
**Purpose**: Executive reporting, time-based trend analysis

---

### 10. v_product_performance
Revenue and sales volume by product and category.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| product_name | VARCHAR(100) | Product name |
| category | VARCHAR(50) | Product category |
| total_revenue | DECIMAL(12,2) | Total net revenue |
| total_units | BIGINT | Total units sold |
| unit_margin | DECIMAL(10,2) | Average unit margin |
| realized_price | DECIMAL(12,2) | Net revenue / net units |

**Grain**: One row per Product / Category  
**Purpose**: Product analysis, category performance

---

### 11. v_store_performance
Store-level revenue productivity metrics.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| site_name | VARCHAR(100) | Store name |
| region | VARCHAR(50) | Geographic region |
| total_revenue | DECIMAL(12,2) | Total net revenue |
| total_units | BIGINT | Total units sold |
| revenue_per_sqft | DECIMAL(12,2) | Revenue / store size |
| revenue_per_year_open | DECIMAL(12,2) | Revenue / years operational |

**Grain**: One row per Site / Region  
**Purpose**: Store performance analysis, capacity planning

---

### 12. v_inventory_efficiency
Sell-through performance by product and store.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| product_name | VARCHAR(100) | Product name |
| site_name | VARCHAR(100) | Store name |
| ending_inventory | INT | Current inventory level |
| total_units_sold | BIGINT | Units sold in period |
| sell_through_rate | DECIMAL(12,2) | Units sold / ending inventory |

**Grain**: One row per Product / Site  
**Purpose**: Inventory optimization, stock management

---

### 13. v_customer_value
Customer lifetime value and engagement metrics.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| customer_id | VARCHAR(50) | Customer identifier |
| income_bracket | VARCHAR(50) | Income category |
| lifetime_value | DECIMAL(12,2) | Sum of all revenue |
| active_days | INT | Number of unique purchase days |
| avg_spend_per_purchase | DECIMAL(12,2) | Average spend per transaction |

**Grain**: One row per Customer  
**Purpose**: Customer segmentation, retention analysis

---

### 14. v_forecast_accuracy
Forecast vs actual sales performance.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| product_category | VARCHAR(50) | Product category |
| total_forecast | DECIMAL(12,2) | Sum of forecasted sales |
| total_actual | DECIMAL(14,6) | Sum of actual sales |
| variance | DECIMAL(14,6) | Actual - Forecast |

**Grain**: One row per Product Category  
**Purpose**: Planning accuracy, demand forecasting

---

### 15. v_sales_rolling_7d
Daily revenue with 7-day rolling average.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| sales_date | DATE | Date |
| daily_revenue | DECIMAL(12,2) | Revenue for the day |
| rolling_7d_avg_revenue | DECIMAL(12,2) | 7-day rolling average |

**Grain**: One row per Sales Date  
**Purpose**: Short-term trend analysis, volatility measurement

---

### 16. v_sales_running_total
Cumulative revenue over time.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| sales_date | DATE | Date |
| daily_revenue | DECIMAL(12,2) | Revenue for the day |
| cumulative_revenue | DECIMAL(12,2) | Running total revenue |

**Grain**: One row per Sales Date  
**Purpose**: Growth trajectory tracking

---

### 17. v_monthly_growth
Month-over-month revenue growth.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| month | DATE | Month (first day) |
| total_revenue | DECIMAL(12,2) | Revenue for the month |
| prev_month_revenue | DECIMAL(12,2) | Previous month revenue |
| mom_growth_pct | DECIMAL(10,4) | Month-over-month growth % |

**Grain**: One row per Month  
**Purpose**: Growth rate analysis, performance trends
