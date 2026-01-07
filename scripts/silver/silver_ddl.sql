/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Database      : RETAIL_DATA_ANALYTICS_DB
Schema        : silver
Purpose       :
    - Creates Silver layer tables for curated and standardized retail data
    - Includes audit timestamps for warehouse tracking
    - Serves as the source for downstream Gold analytical models
===============================================================================
*/

USE ROLE SYSADMIN;
USE DATABASE RETAIL_DATA_ANALYTICS_DB;

CREATE 
    OR REPLACE TABLE silver.customer_demographics (
    customer_id            VARCHAR(50) NOT NULL,
    age                    INT,
    gender                 VARCHAR(20),
    income_bracket         VARCHAR(50),
    purchase_frequency     INT,
    average_spend          DECIMAL(10,2),
    dwh_insert_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    dwh_update_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE 
    OR REPLACE TABLE silver.inventory_data (
    site_id                VARCHAR(50) NOT NULL,
    product_id             VARCHAR(50) NOT NULL,
    beginning_inventory    INT,
    ending_inventory       INT,
    replenishment          INT,
    stockout_flag          VARCHAR(10),
    dwh_insert_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    dwh_update_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE 
    OR REPLACE TABLE silver.logistics_data (
    shipment_id            VARCHAR(50) NOT NULL,
    site_id                VARCHAR(50) NOT NULL,
    product_id             VARCHAR(50) NOT NULL,
    shipment_date          DATE,
    quantity               INT,
    delivery_status        VARCHAR(30),
    transportation_type    VARCHAR(30),
    dwh_insert_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    dwh_update_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE 
    OR REPLACE TABLE silver.monthly_seasonal_planning (
    month                  VARCHAR(20) NOT NULL,
    site_id                VARCHAR(10) NOT NULL,
    product_category       VARCHAR(50) NOT NULL,
    forecasted_sales       DECIMAL(12,2),
    actual_sales           DECIMAL(14,6),
    seasonal_adjustment    DECIMAL(6,4),
    dwh_insert_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    dwh_update_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE 
    OR REPLACE TABLE silver.product_information (
    product_id             VARCHAR(20) NOT NULL,
    product_name           VARCHAR(100) NOT NULL,
    category               VARCHAR(50),
    subcategory            VARCHAR(50),
    unit_cost              DECIMAL(10,2),
    unit_price             DECIMAL(10,2),
    supplier               VARCHAR(50),
    shelf_life             INT,
    dwh_insert_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    dwh_update_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE 
    OR REPLACE TABLE silver.promotions_and_discounts (
    promotion_id           VARCHAR(20) NOT NULL,
    product_id             VARCHAR(20) NOT NULL,
    site_id                VARCHAR(10) NOT NULL,
    start_date             DATE,
    end_date               DATE,
    discount_type          VARCHAR(20),
    discount_amount        DECIMAL(10,2),
    dwh_insert_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    dwh_update_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE 
    OR REPLACE TABLE silver.sales_data (
    sales_date             DATE NOT NULL,
    site_id                VARCHAR(10) NOT NULL,
    product_id             VARCHAR(20) NOT NULL,
    units_sold             INT NOT NULL,
    revenue                DECIMAL(12,2),
    discounts              DECIMAL(12,2),
    returns                INT,
    customer_id            VARCHAR(20),
    dwh_insert_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    dwh_update_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE 
    OR REPLACE TABLE silver.site_details (
    site_id                VARCHAR(10) NOT NULL,
    site_name              VARCHAR(100) NOT NULL,
    site_format            VARCHAR(50) NOT NULL,
    region                 VARCHAR(50),
    city                   VARCHAR(50),
    state                  VARCHAR(50),
    store_size             INT,
    open_date              DATE,
    status                 VARCHAR(20),
    dwh_insert_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    dwh_update_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
