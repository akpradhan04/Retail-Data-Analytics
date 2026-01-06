/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Database      : RETAIL_DATA_ANALYTICS_DB
Schema        : bronze
Purpose       :
    - Creates Bronze layer tables for raw retail data ingestion
    - Existing tables are replaced to refresh the DDL structure
    - Used as the foundational layer for downstream Silver/Gold models
===============================================================================
*/

USE ROLE SYSADMIN;
USE DATABASE RETAIL_DATA_ANALYTICS_DB;

CREATE 
    OR REPLACE TABLE bronze.customer_demographics (
    customer_id                             VARCHAR(50),
    age                                     INT,
    gender                                  VARCHAR(20),
    income_bracket                          VARCHAR(50),
    purchase_frequency                      INT,
    average_spend                           DECIMAL(10,2)
);

CREATE 
    OR REPLACE TABLE bronze.inventory_data (
    site_id                                 VARCHAR(50),
    product_id                              VARCHAR(50),
    beginning_inventory                     INT,
    ending_inventory                        INT,
    replenishment                           INT,
    stockout_flag                           VARCHAR(10)
);

CREATE 
    OR REPLACE TABLE bronze.logistics_data (
    shipment_id                             VARCHAR(50),
    site_id                                 VARCHAR(50),
    product_id                              VARCHAR(50),
    shipment_date                           DATE,
    quantity                                INT,
    delivery_status                         VARCHAR(30),
    transportation_type                     VARCHAR(30)
);

CREATE 
    OR REPLACE TABLE bronze.monthly_seasonal_planning (
    month                                   VARCHAR(20),
    site_id                                 VARCHAR(10),
    product_category                        VARCHAR(50),
    forecasted_sales                        DECIMAL(12,2),
    actual_sales                            DECIMAL(14,6),
    seasonal_adjustment                     DECIMAL(6,4)
);

CREATE 
    OR REPLACE TABLE bronze.product_information (
    product_id                              VARCHAR(20),
    product_name                            VARCHAR(100),
    category                                VARCHAR(50),
    subcategory                             VARCHAR(50),
    unit_cost                               DECIMAL(10,2),
    unit_price                              DECIMAL(10,2),
    supplier                                VARCHAR(50),
    shelf_life                              INT
);

CREATE 
    OR REPLACE TABLE bronze.promotion_and_discounts (
    promotion_id                            VARCHAR(20),
    product_id                              VARCHAR(20),
    site_id                                 VARCHAR(10),
    start_date                              DATE,
    end_date                                DATE,
    discount_type                           VARCHAR(20),
    discount_amount                         DECIMAL(10,2)
);

CREATE 
    OR REPLACE TABLE bronze.sales_data (
    sales_date                              DATE,
    site_id                                 VARCHAR(10),
    product_id                              VARCHAR(20),
    units_sold                              INT,
    revenue                                 DECIMAL(12,2),
    discounts                               DECIMAL(12,2),
    returns                                 INT,
    customer_id                             VARCHAR(20)
);

CREATE 
    OR REPLACE TABLE bronze.site_details (
    site_id                                 VARCHAR(10),
    site_name                               VARCHAR(100),
    site_format                             VARCHAR(50),
    region                                  VARCHAR(50),
    city                                    VARCHAR(50),
    state                                   VARCHAR(50),
    store_size                              INT,
    open_date                               DATE,
    status                                  VARCHAR(20)
);
