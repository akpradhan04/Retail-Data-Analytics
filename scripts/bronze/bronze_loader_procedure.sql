/*
===============================================================================
DDL Script: Bronze Layer Data Load Procedure
===============================================================================
Database      : RETAIL_DATA_ANALYTICS_DB
Schema        : bronze
Procedure     : load_bronze
Purpose       :
    - Truncates and reloads all Bronze layer tables from staged CSV files
    - Acts as the raw ingestion layer for retail source data
Pre-requisites:
    - Files need to be uploaded to the file stage bronze.bronze_stage
Execution     :
    CALL bronze.load_bronze();
===============================================================================
*/

-- ----------------------------------------------------------------------------
-- Context Setup
-- ----------------------------------------------------------------------------
USE ROLE SYSADMIN;
USE DATABASE RETAIL_DATA_ANALYTICS_DB;

-- ----------------------------------------------------------------------------
-- Stage Creation (used for raw file ingestion)
-- ----------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS bronze.bronze_stage;

-- ----------------------------------------------------------------------------
-- Stored Procedure: Bronze Data Load
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE bronze.load_bronze()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    batch_start_time TIMESTAMP;
    batch_end_time   TIMESTAMP;
BEGIN
    -- Capture batch start time
    batch_start_time := CURRENT_TIMESTAMP();

    /* =======================================================================
       CUSTOMER DEMOGRAPHICS
       ======================================================================= */
    TRUNCATE TABLE bronze.customer_demographics;

    COPY INTO bronze.customer_demographics
    FROM @bronze.bronze_stage
    FILES = ('Customer_Demographics.csv')
    FILE_FORMAT = (
        TYPE = 'csv'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
    );

    /* =======================================================================
       INVENTORY DATA
       ======================================================================= */
    TRUNCATE TABLE bronze.inventory_data;

    COPY INTO bronze.inventory_data
    FROM @bronze.bronze_stage
    FILES = ('Inventory_Data.csv')
    FILE_FORMAT = (
        TYPE = 'csv'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
    );

    /* =======================================================================
       LOGISTICS DATA
       ======================================================================= */
    TRUNCATE TABLE bronze.logistics_data;

    COPY INTO bronze.logistics_data
    FROM @bronze.bronze_stage
    FILES = ('Logistics_Data.csv')
    FILE_FORMAT = (
        TYPE = 'csv'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
    );

    /* =======================================================================
       MONTHLY SEASONAL PLANNING
       ======================================================================= */
    TRUNCATE TABLE bronze.monthly_seasonal_planning;

    COPY INTO bronze.monthly_seasonal_planning
    FROM @bronze.bronze_stage
    FILES = ('Monthly_Seasonal_Planning.csv')
    FILE_FORMAT = (
        TYPE = 'csv'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
    );

    /* =======================================================================
       PRODUCT INFORMATION
       ======================================================================= */
    TRUNCATE TABLE bronze.product_information;

    COPY INTO bronze.product_information
    FROM @bronze.bronze_stage
    FILES = ('Product_Information.csv')
    FILE_FORMAT = (
        TYPE = 'csv'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
    );

    /* =======================================================================
       PROMOTIONS AND DISCOUNTS
       ======================================================================= */
    TRUNCATE TABLE bronze.promotions_and_discounts;

    COPY INTO bronze.promotions_and_discounts
    FROM @bronze.bronze_stage
    FILES = ('Promotions_and_Discounts.csv')
    FILE_FORMAT = (
        TYPE = 'csv'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
    );

    /* =======================================================================
       SALES DATA
       ======================================================================= */
    TRUNCATE TABLE bronze.sales_data;

    COPY INTO bronze.sales_data
    FROM @bronze.bronze_stage
    FILES = ('Sales_Data.csv')
    FILE_FORMAT = (
        TYPE = 'csv'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
    );

    /* =======================================================================
       SITE DETAILS
       ======================================================================= */
    TRUNCATE TABLE bronze.site_details;

    COPY INTO bronze.site_details
    FROM @bronze.bronze_stage
    FILES = ('Site_Details.csv')
    FILE_FORMAT = (
        TYPE = 'csv'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
    );

    -- Capture batch end time
    batch_end_time := CURRENT_TIMESTAMP();

    -- Return execution summary
    RETURN 'Bronze layer loaded successfully. Total duration (seconds): ' ||
           DATEDIFF('second', batch_start_time, batch_end_time);

EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR during Bronze load: ' || SQLERRM;
END;
$$;