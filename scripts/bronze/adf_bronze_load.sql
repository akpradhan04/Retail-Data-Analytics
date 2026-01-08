/*
===============================================================================
FULL SETUP SCRIPT: Azure → Snowflake Bronze Layer Ingestion
===============================================================================
Database      : RETAIL_DATA_ANALYTICS_DB
Schema        : BRONZE
Purpose       :
    - Create Azure storage integration
    - Create CSV file format
    - Create external stage pointing to Azure Blob Storage
    - Create Bronze load stored procedure
    - Load raw retail CSV data into Bronze tables
===============================================================================
*/

-- ----------------------------------------------------------------------------
-- 1. CONTEXT SETUP
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
USE DATABASE RETAIL_DATA_ANALYTICS_DB;
USE SCHEMA BRONZE;

-- ----------------------------------------------------------------------------
-- 2. AZURE STORAGE INTEGRATION
-- ----------------------------------------------------------------------------
CREATE OR REPLACE STORAGE INTEGRATION SNOWFLAKE_AZURE_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = AZURE
    ENABLED = TRUE
    AZURE_TENANT_ID = '<AZURE_TENANT_ID>'
    STORAGE_ALLOWED_LOCATIONS = ('<AZURE_STORAGE_URL>');

-- ----------------------------------------------------------------------------
-- 3. DESCRIBE INTEGRATION (USE THIS OUTPUT IN AZURE)
-- ----------------------------------------------------------------------------
-- Run this and copy:
--   AZURE_CONSENT_URL → grant admin consent
--   AZURE_MULTI_TENANT_APP_NAME → assign RBAC role in Azure
--
-- DESC STORAGE INTEGRATION SNOWFLAKE_AZURE_INTEGRATION;

-- ----------------------------------------------------------------------------
-- 4. FILE FORMAT (CSV)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT BRONZE.CSV_FILEFORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    EMPTY_FIELD_AS_NULL = TRUE
    NULL_IF = ('', 'NULL')
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    SKIP_BLANK_LINES = TRUE;

-- ----------------------------------------------------------------------------
-- 5. EXTERNAL STAGE (AZURE BLOB CONTAINER)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE STAGE BRONZE.STG_AZURE_CONTAINER
    URL = '<AZURE_STORAGE_URL>'
    STORAGE_INTEGRATION = SNOWFLAKE_AZURE_INTEGRATION
    FILE_FORMAT = BRONZE.CSV_FILEFORMAT;

-- ----------------------------------------------------------------------------
-- 6. VALIDATION
-- ----------------------------------------------------------------------------
-- List files to confirm access
LIST @BRONZE.STG_AZURE_CONTAINER;

-- ----------------------------------------------------------------------------
-- 7. BRONZE LOAD STORED PROCEDURE
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE BRONZE.AZURE_LOAD_BRONZE()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    batch_start_time TIMESTAMP;
    batch_end_time   TIMESTAMP;
BEGIN
    batch_start_time := CURRENT_TIMESTAMP();

    /* =======================================================================
       CUSTOMER DEMOGRAPHICS
       ======================================================================= */
    TRUNCATE TABLE BRONZE.CUSTOMER_DEMOGRAPHICS;

    COPY INTO BRONZE.CUSTOMER_DEMOGRAPHICS
    FROM @BRONZE.STG_AZURE_CONTAINER
    FILES = ('Customer_Demographics.csv');

    /* =======================================================================
       INVENTORY DATA
       ======================================================================= */
    TRUNCATE TABLE BRONZE.INVENTORY_DATA;

    COPY INTO BRONZE.INVENTORY_DATA
    FROM @BRONZE.STG_AZURE_CONTAINER
    FILES = ('Inventory_Data.csv');

    /* =======================================================================
       LOGISTICS DATA
       ======================================================================= */
    TRUNCATE TABLE BRONZE.LOGISTICS_DATA;

    COPY INTO BRONZE.LOGISTICS_DATA
    FROM @BRONZE.STG_AZURE_CONTAINER
    FILES = ('Logistics_Data.csv');

    /* =======================================================================
       MONTHLY SEASONAL PLANNING
       ======================================================================= */
    TRUNCATE TABLE BRONZE.MONTHLY_SEASONAL_PLANNING;

    COPY INTO BRONZE.MONTHLY_SEASONAL_PLANNING
    FROM @BRONZE.STG_AZURE_CONTAINER
    FILES = ('Monthly_Seasonal_Planning.csv');

    /* =======================================================================
       PRODUCT INFORMATION
       ======================================================================= */
    TRUNCATE TABLE BRONZE.PRODUCT_INFORMATION;

    COPY INTO BRONZE.PRODUCT_INFORMATION
    FROM @BRONZE.STG_AZURE_CONTAINER
    FILES = ('Product_Information.csv');

    /* =======================================================================
       PROMOTIONS AND DISCOUNTS
       ======================================================================= */
    TRUNCATE TABLE BRONZE.PROMOTIONS_AND_DISCOUNTS;

    COPY INTO BRONZE.PROMOTIONS_AND_DISCOUNTS
    FROM @BRONZE.STG_AZURE_CONTAINER
    FILES = ('Promotions_and_Discounts.csv');

    /* =======================================================================
       SALES DATA
       ======================================================================= */
    TRUNCATE TABLE BRONZE.SALES_DATA;

    COPY INTO BRONZE.SALES_DATA
    FROM @BRONZE.STG_AZURE_CONTAINER
    FILES = ('Sales_Data.csv');

    /* =======================================================================
       SITE DETAILS
       ======================================================================= */
    TRUNCATE TABLE BRONZE.SITE_DETAILS;

    COPY INTO BRONZE.SITE_DETAILS
    FROM @BRONZE.STG_AZURE_CONTAINER
    FILES = ('Site_Details.csv');

    batch_end_time := CURRENT_TIMESTAMP();

    RETURN 'Bronze load completed successfully. Duration (seconds): ' ||
           DATEDIFF('second', batch_start_time, batch_end_time);

EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR during Bronze load: ' || SQLERRM;
END;
$$;

-- ----------------------------------------------------------------------------
-- 8. EXECUTION
-- ----------------------------------------------------------------------------
-- CALL BRONZE.AZURE_LOAD_BRONZE();
