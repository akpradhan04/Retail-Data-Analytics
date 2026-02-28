/*
===============================================================================
DDL Script: Silver Layer Data Load Procedure
===============================================================================
Database      : RETAIL_DATA_ANALYTICS_DB
Schema        : silver
Procedure     : load_silver
Purpose       :
    - Loads all Silver layer tables from Bronze schema
    - Applies basic data standardization (TRIM, UPPER)
    - Performs full reload (TRUNCATE + INSERT) for dimension-style tables
    - Performs MERGE (upsert) for sales_data
    - Maintains DWH audit columns (dwh_insert_ts, dwh_update_ts)
Pre-requisites:
    - Bronze tables must be populated
    - Required roles must have INSERT, UPDATE, DELETE privileges on silver schema
Execution     :
    CALL silver.load_silver();
===============================================================================
*/

-- ----------------------------------------------------------------------------
-- Context Setup
-- ----------------------------------------------------------------------------
USE ROLE ANALYST_ROLE;
USE DATABASE RETAIL_DATA_ANALYTICS_DB;
USE SCHEMA silver;

-- ----------------------------------------------------------------------------
-- Stored Procedure: Silver Data Load
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE silver.load_silver()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time   TIMESTAMP;
BEGIN

    -- Capture batch start time
    v_start_time := CURRENT_TIMESTAMP();

    /* =======================================================================
       CUSTOMER DEMOGRAPHICS
       ======================================================================= */
    TRUNCATE TABLE silver.customer_demographics;

    INSERT INTO silver.customer_demographics
    SELECT
        TRIM(customer_id),
        age,
        UPPER(TRIM(gender)),
        TRIM(income_bracket),
        purchase_frequency,
        average_spend,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    FROM bronze.customer_demographics;


    /* =======================================================================
       PRODUCT INFORMATION
       ======================================================================= */
    TRUNCATE TABLE silver.product_information;

    INSERT INTO silver.product_information
    SELECT
        TRIM(product_id),
        TRIM(product_name),
        UPPER(TRIM(category)),
        UPPER(TRIM(subcategory)),
        unit_cost,
        unit_price,
        TRIM(supplier),
        shelf_life,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    FROM bronze.product_information;


    /* =======================================================================
       SITE DETAILS
       ======================================================================= */
    TRUNCATE TABLE silver.site_details;

    INSERT INTO silver.site_details
    SELECT
        TRIM(site_id),
        TRIM(site_name),
        UPPER(TRIM(site_format)),
        TRIM(region),
        TRIM(city),
        TRIM(state),
        store_size,
        TO_DATE(open_date,'DD-MM-YYYY'),
        UPPER(TRIM(status)),
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    FROM bronze.site_details;


    /* =======================================================================
       MONTHLY SEASONAL PLANNING
       ======================================================================= */
    TRUNCATE TABLE silver.monthly_seasonal_planning;

    INSERT INTO silver.monthly_seasonal_planning
    SELECT
        TRIM(month),
        TRIM(site_id),
        UPPER(TRIM(product_category)),
        forecasted_sales,
        actual_sales,
        seasonal_adjustment,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    FROM bronze.monthly_seasonal_planning;


    /* =======================================================================
       PROMOTIONS AND DISCOUNTS
       ======================================================================= */
    TRUNCATE TABLE silver.promotions_and_discounts;

    INSERT INTO silver.promotions_and_discounts
    SELECT
        TRIM(promotion_id),
        TRIM(product_id),
        TRIM(site_id),
        start_date,
        end_date,
        UPPER(TRIM(discount_type)),
        discount_amount,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    FROM bronze.promotions_and_discounts;


    /* =======================================================================
       LOGISTICS DATA
       ======================================================================= */
    TRUNCATE TABLE silver.logistics_data;

    INSERT INTO silver.logistics_data
    SELECT
        TRIM(shipment_id),
        TRIM(site_id),
        TRIM(product_id),
        shipment_date,
        quantity,
        UPPER(TRIM(delivery_status)),
        UPPER(TRIM(transportation_type)),
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    FROM bronze.logistics_data;


    /* =======================================================================
       INVENTORY DATA
       ======================================================================= */
    TRUNCATE TABLE silver.inventory_data;

    INSERT INTO silver.inventory_data
    SELECT
        TRIM(site_id),
        TRIM(product_id),
        beginning_inventory,
        ending_inventory,
        replenishment,
        UPPER(TRIM(stockout_flag)),
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    FROM bronze.inventory_data;


    /* =======================================================================
       SALES DATA (MERGE / UPSERT LOGIC)
       ======================================================================= */
    MERGE INTO silver.sales_data tgt
    USING (
        SELECT
            TO_DATE(sales_date,'DD-MM-YYYY') AS sales_date,
            TRIM(site_id)      AS site_id,
            TRIM(product_id)   AS product_id,
            units_sold,
            revenue,
            discounts,
            returns,
            TRIM(customer_id)  AS customer_id
        FROM bronze.sales_data
    ) src
    ON tgt.sales_date  = src.sales_date
       AND tgt.site_id = src.site_id
       AND tgt.product_id = src.product_id
       AND tgt.customer_id = src.customer_id

    WHEN MATCHED THEN UPDATE SET
        tgt.units_sold     = src.units_sold,
        tgt.revenue        = src.revenue,
        tgt.discounts      = src.discounts,
        tgt.returns        = src.returns,
        tgt.dwh_update_ts  = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT
    VALUES (
        src.sales_date,
        src.site_id,
        src.product_id,
        src.units_sold,
        src.revenue,
        src.discounts,
        src.returns,
        src.customer_id,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    );


    -- Capture batch end time
    v_end_time := CURRENT_TIMESTAMP();

    RETURN 'Silver Load Completed Successfully. Duration: ' ||
           DATEDIFF('second', v_start_time, v_end_time) || ' seconds';

END;
$$;


-- ----------------------------------------------------------------------------
-- Execution Example
-- ----------------------------------------------------------------------------
-- CALL silver.load_silver();