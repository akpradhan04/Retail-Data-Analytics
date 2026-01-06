/*
=============================================================
Role-Based Access Control (RBAC) Setup
Retail Data Analytics Project
=============================================================
Script Purpose:
    This script defines and configures role-based access control
    for the Retail Data Analytics Snowflake environment.

    It performs the following actions:
        - Uses SECURITYADMIN role to manage roles and grants
        - Creates three project-specific roles:
            * VIEWER_ROLE  : Read-only access (Bronze, Silver, Gold)
            * BRONZE_ROLE  : Write access to Bronze layer
            * ANALYST_ROLE : Write access to Silver & Gold layers
        - Assigns roles to a specific user
        - Grants warehouse, database, schema, and object privileges
        - Ensures future tables and views inherit correct permissions

Architecture Alignment:
    This RBAC model aligns with a Medallion Architecture:
        - Bronze  : Raw / ingestion layer
        - Silver  : Cleaned & transformed data
        - Gold    : Business-ready analytics layer

=============================================================
*/

-- -----------------------------------------------------------
-- Use SECURITYADMIN role for role and privilege management
-- -----------------------------------------------------------
USE ROLE SECURITYADMIN;

-- -----------------------------------------------------------
-- Create project roles (idempotent)
-- -----------------------------------------------------------
CREATE ROLE IF NOT EXISTS VIEWER_ROLE;
CREATE ROLE IF NOT EXISTS BRONZE_ROLE;
CREATE ROLE IF NOT EXISTS ANALYST_ROLE;

-- -----------------------------------------------------------
-- Assign roles to user
-- -----------------------------------------------------------
GRANT ROLE VIEWER_ROLE  TO USER akpradhan;
GRANT ROLE BRONZE_ROLE  TO USER akpradhan;
GRANT ROLE ANALYST_ROLE TO USER akpradhan;

-- -----------------------------------------------------------
-- Grant warehouse usage (required to execute queries)
-- -----------------------------------------------------------
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE VIEWER_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE BRONZE_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST_ROLE;

-- -----------------------------------------------------------
-- Grant database usage
-- -----------------------------------------------------------
GRANT USAGE ON DATABASE RETAIL_DATA_ANALYTICS_DB TO ROLE VIEWER_ROLE;
GRANT USAGE ON DATABASE RETAIL_DATA_ANALYTICS_DB TO ROLE BRONZE_ROLE;
GRANT USAGE ON DATABASE RETAIL_DATA_ANALYTICS_DB TO ROLE ANALYST_ROLE;

-- ===========================================================
-- VIEWER_ROLE
-- Read-only access to all layers (Bronze, Silver, Gold)
-- ===========================================================

-- Schema usage
GRANT USAGE ON SCHEMA RETAIL_DATA_ANALYTICS_DB.bronze TO ROLE VIEWER_ROLE;
GRANT USAGE ON SCHEMA RETAIL_DATA_ANALYTICS_DB.silver TO ROLE VIEWER_ROLE;
GRANT USAGE ON SCHEMA RETAIL_DATA_ANALYTICS_DB.gold   TO ROLE VIEWER_ROLE;

-- Read access to existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA RETAIL_DATA_ANALYTICS_DB.bronze TO ROLE VIEWER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA RETAIL_DATA_ANALYTICS_DB.silver TO ROLE VIEWER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA RETAIL_DATA_ANALYTICS_DB.gold   TO ROLE VIEWER_ROLE;

-- Read access to future tables
GRANT SELECT ON FUTURE TABLES IN SCHEMA RETAIL_DATA_ANALYTICS_DB.bronze TO ROLE VIEWER_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA RETAIL_DATA_ANALYTICS_DB.silver TO ROLE VIEWER_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA RETAIL_DATA_ANALYTICS_DB.gold   TO ROLE VIEWER_ROLE;

-- Read access to existing views
GRANT SELECT ON ALL VIEWS IN SCHEMA RETAIL_DATA_ANALYTICS_DB.bronze TO ROLE VIEWER_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA RETAIL_DATA_ANALYTICS_DB.silver TO ROLE VIEWER_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA RETAIL_DATA_ANALYTICS_DB.gold   TO ROLE VIEWER_ROLE;

-- Read access to future views
GRANT SELECT ON FUTURE VIEWS IN SCHEMA RETAIL_DATA_ANALYTICS_DB.bronze TO ROLE VIEWER_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA RETAIL_DATA_ANALYTICS_DB.silver TO ROLE VIEWER_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA RETAIL_DATA_ANALYTICS_DB.gold   TO ROLE VIEWER_ROLE;

-- ===========================================================
-- BRONZE_ROLE
-- Full DML access to Bronze layer only
-- Used for ingestion and raw data loading
-- ===========================================================

GRANT USAGE ON SCHEMA RETAIL_DATA_ANALYTICS_DB.bronze TO ROLE BRONZE_ROLE;

-- Table write permissions
GRANT INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA RETAIL_DATA_ANALYTICS_DB.bronze
TO ROLE BRONZE_ROLE;

GRANT INSERT, UPDATE, DELETE
ON FUTURE TABLES IN SCHEMA RETAIL_DATA_ANALYTICS_DB.bronze
TO ROLE BRONZE_ROLE;

-- View write permissions (if applicable)
GRANT INSERT, UPDATE, DELETE
ON ALL VIEWS IN SCHEMA RETAIL_DATA_ANALYTICS_DB.bronze
TO ROLE BRONZE_ROLE;

GRANT INSERT, UPDATE, DELETE
ON FUTURE VIEWS IN SCHEMA RETAIL_DATA_ANALYTICS_DB.bronze
TO ROLE BRONZE_ROLE;

-- ===========================================================
-- ANALYST_ROLE
-- Full DML access to Silver & Gold layers
-- Used for transformations, analytics, and reporting
-- ===========================================================

/* Silver layer access */
GRANT USAGE ON SCHEMA RETAIL_DATA_ANALYTICS_DB.silver TO ROLE ANALYST_ROLE;

GRANT INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA RETAIL_DATA_ANALYTICS_DB.silver
TO ROLE ANALYST_ROLE;

GRANT INSERT, UPDATE, DELETE
ON FUTURE TABLES IN SCHEMA RETAIL_DATA_ANALYTICS_DB.silver
TO ROLE ANALYST_ROLE;

GRANT INSERT, UPDATE, DELETE
ON ALL VIEWS IN SCHEMA RETAIL_DATA_ANALYTICS_DB.silver
TO ROLE ANALYST_ROLE;

GRANT INSERT, UPDATE, DELETE
ON FUTURE VIEWS IN SCHEMA RETAIL_DATA_ANALYTICS_DB.silver
TO ROLE ANALYST_ROLE;

/* Gold layer access */
GRANT USAGE ON SCHEMA RETAIL_DATA_ANALYTICS_DB.gold TO ROLE ANALYST_ROLE;

GRANT INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA RETAIL_DATA_ANALYTICS_DB.gold
TO ROLE ANALYST_ROLE;

GRANT INSERT, UPDATE, DELETE
ON FUTURE TABLES IN SCHEMA RETAIL_DATA_ANALYTICS_DB.gold
TO ROLE ANALYST_ROLE;

GRANT INSERT, UPDATE, DELETE
ON ALL VIEWS IN SCHEMA RETAIL_DATA_ANALYTICS_DB.gold
TO ROLE ANALYST_ROLE;

GRANT INSERT, UPDATE, DELETE
ON FUTURE VIEWS IN SCHEMA RETAIL_DATA_ANALYTICS_DB.gold
TO ROLE ANALYST_ROLE;
