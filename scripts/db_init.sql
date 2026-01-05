/*
=============================================================
Create Retail Data Analytics Database and Schemas
=============================================================
Script Purpose:
    This script initializes the environment for the Retail Data
    Analytics project in Snowflake. It performs the following:
        - Sets the appropriate role and warehouse
        - Drops the existing database if it exists
        - Creates a new database
        - Creates three schemas (bronze, silver, gold) to support
          a medallion architecture

WARNING:
    Running this script will DROP the database 
    'Retail_Data_Analytics_DB' if it exists.
    All objects and data within the database will be permanently
    deleted. Ensure this is intentional before executing.
=============================================================
*/

-- Set role with sufficient privileges, use SYSADMIN to crate
USE ROLE SYSADMIN;

-- Set the compute warehouse to be used
USE WAREHOUSE COMPUTE_WH;

-- Drop the database if it already exists
DROP DATABASE IF EXISTS Retail_Data_Analytics_DB;

-- Create a new Retail Data Analytics database
CREATE DATABASE Retail_Data_Analytics_DB;

-- Switch to the newly created database
USE DATABASE Retail_Data_Analytics_DB;


/* Create schemas for the medallion architecture */

-- Bronze schema: raw, ingested source data
CREATE OR REPLACE SCHEMA bronze;

-- Silver schema: cleaned and transformed data
CREATE OR REPLACE SCHEMA silver;

-- Gold schema: business-ready data
CREATE OR REPLACE SCHEMA gold;
