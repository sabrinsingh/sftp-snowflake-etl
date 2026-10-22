-- ==============================================================================
-- 02_create_database.sql
-- STEP 2: Create the Snowflake objects for our retail pipeline
-- ==============================================================================

USE ROLE ACCOUNTADMIN;

-- 1. Create the Database
CREATE DATABASE IF NOT EXISTS RETAIL_DB
    COMMENT = 'Database for storing retail sales and analytics data';

USE DATABASE RETAIL_DB;

-- 2. Create the Schema
CREATE SCHEMA IF NOT EXISTS SALES_SCHEMA
    COMMENT = 'Schema containing daily sales tables, stages, and views';

USE SCHEMA SALES_SCHEMA;

-- 3. Define how Snowflake reads incoming CSV files
-- Strict validation: Fail visibly on column count mismatch instead of silently truncating
CREATE OR REPLACE FILE FORMAT CSV_SALES_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
    NULL_IF = ('', 'NULL', 'null');

-- 4. Create the internal stage for uploaded files
CREATE STAGE IF NOT EXISTS STG_DAILY_SALES
    FILE_FORMAT = CSV_SALES_FORMAT
    COMMENT = 'Staging area for incoming SFTP sales CSV files';

-- 5. Track file-processing attempts
-- Note: UNIQUE is omitted because Snowflake standard tables do not enforce UNIQUE constraints.
-- Idempotency is enforced explicitly in pipeline logic.
CREATE TABLE IF NOT EXISTS ETL_LOAD_AUDIT_LOG (
    LOG_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    FILE_NAME VARCHAR(500) NOT NULL,
    FILE_CHECKSUM VARCHAR(64),
    FILE_SIZE_BYTES NUMBER,
    STATUS VARCHAR(50) NOT NULL,
    ROWS_LOADED NUMBER DEFAULT 0,
    ERROR_MESSAGE VARCHAR(4000),
    LOAD_START_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    LOAD_END_TIME TIMESTAMP_NTZ
)
COMMENT = 'Audit table for tracking daily sales file processing';
