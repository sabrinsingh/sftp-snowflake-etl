-- ==============================================================================
-- 03_create_sales_table.sql
-- STEP 3: Define the DAILY_SALES target table
-- ==============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE RETAIL_DB;
USE SCHEMA SALES_SCHEMA;

CREATE TABLE IF NOT EXISTS DAILY_SALES (
    TRANSACTION_ID VARCHAR(100),
    TRANSACTION_DATE DATE,
    STORE_ID VARCHAR(50),
    STORE_NAME VARCHAR(255),
    PRODUCT_CATEGORY VARCHAR(100),
    PRODUCT_SKU VARCHAR(100),
    QUANTITY NUMBER(10, 0),
    UNIT_PRICE NUMBER(12, 2),
    DISCOUNT_AMOUNT NUMBER(12, 2) DEFAULT 0.00,
    TOTAL_AMOUNT NUMBER(14, 2),
    
    -- Metadata columns for audit and data lineage
    SOURCE_FILE_NAME VARCHAR(500),
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Consolidated daily retail sales table loaded from CSV drops';
