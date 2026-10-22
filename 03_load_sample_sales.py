#!/usr/bin/env python3
"""
03_load_sample_sales.py
-----------------------
STEP 3: Stage and Load Initial Sample Sales Data into Snowflake.

Key Pipeline Steps:
1. Connect to Snowflake using credentials from .env.
2. Ensure the DAILY_SALES table exists (from 03_create_sales_table.sql).
3. Stage the local CSV file ('sample_sales.csv') to Snowflake internal stage @STG_DAILY_SALES.
4. Execute COPY INTO DAILY_SALES to ingest the staged file.
5. Query and display row counts and sample records to verify successful ingestion.
"""

import os
import sys
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()


def main():
    conn = None
    cursor = None
    sample_file = "sample_sales.csv"

    if not os.path.exists(sample_file):
        sys.stderr.write(f"Error: Sample file '{sample_file}' not found.\n")
        sys.exit(1)

    try:
        print("--- Step 3: Staging and Loading Sample Sales Data ---")

        sf_account = (os.getenv("SNOWFLAKE_ACCOUNT") or "").strip()
        sf_user = (os.getenv("SNOWFLAKE_USER") or "").strip()
        sf_pass = (os.getenv("SNOWFLAKE_PASSWORD") or "").strip()
        sf_role = (os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN") or "ACCOUNTADMIN").strip()
        sf_wh = (os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH") or "COMPUTE_WH").strip()
        sf_db = (os.getenv("SNOWFLAKE_DATABASE", "RETAIL_DB") or "RETAIL_DB").strip()
        sf_schema = (os.getenv("SNOWFLAKE_SCHEMA", "SALES_SCHEMA") or "SALES_SCHEMA").strip()

        conn = snowflake.connector.connect(
            account=sf_account,
            user=sf_user,
            password=sf_pass,
            role=sf_role,
            warehouse=sf_wh,
            database=sf_db,
            schema=sf_schema,
            autocommit=True
        )
        cursor = conn.cursor()

        # 1. Ensure DAILY_SALES table exists
        print("\n1. Ensuring DAILY_SALES table exists...")
        if os.path.exists("03_create_sales_table.sql"):
            with open("03_create_sales_table.sql", "r") as sql_f:
                for stmt in sql_f.read().split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        cursor.execute(stmt)
            print("   ✓ Table DAILY_SALES verified.")

        # 2. Upload file to internal stage @STG_DAILY_SALES
        print(f"\n2. Staging '{sample_file}' to @STG_DAILY_SALES...")
        clean_path = os.path.abspath(sample_file).replace("\\", "/")
        put_sql = f"PUT 'file://{clean_path}' @STG_DAILY_SALES AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
        cursor.execute(put_sql)
        put_result = cursor.fetchone()
        print(f"   ✓ File staged: {put_result[0]} -> {put_result[1]} ({put_result[4]})")

        staged_name = os.path.basename(put_result[1])

        # 3. Load from Stage into DAILY_SALES
        print("\n3. Ingesting into DAILY_SALES via COPY INTO...")
        copy_sql = f"""
        COPY INTO DAILY_SALES (
            TRANSACTION_ID,
            TRANSACTION_DATE,
            STORE_ID,
            STORE_NAME,
            PRODUCT_CATEGORY,
            PRODUCT_SKU,
            QUANTITY,
            UNIT_PRICE,
            DISCOUNT_AMOUNT,
            TOTAL_AMOUNT,
            SOURCE_FILE_NAME,
            LOADED_AT
        )
        FROM (
            SELECT
                t.$1,
                TO_DATE(t.$2, 'YYYY-MM-DD'),
                t.$3,
                t.$4,
                t.$5,
                t.$6,
                TO_NUMBER(t.$7),
                TO_DECIMAL(t.$8, 12, 2),
                COALESCE(TO_DECIMAL(t.$9, 12, 2), 0.00),
                TO_DECIMAL(t.$10, 14, 2),
                '{sample_file}',
                CURRENT_TIMESTAMP()
            FROM @STG_DAILY_SALES/{staged_name} t
        )
        FILE_FORMAT = (FORMAT_NAME = 'CSV_SALES_FORMAT')
        ON_ERROR = 'ABORT_STATEMENT'
        FORCE = TRUE;
        """
        cursor.execute(copy_sql)
        copy_res = cursor.fetchone()
        print(f"   ✓ COPY INTO result: Status={copy_res[0]}, Rows Loaded={copy_res[1]}")

        # 4. Verify loaded rows
        print("\n4. Verifying Loaded Data in DAILY_SALES:")
        cursor.execute("""
            SELECT 
                COUNT(*) AS row_count,
                COUNT(DISTINCT TRANSACTION_DATE) AS unique_dates,
                SUM(TOTAL_AMOUNT) AS total_revenue
            FROM DAILY_SALES;
        """)
        stats = cursor.fetchone()
        print(f"   • Total Rows   : {stats[0]}")
        print(f"   • Unique Dates : {stats[1]}")
        print(f"   • Total Revenue: ${stats[2]:,.2f}")

        print("\n✅ Step 3 complete! Sample sales data staged and loaded successfully.")

    except Exception as exc:
        sys.stderr.write(f"\n❌ Step 3 Failed: {exc}\n")
        sys.exit(1)

    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
