
"""
02_setup_database.py

STEP 2: Create and validate the Snowflake project objects.
"""

import os
from io import StringIO

from dotenv import load_dotenv
import snowflake.connector

load_dotenv()


def main():
    conn = None
    cursor = None

    try:
        print("--- Step 2: Preparing Snowflake Database Objects ---")

        # Connect using the credentials in .env
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            autocommit=True,
        )

        cursor = conn.cursor()

        # Read the SQL script
        with open("02_create_database.sql", "r", encoding="utf-8") as file:
            sql_script = file.read()

        # Execute the script safely statement by statement.
        # remove_comments=True also handles trailing comment lines.
        for index, result_cursor in enumerate(
            conn.execute_stream(StringIO(sql_script), remove_comments=True),
            start=1,
        ):
            print(f"  [{index}] Executed successfully")

            # Close each result cursor after execution
            result_cursor.close()

        print("\n--- Running Validation Queries ---")

        cursor.execute("USE DATABASE RETAIL_DB")
        cursor.execute("USE SCHEMA SALES_SCHEMA")

        # Verify the file format
        cursor.execute("SHOW FILE FORMATS IN SCHEMA SALES_SCHEMA")
        formats = [row[1] for row in cursor.fetchall()]
        print("  File formats:", formats)

        # Verify the stage
        cursor.execute("SHOW STAGES IN SCHEMA SALES_SCHEMA")
        stages = [row[1] for row in cursor.fetchall()]
        print("  Stages:", stages)

        # Verify the tables
        cursor.execute("SHOW TABLES IN SCHEMA SALES_SCHEMA")
        tables = [row[1] for row in cursor.fetchall()]
        print("  Tables:", tables)

        # Verify the audit table exists and is empty
        cursor.execute(
            "SELECT COUNT(*) FROM RETAIL_DB.SALES_SCHEMA.ETL_LOAD_AUDIT_LOG"
        )
        count = cursor.fetchone()[0]
        print(f"  Audit log rows: {count}")

        # Check that the expected objects exist
        assert "CSV_SALES_FORMAT" in formats, "CSV file format not found"
        assert "STG_DAILY_SALES" in stages, "Internal stage not found"
        assert "ETL_LOAD_AUDIT_LOG" in tables, "Audit table not found"
        assert "DAILY_SALES" in tables, "DAILY_SALES table not found"

        print("\nSUCCESS: Snowflake objects created and validated.")

    except Exception as error:
        print(f"\nFAILED: {type(error).__name__}: {error}")
        raise

    finally:
        # Always release database resources, even after an error
        if cursor is not None:
            cursor.close()

        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
