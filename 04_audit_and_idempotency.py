"""
04_audit_and_idempotency.py
---------------------------
STEP 4: Robust Audit Log Integration & Duplicate Protection.

Engineering Highlights:
1. Real Data Quality Assertions (row count & revenue verified before commit).
2. Proper Transaction Boundaries (Ingestion Transaction separated from Informational Reporting).
3. Append-only ledger by application design (pure INSERTs).
4. Resource Cleanup: Master finally block guarantees conn.close() even on early returns.
"""

import os
import sys
import hashlib
import argparse
from datetime import datetime, timezone
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()


def calculate_file_checksum(file_path: str) -> str:
    """Calculates the SHA-256 hash (digital fingerprint) of a local file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def record_failure_event(conn, target_file, file_checksum, file_size, start_time, error):
    """
    Records a FAILED attempt into the audit ledger.
    Uses its own transaction so the failure log survives a database rollback.
    """
    try:
        cur = conn.cursor()
        fail_sql = """
            INSERT INTO ETL_LOAD_AUDIT_LOG (
                FILE_NAME, FILE_CHECKSUM, FILE_SIZE_BYTES, STATUS, 
                ERROR_MESSAGE, LOAD_START_TIME, LOAD_END_TIME
            )
            VALUES (%s, %s, %s, 'FAILED', %s, %s, CURRENT_TIMESTAMP());
        """
        err_message = str(error)[:3900]
        cur.execute(fail_sql, (
            target_file, file_checksum, file_size, err_message, start_time
        ))
        conn.commit()
        cur.close()
        print("   ✓ Failure event recorded in ETL_LOAD_AUDIT_LOG.")
    except Exception as audit_err:
        sys.stderr.write(
            f"\n[CRITICAL ALERT] Failed to write failure event to ETL_LOAD_AUDIT_LOG!\n"
            f"Original Error: {error}\n"
            f"Audit Logging Error: {audit_err}\n"
        )


def parse_copy_result(cursor, result_row):
    """
    Strict fail-safe parser for COPY INTO results.
    Validates column presence without guessing positions.
    """
    if not result_row or not cursor.description:
        raise RuntimeError("COPY INTO returned no result row.")

    col_names = [col[0].lower() for col in cursor.description]
    required = {"status", "rows_loaded"}
    missing = required - set(col_names)
    if missing:
        raise RuntimeError(f"Unexpected COPY result columns. Missing: {missing}")

    if len(result_row) != len(col_names):
        raise RuntimeError("COPY result row does not match its column metadata.")

    result = dict(zip(col_names, result_row))
    status = str(result["status"]).upper()
    try:
        rows_loaded = int(result["rows_loaded"])
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Invalid rows_loaded value in COPY result.") from exc

    return status, rows_loaded


def main():
    parser = argparse.ArgumentParser(description="Step 4: Idempotent Snowflake Ingestion with Append-Only Audit Logging")
    parser.add_argument("--force", action="store_true", help="Force reload even if file was already loaded")
    args = parser.parse_args()

    conn = None
    cursor = None
    target_file = "sample_sales.csv"
    file_path = os.path.abspath(target_file).replace("\\", "/")
    
    if not os.path.exists(file_path):
        print(f"❌ Error: File '{target_file}' not found.")
        sys.exit(1)

    file_size = os.path.getsize(file_path)
    file_checksum = calculate_file_checksum(file_path)
    run_start_time = datetime.now(timezone.utc).replace(tzinfo=None)

    print("--- Step 4: Audit Log Integration & Duplicate Protection ---")
    print(f"File Name     : {target_file}")
    print(f"File Size     : {file_size:,} bytes")
    print(f"SHA-256 Hash  : {file_checksum}")
    print(f"Start Time    : {run_start_time} (UTC naive)")

    # MASTER RESOURCE WRAPPER: Guarantees conn.close() even on early returns
    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            database="RETAIL_DB",
            schema="SALES_SCHEMA",
            autocommit=False
        )
        cursor = conn.cursor()

        # =====================================================================
        # PHASE 1: Pre-Check (Checksum-aware duplicate protection)
        # =====================================================================
        print("\n1. Checking Audit Table (ETL_LOAD_AUDIT_LOG)...")
        check_sql = """
            SELECT FILE_CHECKSUM, STATUS, ROWS_LOADED, LOAD_END_TIME
            FROM ETL_LOAD_AUDIT_LOG
            WHERE FILE_NAME = %s
              AND STATUS = 'SUCCESS'
            ORDER BY LOAD_END_TIME DESC
            LIMIT 1;
        """
        cursor.execute(check_sql, (target_file,))
        audit_record = cursor.fetchone()

        if audit_record and not args.force:
            previous_checksum = audit_record[0]
            if previous_checksum == file_checksum:
                print("\n⚠️  File already loaded successfully with identical content.")
                print("   Skipping to prevent duplicate processing.")
                print(f"   • Loaded at   : {audit_record[3]}")
                print(f"   • Rows loaded : {audit_record[2]}")
                return  # conn.close() will execute cleanly in master finally block
            raise RuntimeError(
                "This filename was previously loaded with different content. "
                "Review the change and rerun with --force only if intended."
            )

        if args.force and audit_record:
            print("   ⚠️  --force supplied: proceeding with controlled reload.")

        # Stage local file via PUT (PUT operates outside the SQL transaction)
        print("\n2. Staging local file to @STG_DAILY_SALES...")
        put_sql = f"PUT 'file://{file_path}' @STG_DAILY_SALES AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
        cursor.execute(put_sql)
        print("   ✓ File staged successfully.")

        # =====================================================================
        # PHASE 2: Atomic Ingestion Transaction (Enclosed Boundary)
        # =====================================================================
        staged_filename = f"{target_file}.gz"
        try:
            print("\n3. Loading data inside atomic transaction boundary...")

            # 1. Clean up existing rows matching logical name OR legacy staged name
            delete_sql = """
                DELETE FROM DAILY_SALES 
                WHERE SOURCE_FILE_NAME = %s 
                   OR SOURCE_FILE_NAME = %s;
            """
            cursor.execute(delete_sql, (target_file, staged_filename))
            deleted_count = cursor.rowcount
            if deleted_count > 0:
                print(f"   • Cleaned up {deleted_count} existing rows from previous load.")

            # 2. COPY INTO with strict type conversion and controlled FORCE = TRUE
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
                    '{target_file}',
                    CURRENT_TIMESTAMP()
                FROM @STG_DAILY_SALES/{staged_filename} t
            )
            FILE_FORMAT = (FORMAT_NAME = 'CSV_SALES_FORMAT')
            ON_ERROR = 'ABORT_STATEMENT'
            FORCE = TRUE;
            """
            cursor.execute(copy_sql)
            copy_res = cursor.fetchone()
            
            status_str, rows_loaded = parse_copy_result(cursor, copy_res)
            if status_str != "LOADED":
                raise RuntimeError(f"COPY INTO did not report LOADED status: {status_str}")
            if rows_loaded <= 0:
                raise RuntimeError(f"Expected sales rows, but COPY loaded {rows_loaded} rows.")

            print(f"   • COPY INTO verified: status={status_str}, rows_loaded={rows_loaded}.")

            # 3. Real Data-Quality Assertions on the loaded batch (BEFORE commit)
            print("\n4. Running in-transaction Data Quality Assertions...")
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS ROW_COUNT,
                    COALESCE(SUM(TOTAL_AMOUNT), 0) AS TOTAL_REVENUE
                FROM DAILY_SALES
                WHERE SOURCE_FILE_NAME = %s;
                """,
                (target_file,)
            )
            dq_row_count, dq_total_revenue = cursor.fetchone()
            
            if dq_row_count != 15:
                raise RuntimeError(f"Data Quality Failed: Expected 15 rows for {target_file}, found {dq_row_count}.")
            if round(float(dq_total_revenue), 2) != 2047.98:
                raise RuntimeError(f"Data Quality Failed: Unexpected revenue for {target_file}: {dq_total_revenue} (Expected 2047.98).")
            
            print(f"   ✓ Data Quality Passed: {dq_row_count} rows, ${dq_total_revenue:,.2f} verified.")

            # 4. Append SUCCESS event to ETL_LOAD_AUDIT_LOG
            print("\n5. Appending SUCCESS event to ETL_LOAD_AUDIT_LOG...")
            audit_insert = """
                INSERT INTO ETL_LOAD_AUDIT_LOG (
                    FILE_NAME, FILE_CHECKSUM, FILE_SIZE_BYTES, STATUS, 
                    ROWS_LOADED, LOAD_START_TIME, LOAD_END_TIME
                )
                VALUES (%s, %s, %s, 'SUCCESS', %s, %s, CURRENT_TIMESTAMP());
            """
            cursor.execute(audit_insert, (
                target_file, file_checksum, file_size, rows_loaded, run_start_time
            ))

            # 5. Commit Transaction
            conn.commit()
            print("   ✓ Ingestion transaction committed successfully.")

        except Exception as txn_error:
            print(f"\n❌ INGESTION FAILED: {type(txn_error).__name__}: {txn_error}")
            if conn:
                conn.rollback()
                print("   ⚠️ Ingestion changes rolled back.")
                record_failure_event(conn, target_file, file_checksum, file_size, run_start_time, txn_error)
            raise txn_error

        # =====================================================================
        # PHASE 3: Informational Post-Commit Reporting (Isolated from Ingestion)
        # =====================================================================
        try:
            print("\n6. Post-Commit Reporting (Informational):")
            cursor.execute("""
                SELECT LOG_ID, FILE_NAME, STATUS, ROWS_LOADED, FILE_CHECKSUM, LOAD_END_TIME
                FROM ETL_LOAD_AUDIT_LOG
                ORDER BY LOG_ID DESC
                LIMIT 3;
            """)
            print("   Recent Audit Log Events:")
            for r in cursor.fetchall():
                checksum = (r[4] or "N/A")[:12]
                rows = r[3] if r[3] is not None else 0
                print(f"   -> [ID {r[0]}] {r[1]} | {r[2]} | {rows} rows | {checksum}... | {r[5]}")

            cursor.execute("SELECT COUNT(*) AS total_rows, SUM(TOTAL_AMOUNT) AS total_revenue FROM DAILY_SALES;")
            summary = cursor.fetchone()
            print(f"\n   Total rows in DAILY_SALES   : {summary[0]}")
            print(f"   Total revenue in DAILY_SALES: ${summary[1]:,.2f}")

            print("\n✅ SUCCESS: Step 4 complete! Transaction boundaries & data quality assertions verified.")

        except Exception as report_error:
            print(f"\n⚠️ Note: Ingestion succeeded, but post-commit display query encountered: {report_error}")

    finally:
        # Guaranteed cleanup regardless of early return or exceptions
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()