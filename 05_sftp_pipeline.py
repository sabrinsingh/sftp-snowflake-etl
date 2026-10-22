"""
05_sftp_pipeline.py
-------------------
STEP 5: Production-Grade SFTP-to-Snowflake Ingestion Pipeline.

Key Engineering Features:
1. Strict SFTP Security:
   - Requires credentials from .env (no hardcoded fallbacks).
   - Uses paramiko.SSHClient with configurable host-key policy.
   - Verifies downloaded file size against remote SFTP metadata.
2. Dynamic Staged Filename Discovery:
   - Reads the actual staged target name directly from Snowflake PUT results.
3. Safe Filename Validation:
   - Sanitizes and validates filenames to prevent injection in SQL templates.
4. Configurable Data Quality Assertions:
   - Evaluates against optional EXPECTED_ROW_COUNT and EXPECTED_TOTAL_REVENUE env vars.
   - Requires dq_row_count > 0 by default.
5. Atomic Transaction Ingestion:
   - DELETE prior batch -> COPY INTO -> DQ Validation -> Insert SUCCESS -> COMMIT.
"""

import os
import re
import sys
import fnmatch
import hashlib
import argparse
import tempfile
from datetime import datetime, timezone
from dotenv import load_dotenv
import paramiko
import snowflake.connector

load_dotenv()


# ==============================================================================
# Helper Functions
# ==============================================================================
def calculate_file_checksum(file_path: str) -> str:
    """Calculates the SHA-256 hash (digital fingerprint) of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def validate_safe_filename(filename: str) -> str:
    """Ensures filename contains only safe characters (alphanumeric, underscore, dash, dot)."""
    if not re.match(r"^[a-zA-Z0-9_\.-]+$", filename):
        raise ValueError(f"Unsafe filename detected: '{filename}'. Ingestion aborted.")
    return filename


def record_failure_event(conn, target_file, file_checksum, file_size, start_time, error):
    """Records a FAILED attempt into the audit ledger in an isolated transaction."""
    cur = None
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
        print("   ✓ Failure event recorded in ETL_LOAD_AUDIT_LOG.")
    except Exception as audit_err:
        sys.stderr.write(
            f"\n[CRITICAL ALERT] Failed to write failure event to ETL_LOAD_AUDIT_LOG!\n"
            f"Original Error: {error}\n"
            f"Audit Logging Error: {audit_err}\n"
        )
    finally:
        if cur is not None:
            cur.close()


def parse_copy_result(cursor, result_row):
    """Strict fail-safe parser for COPY INTO results."""
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


# ==============================================================================
# SFTP Downloader Component (Paramiko SSHClient)
# ==============================================================================
class SFTPDownloader:
    def __init__(self):
        # Fail fast if required environment variables are missing
        self.host = os.getenv("SFTP_HOST")
        self.port = int(os.getenv("SFTP_PORT", 22))
        self.user = os.getenv("SFTP_USERNAME")
        self.password = os.getenv("SFTP_PASSWORD")
        self.remote_dir = os.getenv("SFTP_REMOTE_DIR", "/incoming")
        self.pattern = os.getenv("SFTP_FILE_PATTERN", "sales_*.csv")
        self.strict_host_check = os.getenv("SFTP_STRICT_HOST_KEY_CHECK", "true").lower() == "true"
        self.known_hosts_file = os.getenv("SFTP_KNOWN_HOSTS_FILE", "known_hosts")

        if not self.host or not self.user or not self.password:
            raise ValueError("Missing required SFTP credentials in .env (SFTP_HOST, SFTP_USERNAME, SFTP_PASSWORD).")

        self.client = None
        self.sftp = None

    def __enter__(self):
        print(f"Connecting to SFTP server at {self.host}:{self.port} as '{self.user}'...")
        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        if os.path.exists(self.known_hosts_file):
            self.client.load_host_keys(self.known_hosts_file)

        # Host key verification policy
        if self.strict_host_check:
            self.client.set_missing_host_key_policy(paramiko.RejectPolicy())
        else:
            # Explicitly documented: local test development bypass
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self.client.connect(
            hostname=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            allow_agent=False,
            look_for_keys=False
        )
        self.sftp = self.client.open_sftp()
        print("   ✓ SFTP session established successfully.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.sftp:
            self.sftp.close()
        if self.client:
            self.client.close()
        print("   ✓ SFTP connection closed.")

    def find_eligible_file(self, specific_filename: str = None):
        """Scans the remote SFTP folder for eligible files."""
        remote_files = self.sftp.listdir_attr(self.remote_dir)
        matched = []
        for f in remote_files:
            if specific_filename:
                if f.filename == specific_filename:
                    return f
            elif fnmatch.fnmatch(f.filename, self.pattern):
                matched.append(f)

        if not matched:
            return None

        # Sort by modification time descending (newest first)
        matched.sort(key=lambda x: x.st_mtime, reverse=True)
        return matched[0]

    def download_file(self, filename: str, local_dest: str, expected_size: int):
        """Downloads file and verifies local byte size matches SFTP metadata."""
        remote_path = f"{self.remote_dir.rstrip('/')}/{filename}"
        print(f"Downloading remote file: {remote_path} -> {local_dest}...")
        self.sftp.get(remote_path, local_dest)
        
        actual_size = os.path.getsize(local_dest)
        if actual_size != expected_size:
            raise RuntimeError(
                f"Downloaded file size mismatch: expected {expected_size} bytes, received {actual_size} bytes."
            )
        print(f"   ✓ Downloaded and verified {actual_size:,} bytes.")


# ==============================================================================
# Main Pipeline
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="Step 5: Full SFTP to Snowflake Pipeline")
    parser.add_argument("--file", type=str, help="Specific filename to process from SFTP")
    parser.add_argument("--force", action="store_true", help="Force reload even if file was already loaded")
    args = parser.parse_args()

    conn = None
    cursor = None
    run_start_time = datetime.now(timezone.utc).replace(tzinfo=None)

    print("===========================================================")
    print("--- Step 5: Automated SFTP to Snowflake Pipeline ---")
    print("===========================================================")

    with tempfile.TemporaryDirectory() as temp_dir:
        # 1. Connect to SFTP & Download File
        print("\n--- PHASE 1: SFTP File Discovery & Download ---")
        try:
            with SFTPDownloader() as sftp:
                file_meta = sftp.find_eligible_file(args.file)
                if not file_meta:
                    raise FileNotFoundError(f"No files matching '{sftp.pattern}' found in SFTP folder '{sftp.remote_dir}'.")

                target_file = validate_safe_filename(file_meta.filename)
                file_size = file_meta.st_size
                local_path = os.path.join(temp_dir, target_file)
                sftp.download_file(target_file, local_path, expected_size=file_size)

        except Exception as sftp_err:
            print(f"\n❌ SFTP Extraction Failed: {sftp_err}")
            sys.exit(1)

        # 2. Compute Checksum
        file_checksum = calculate_file_checksum(local_path)
        print(f"\nTarget File  : {target_file}")
        print(f"File Size    : {file_size:,} bytes")
        print(f"SHA-256 Hash : {file_checksum}")

        failure_recorded = False
        # 3. Snowflake Ingestion Lifecycle
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

            # Pre-Check for Duplicate Load
            print("\n--- PHASE 2: Audit Pre-Check ---")
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
                    return
                raise RuntimeError(
                    "This filename was previously loaded with different content. "
                    "Review the change and rerun with --force only if intended."
                )

            if args.force and audit_record:
                print("   ⚠️  --force supplied: proceeding with controlled reload.")

            # Stage the file via PUT and validate response metadata
            print("\n--- PHASE 3: Snowflake Staging ---")
            clean_local_path = local_path.replace("\\", "/")
            put_sql = f"PUT 'file://{clean_local_path}' @STG_DAILY_SALES AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
            cursor.execute(put_sql)
            put_res = cursor.fetchone()
            
            if not put_res or not cursor.description:
                raise RuntimeError("Snowflake PUT returned no result.")
            put_columns = [col[0].lower() for col in cursor.description]
            put_result = dict(zip(put_columns, put_res))
            if "target" not in put_result or "status" not in put_result:
                raise RuntimeError(f"Unexpected PUT result columns: {put_columns}")
            put_status = str(put_result["status"]).upper()
            if put_status not in ("UPLOADED", "SKIPPED"):
                raise RuntimeError(f"Snowflake PUT failed: {put_result}")
            staged_filename = os.path.basename(str(put_result["target"]))
            if not staged_filename:
                raise RuntimeError("Snowflake PUT returned an empty target filename.")
            validate_safe_filename(staged_filename)
            print(f"   ✓ File staged: {staged_filename} (Status: {put_status}).")

            # Atomic Ingestion Transaction
            print("\n--- PHASE 4: Atomic Ingestion Transaction ---")
            try:
                # 1. Clean up previous records for this logical filename
                delete_sql = """
                    DELETE FROM DAILY_SALES 
                    WHERE SOURCE_FILE_NAME = %s 
                       OR SOURCE_FILE_NAME = %s;
                """
                cursor.execute(delete_sql, (target_file, staged_filename))
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    print(f"   • Cleaned up {deleted_count} existing rows from previous load.")

                # 2. COPY INTO using dynamically verified staged filename
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

                # 3. Configurable Data Quality Assertions (in-transaction)
                print("\n   Running Data Quality Assertions...")
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

                # Basic validation: Must contain rows
                if dq_row_count <= 0:
                    raise RuntimeError(f"Data Quality Failed: No rows loaded for {target_file}.")

                # Configurable validation against optional .env thresholds
                expected_rows = os.getenv("EXPECTED_ROW_COUNT")
                expected_revenue = os.getenv("EXPECTED_TOTAL_REVENUE")

                if expected_rows is not None and dq_row_count != int(expected_rows):
                    raise RuntimeError(f"Data Quality Failed: Expected {expected_rows} rows, found {dq_row_count}.")
                
                if expected_revenue is not None and round(float(dq_total_revenue), 2) != round(float(expected_revenue), 2):
                    raise RuntimeError(
                        f"Data Quality Failed: Expected revenue {expected_revenue}, found {dq_total_revenue}."
                    )

                print(f"   ✓ Data Quality Passed: {dq_row_count} rows, ${dq_total_revenue:,.2f} verified.")

                # 4. Append SUCCESS event to ETL_LOAD_AUDIT_LOG
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

            except Exception as txn_err:
                failure_recorded = True
                print(f"\n❌ Ingestion Failed: {txn_err}")
                if conn:
                    conn.rollback()
                    print("   ⚠️ Data changes rolled back.")
                    record_failure_event(conn, target_file, file_checksum, file_size, run_start_time, txn_err)
                raise txn_err

            # --- PHASE 5: Post-Commit Informational Reporting ---
            try:
                print("\n--- PHASE 5: Post-Commit Reporting ---")
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

                print("\n✅ SUCCESS: Step 5 complete! Full SFTP-to-Snowflake lifecycle verified.")

            except Exception as report_err:
                print(f"\n⚠️ Note: Post-commit display query encountered: {report_err}")

        except Exception as outer_err:
            if not failure_recorded and conn:
                print(f"\n❌ Pipeline Failed prior to transaction commit: {outer_err}")
                record_failure_event(conn, target_file, file_checksum, file_size, run_start_time, outer_err)
            raise outer_err

        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()


if __name__ == "__main__":
    main()



