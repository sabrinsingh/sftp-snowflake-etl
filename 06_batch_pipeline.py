#!/usr/bin/env python3
"""
06_batch_pipeline.py
--------------------
STEP 6: Automated, Incremental SFTP-to-Snowflake Batch Ingestion Pipeline.

Key Engineering Features:
6.1 Multi-File SFTP Batch Discovery:
    - Scans SFTP /incoming folder for all eligible sales_*.csv files.
    - Excludes incomplete uploads (.tmp, .part, hidden files).
    - Processes files predictably in chronological order (FIFO, oldest first).
6.2 Prevent Duplicate Processing (Per-File Idempotency):
    - Uses SHA-256 digital fingerprint and audit table history.
    - Automatically skips previously successful files with identical checksums.
    - Flags checksum mismatches on same filename unless --force is passed.
6.3 Fault Isolation & Reliability:
    - Strict per-file transaction boundaries:
      One bad file rolls back and logs FAILED, but does NOT block valid files.
    - Staged metadata validation without index assumptions.
    - Clean resource management and comprehensive batch summary.
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
    """Calculates the SHA-256 hash of a local file."""
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
        print("      ✓ Failure event recorded in ETL_LOAD_AUDIT_LOG.")
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
# SFTP Client Component (Paramiko SSHClient with Host Key Verification)
# ==============================================================================
class SFTPClient:
    def __init__(self):
        self.host = (os.getenv("SFTP_HOST") or "").strip()
        self.port = int((os.getenv("SFTP_PORT", "22") or "22").strip())
        self.user = (os.getenv("SFTP_USERNAME") or "").strip()
        self.password = (os.getenv("SFTP_PASSWORD") or "").strip()
        self.remote_dir = (os.getenv("SFTP_REMOTE_DIR", "/incoming") or "/incoming").strip()
        self.pattern = (os.getenv("SFTP_FILE_PATTERN", "sales_*.csv") or "sales_*.csv").strip()
        self.strict_host_check = (os.getenv("SFTP_STRICT_HOST_KEY_CHECK", "true") or "true").strip().lower() == "true"
        self.known_hosts_file = (os.getenv("SFTP_KNOWN_HOSTS_FILE", "known_hosts") or "known_hosts").strip()

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

        if self.strict_host_check:
            self.client.set_missing_host_key_policy(paramiko.RejectPolicy())
        else:
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

    def list_eligible_files(self, specific_filename: str = None):
        """
        Discovers all eligible sales files in /incoming.
        Excludes incomplete files (.tmp, .part, hidden).
        Sorts chronologically (oldest first - FIFO order).
        """
        remote_files = self.sftp.listdir_attr(self.remote_dir)
        matched = []

        for f in remote_files:
            fname = f.filename
            # Exclude hidden files or incomplete uploads
            if fname.startswith(".") or fname.endswith((".tmp", ".part", ".download")):
                continue

            if specific_filename:
                if fname == specific_filename:
                    matched.append(f)
            elif fnmatch.fnmatch(fname, self.pattern):
                matched.append(f)

        # Sort chronologically by authoritative business date extracted from filename
        def get_business_date(file_meta):
            match = re.search(
                r"sales_(\d{4}-\d{2}-\d{2})(?:.*)?\.csv$",
                file_meta.filename,
                re.IGNORECASE
            )
            if not match:
                raise ValueError(
                    f"Filename does not contain a valid sales date: {file_meta.filename}"
                )
            return datetime.strptime(match.group(1), "%Y-%m-%d").date()

        matched.sort(key=get_business_date)
        return matched

    def download_file(self, filename: str, local_dest: str, expected_size: int):
        """Downloads a remote file and verifies local byte size matches remote metadata."""
        remote_path = f"{self.remote_dir.rstrip('/')}/{filename}"
        self.sftp.get(remote_path, local_dest)
        actual_size = os.path.getsize(local_dest)
        if actual_size != expected_size:
            raise RuntimeError(
                f"Downloaded file size mismatch: expected {expected_size} bytes, received {actual_size} bytes."
            )


# ==============================================================================
# File Ingestion Worker (Isolated Transaction Per File)
# ==============================================================================
def process_single_file(conn, cursor, sftp, file_meta, temp_dir, force: bool = False):
    """
    Ingests a single file inside an isolated transaction.
    Returns: dict with file status ('SUCCESS', 'SKIPPED', 'FAILED'), rows, revenue, error.
    """
    raw_filename = file_meta.filename
    target_file = validate_safe_filename(raw_filename)
    file_size = file_meta.st_size
    run_start_time = datetime.now(timezone.utc).replace(tzinfo=None)
    local_path = os.path.join(temp_dir, target_file)

    print(f"\n▶ Processing: {target_file} ({file_size:,} bytes, mtime: {file_meta.st_mtime})")

    # 1. Download file and verify size
    try:
        sftp.download_file(target_file, local_path, expected_size=file_size)
    except Exception as dl_err:
        print(f"   ❌ Download Failed: {dl_err}")
        return {"file": target_file, "status": "FAILED", "error": f"Download failed: {dl_err}", "rows": 0, "rev": 0.0}

    # 2. Checksum calculation
    file_checksum = calculate_file_checksum(local_path)
    print(f"   • SHA-256    : {file_checksum}")

    # 3. Duplicate Pre-Check
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

    if audit_record and not force:
        previous_checksum = audit_record[0]
        if previous_checksum == file_checksum:
            print(f"   ⚠️ Duplicate file detected: previously loaded at {audit_record[3]} ({audit_record[2]} rows).")
            print("   ↳ SKIPPED to prevent duplicate processing.")
            return {"file": target_file, "status": "SKIPPED", "error": "Already loaded with identical checksum", "rows": audit_record[2], "rev": 0.0}
        else:
            err_msg = f"Filename previously loaded with different checksum (old: {previous_checksum[:8]}..., new: {file_checksum[:8]}...). Use --force to reload."
            print(f"   ❌ Checksum Mismatch: {err_msg}")
            record_failure_event(conn, target_file, file_checksum, file_size, run_start_time, err_msg)
            return {"file": target_file, "status": "FAILED", "error": err_msg, "rows": 0, "rev": 0.0}

    if force and audit_record:
        print("   ⚠️ --force enabled: executing controlled reload.")

    # 4. Snowflake Staging via PUT
    clean_local_path = local_path.replace("\\", "/")
    put_sql = f"PUT 'file://{clean_local_path}' @STG_DAILY_SALES AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
    cursor.execute(put_sql)
    put_res = cursor.fetchone()

    if not put_res or not cursor.description:
        err_msg = "Snowflake PUT returned no result."
        record_failure_event(conn, target_file, file_checksum, file_size, run_start_time, err_msg)
        return {"file": target_file, "status": "FAILED", "error": err_msg, "rows": 0, "rev": 0.0}

    put_columns = [col[0].lower() for col in cursor.description]
    put_result = dict(zip(put_columns, put_res))

    if "target" not in put_result or "status" not in put_result:
        err_msg = f"Unexpected PUT result columns: {put_columns}"
        record_failure_event(conn, target_file, file_checksum, file_size, run_start_time, err_msg)
        return {"file": target_file, "status": "FAILED", "error": err_msg, "rows": 0, "rev": 0.0}

    put_status = str(put_result["status"]).upper()
    if put_status not in ("UPLOADED", "SKIPPED"):
        err_msg = f"Snowflake PUT status failed: {put_result}"
        record_failure_event(conn, target_file, file_checksum, file_size, run_start_time, err_msg)
        return {"file": target_file, "status": "FAILED", "error": err_msg, "rows": 0, "rev": 0.0}

    staged_filename = validate_safe_filename(os.path.basename(str(put_result["target"])))
    print(f"   • Staged to  : @STG_DAILY_SALES/{staged_filename} (Status: {put_status})")

    # 5. Atomic Ingestion Transaction (Isolated to this file)
    try:
        # A. Delete prior batch rows for this file
        delete_sql = """
            DELETE FROM DAILY_SALES 
            WHERE SOURCE_FILE_NAME = %s 
               OR SOURCE_FILE_NAME = %s;
        """
        cursor.execute(delete_sql, (target_file, staged_filename))
        cleaned_rows = cursor.rowcount
        if cleaned_rows > 0:
            print(f"   • Cleaned up {cleaned_rows} prior rows for idempotency.")

        # B. COPY INTO DAILY_SALES
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
            raise RuntimeError(f"COPY INTO status: {status_str}")
        if rows_loaded <= 0:
            raise RuntimeError(f"Zero rows loaded from file: {target_file}")

        # C. Data Quality Validation
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
        dq_rows, dq_revenue = cursor.fetchone()

        if dq_rows <= 0:
            raise RuntimeError(f"Data Quality Assertion Failed: 0 rows in DAILY_SALES for {target_file}")
        if float(dq_revenue) < 0:
            raise RuntimeError(f"Data Quality Assertion Failed: Negative total revenue ({dq_revenue})")

        # D. Record SUCCESS in audit log
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

        # E. Commit Transaction
        conn.commit()
        print(f"   ✓ Ingestion Successful: {rows_loaded} rows, ${dq_revenue:,.2f} revenue committed.")
        return {"file": target_file, "status": "SUCCESS", "error": None, "rows": rows_loaded, "rev": float(dq_revenue)}

    except Exception as txn_err:
        print(f"   ❌ Ingestion Transaction Failed: {txn_err}")
        conn.rollback()
        print("   ⚠️ Rolled back file changes.")
        record_failure_event(conn, target_file, file_checksum, file_size, run_start_time, txn_err)
        return {"file": target_file, "status": "FAILED", "error": str(txn_err), "rows": 0, "rev": 0.0}


# ==============================================================================
# Main Batch Processing Orchestrator
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="Step 6: Batch Processing & Incremental SFTP Pipeline")
    parser.add_argument("--file", type=str, help="Process a single specific filename")
    parser.add_argument("--pattern", type=str, help="Override file pattern (e.g. sales_*.csv)")
    parser.add_argument("--force", action="store_true", help="Force reload previously ingested files")
    args = parser.parse_args()

    print("==================================================================")
    print("--- Step 6: Automated Multi-File Batch SFTP Ingestion Pipeline ---")
    print("==================================================================")

    batch_start = datetime.now(timezone.utc)
    results = []

    with tempfile.TemporaryDirectory() as temp_dir:
        # Phase 1: Connect to SFTP & Discover eligible files
        print("\n--- PHASE 1: SFTP Discovery ---")
        try:
            with SFTPClient() as sftp:
                if args.pattern:
                    sftp.pattern = args.pattern

                eligible_files = sftp.list_eligible_files(args.file)
                if not eligible_files:
                    print(f"\n⚠️ No files matching pattern '{sftp.pattern}' found in '{sftp.remote_dir}'.")
                    print("Ingestion batch completed (0 files to process).")
                    return

                print(f"Found {len(eligible_files)} eligible file(s) for batch processing:")
                for idx, ef in enumerate(eligible_files, start=1):
                    print(f"   [{idx}] {ef.filename} ({ef.st_size:,} bytes)")

                # Phase 2: Connect to Snowflake
                print("\n--- PHASE 2: Snowflake Batch Connection ---")
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
                    autocommit=False
                )
                cursor = conn.cursor()

                try:
                    # Phase 3: Process each file with isolated transaction
                    print("\n--- PHASE 3: Batch Ingestion Execution ---")
                    for file_meta in eligible_files:
                        res = process_single_file(
                            conn=conn,
                            cursor=cursor,
                            sftp=sftp,
                            file_meta=file_meta,
                            temp_dir=temp_dir,
                            force=args.force
                        )
                        results.append(res)

                finally:
                    cursor.close()
                    conn.close()

        except Exception as sftp_err:
            print(f"\n❌ SFTP Session or Pipeline Orchestration Failed: {sftp_err}")
            try:
                from scripts.send_alert import send_email_alert
                send_email_alert(
                    subject="🚨 [CRITICAL ALERT] SFTP Ingestion Orchestration Failed",
                    body=f"Pipeline failed before batch could complete:\n\nError: {sftp_err}",
                    recipient="sabrinlalsingh@gmail.com"
                )
            except Exception:
                pass
            sys.exit(1)

    # Phase 4: Batch Summary Report
    batch_end = datetime.now(timezone.utc)
    duration = (batch_end - batch_start).total_seconds()

    success_cnt = sum(1 for r in results if r["status"] == "SUCCESS")
    skipped_cnt = sum(1 for r in results if r["status"] == "SKIPPED")
    failed_cnt = sum(1 for r in results if r["status"] == "FAILED")
    total_loaded_rows = sum(r["rows"] for r in results if r["status"] == "SUCCESS")
    total_loaded_rev = sum(r["rev"] for r in results if r["status"] == "SUCCESS")

    print("\n==================================================================")
    print("--- BATCH INGESTION SUMMARY REPORT ---")
    print("==================================================================")
    print(f"Total Eligible Files : {len(results)}")
    print(f"Successfully Loaded  : {success_cnt}")
    print(f"Skipped (Duplicates) : {skipped_cnt}")
    print(f"Failed (Isolated)    : {failed_cnt}")
    print(f"New Rows Loaded      : {total_loaded_rows:,}")
    print(f"New Revenue Loaded   : ${total_loaded_rev:,.2f}")
    print(f"Batch Duration       : {duration:.2f} seconds")

    print("\nDetailed File Status:")
    for r in results:
        status_icon = "✓" if r["status"] == "SUCCESS" else ("⚠️" if r["status"] == "SKIPPED" else "❌")
        note = f"{r['rows']} rows, ${r['rev']:,.2f}" if r["status"] == "SUCCESS" else r["error"]
        print(f"   {status_icon} [{r['status']:<7}] {r['file']} -> {note}")

    if failed_cnt > 0:
        print("\n⚠️ Note: Some files encountered errors, but successful files were safely committed.")
        try:
            from scripts.send_alert import send_email_alert
            failed_files = [r["file"] for r in results if r["status"] == "FAILED"]
            body_lines = [
                f"SFTP-to-Snowflake batch pipeline encountered {failed_cnt} failure(s).",
                f"Timestamp (UTC): {datetime.now(timezone.utc).isoformat()}",
                f"Failed Files: {', '.join(failed_files)}",
                "",
                "Error Details:"
            ]
            for r in results:
                if r["status"] == "FAILED":
                    body_lines.append(f" - {r['file']}: {r['error']}")
            body_lines.append("\nPlease review Snowflake table ETL_LOAD_AUDIT_LOG for complete history.")

            send_email_alert(
                subject=f"🚨 [ETL ALERT] SFTP Ingestion Failure: {failed_cnt} file(s) failed",
                body="\n".join(body_lines),
                recipient="sabrinlalsingh@gmail.com"
            )
        except Exception as alert_err:
            print(f"⚠️ Note: Failed to dispatch email alert: {alert_err}")
    else:
        print("\n✅ All batch files processed cleanly!")


if __name__ == "__main__":
    main()
