# SFTP-to-Snowflake Production ETL Pipeline

An automated, production-grade data engineering pipeline that extracts daily retail sales CSVs from an SFTP server, verifies data integrity, and loads them idempotently into a Snowflake Data Cloud table (`DAILY_SALES`).

Orchestrated daily via **GitHub Actions** with strict transaction isolation, cryptographic checksum deduplication, and an append-only audit ledger.

---

## Architecture Overview

```
                      SFTP Server (/incoming/)
                                 │
             [1. Paramiko SSHClient Host Key Validation]
                                 │
               ┌─────────────────┼─────────────────┐
               ▼                 ▼                 ▼
        sales_YYYY-MM-DD  sales_YYYY-MM-DD  sales_YYYY-MM-DD
               │                 │                 │
      [FIFO Discovery]   [FIFO Discovery]   [FIFO Discovery]
               │                 │                 │
      [SHA-256 Fingerprint Check vs ETL_LOAD_AUDIT_LOG]
               │                 │                 │
        ┌──────┴──────┐          │                 │
        ▼             ▼          │                 │
    [Matched]      [New]         │                 │
   Skip Ingestion    │           │                 │
                     ▼           ▼                 ▼
          ┌──────────────────────────────────────────────┐
          │      Isolated Snowflake Transaction          │
          │  1. Stage via PUT @STG_DAILY_SALES           │
          │  2. DELETE prior batch for file (Idempotency)│
          │  3. COPY INTO DAILY_SALES                    │
          │  4. Data Quality Assertions (in-transaction) │
          │  5. Append SUCCESS event to Audit Log        │
          │  6. COMMIT (or ROLLBACK + FAILED on error)   │
          └──────────────────────────────────────────────┘
```

---

## Key Engineering Features

1. **Strict SFTP Security & Host Key Verification**:
   - Uses `paramiko.SSHClient()` with `RejectPolicy()` against a verified `known_hosts` fingerprint.
   - Rejects unauthorized or altered server keys to prevent Man-in-the-Middle (MITM) attacks.
   - Verifies downloaded file size against remote SFTP metadata.

2. **Per-File Idempotency & Duplicate Protection**:
   - Computes SHA-256 hash immediately upon file download.
   - Verifies against `ETL_LOAD_AUDIT_LOG`. Identical files already processed are skipped in seconds with zero database pollution.
   - Detects altered content for previously loaded filenames and halts unless explicitly forced (`--force`).

3. **Atomic Ingestion & Fault Isolation**:
   - Runs each file in an independent database transaction.
   - If File $B$ contains malformed data or fails validation, File $B$'s transaction rolls back and logs a `FAILED` event, while File $A$ and File $C$ remain safely committed.

4. **Dynamic Metadata & Safe SQL**:
   - Extracts staged target filenames dynamically from Snowflake `PUT` result columns (no fixed-index assumptions).
   - Validates all filenames against strict regex to prevent SQL injection.

5. **Automated Scheduling via GitHub Actions**:
   - Scheduled daily via `.github/workflows/etl-schedule.yml` at 06:00 UTC.
   - Supports manual one-click execution with optional `--force` flag.
   - Secrets managed entirely via GitHub Repository Secrets.

---

## Database Schema

### Target Table: `DAILY_SALES`
- `TRANSACTION_ID` (VARCHAR)
- `TRANSACTION_DATE` (DATE)
- `STORE_ID` (VARCHAR)
- `STORE_NAME` (VARCHAR)
- `PRODUCT_CATEGORY` (VARCHAR)
- `PRODUCT_SKU` (VARCHAR)
- `QUANTITY` (NUMBER)
- `UNIT_PRICE` (DECIMAL(12, 2))
- `DISCOUNT_AMOUNT` (DECIMAL(12, 2))
- `TOTAL_AMOUNT` (DECIMAL(14, 2))
- `SOURCE_FILE_NAME` (VARCHAR)
- `LOADED_AT` (TIMESTAMP_NTZ)

### Audit Ledger: `ETL_LOAD_AUDIT_LOG`
Append-only log recording every load attempt:
- `LOG_ID` (NUMBER AUTOINCREMENT)
- `FILE_NAME` (VARCHAR)
- `FILE_CHECKSUM` (VARCHAR SHA-256)
- `FILE_SIZE_BYTES` (NUMBER)
- `STATUS` (VARCHAR: `SUCCESS` / `FAILED`)
- `ROWS_LOADED` (NUMBER)
- `ERROR_MESSAGE` (VARCHAR)
- `LOAD_START_TIME` (TIMESTAMP_NTZ)
- `LOAD_END_TIME` (TIMESTAMP_NTZ)

---

## Local Setup & Testing

### 1. Install Dependencies
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Environment Variables
Copy `.env.example` to `.env` and fill in your Snowflake credentials:
```bash
cp .env.example .env
```

### 3. Run the Mock SFTP Server
A pure-Python local mock SFTP server is included for testing:
```bash
python scripts/mock_sftp_server.py
```

### 4. Run the Batch Ingestion Pipeline
```bash
# Normal batch run (discovers all sales_*.csv chronologically)
python 06_batch_pipeline.py

# Force reload previously processed files
python 06_batch_pipeline.py --force

# Ingest a specific file
python 06_batch_pipeline.py --file sales_2026-10-22.csv
```

---

## GitHub Actions Secrets

To enable the automated daily workflow, configure the following secrets in **Settings > Secrets and variables > Actions**:

| Secret Name | Description | Example |
| :--- | :--- | :--- |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | `xy12345.us-east-1` |
| `SNOWFLAKE_USER` | ETL Service account user | `etl_loader` |
| `SNOWFLAKE_PASSWORD` | Service account password | `********` |
| `SNOWFLAKE_ROLE` | Warehouse execution role | `ACCOUNTADMIN` |
| `SNOWFLAKE_WAREHOUSE` | Compute warehouse | `COMPUTE_WH` |
| `SNOWFLAKE_DATABASE` | Database name | `RETAIL_DB` |
| `SNOWFLAKE_SCHEMA` | Schema name | `SALES_SCHEMA` |
| `SFTP_HOST` | Remote SFTP server hostname/IP | `sftp.retailclient.com` |
| `SFTP_PORT` | SFTP port | `22` |
| `SFTP_USERNAME` | SFTP username | `retail_client` |
| `SFTP_PASSWORD` | SFTP password | `********` |
| `SFTP_REMOTE_DIR` | Remote incoming directory | `/incoming` |
| `SFTP_FILE_PATTERN` | Matching file pattern | `sales_*.csv` |
| `SFTP_STRICT_HOST_KEY_CHECK` | Enforce known_hosts verification | `true` |
