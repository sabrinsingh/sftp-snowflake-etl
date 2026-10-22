"""
01_test_connection.py
---------------------
STEP 1: Connect Python to Snowflake and run a simple verification query.

Concepts learned:
1. Environment variables (.env) keep passwords safe and out of code.
2. snowflake.connector creates an encrypted connection to Snowflake in the cloud.
3. A cursor executes SQL queries and fetches results.
"""

import os
from dotenv import load_dotenv
import snowflake.connector

# Step 1.1: Load credentials from the .env file
load_dotenv()

account = os.getenv("SNOWFLAKE_ACCOUNT")
user = os.getenv("SNOWFLAKE_USER")
password = os.getenv("SNOWFLAKE_PASSWORD")
role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

print("--- Step 1: Connecting to Snowflake ---")
print(f"Target Account: {account}")
print(f"Connecting User: {user}")

try:
    # Step 1.2: Open the connection
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role=role,
        warehouse=warehouse
    )

    # Step 1.3: Create a cursor (your tool to run SQL statements)
    cursor = conn.cursor()

    # Step 1.4: Run a simple diagnostic query
    query = "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_VERSION();"
    cursor.execute(query)

    # Step 1.5: Fetch the first row of results
    result = cursor.fetchone()

    print("\n✅ SUCCESS: Python is officially connected to your Snowflake account!")
    print(f"  • Logged in as User : {result[0]}")
    print(f"  • Current Role      : {result[1]}")
    print(f"  • Active Warehouse  : {result[2]}")
    print(f"  • Snowflake Version : {result[3]}")

    # Step 1.6: Always close resources when done
    cursor.close()
    conn.close()

except Exception as error:
    print(f"\n❌ CONNECTION FAILED: {error}")
