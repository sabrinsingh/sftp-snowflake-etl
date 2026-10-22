#!/usr/bin/env python3
"""
scripts/generate_sales_data.py
--------------------------------
Generates realistic retail daily sales CSV files for SFTP ingestion testing.

Schema:
TRANSACTION_ID,TRANSACTION_DATE,STORE_ID,STORE_NAME,PRODUCT_CATEGORY,PRODUCT_SKU,QUANTITY,UNIT_PRICE,DISCOUNT_AMOUNT,TOTAL_AMOUNT
"""

import os
import csv
import argparse
from datetime import datetime, timedelta

STORES = [
    ("STR-001", "Downtown Flagship"),
    ("STR-002", "Northside Galleria"),
    ("STR-003", "Metro Mall"),
    ("STR-004", "West End Market"),
    ("STR-005", "Suburban Plaza"),
]

PRODUCTS = [
    ("Electronics", "SKU-ELEC-101", 299.99, 0.00),
    ("Apparel", "SKU-APP-202", 49.50, 10.00),
    ("Home & Kitchen", "SKU-HOME-303", 85.00, 5.00),
    ("Electronics", "SKU-ELEC-104", 150.00, 0.00),
    ("Beauty", "SKU-BEAU-405", 25.00, 0.00),
    ("Sports & Outdoors", "SKU-SPOR-506", 120.00, 15.00),
    ("Toys & Games", "SKU-TOY-607", 35.00, 0.00),
    ("Electronics", "SKU-ELEC-108", 450.00, 50.00),
    ("Grocery", "SKU-GROC-709", 12.50, 0.00),
    ("Apparel", "SKU-APP-210", 89.99, 0.00),
    ("Home & Kitchen", "SKU-HOME-311", 45.00, 5.00),
    ("Sports & Outdoors", "SKU-SPOR-512", 210.00, 20.00),
    ("Electronics", "SKU-ELEC-113", 99.00, 0.00),
    ("Beauty", "SKU-BEAU-414", 30.00, 0.00),
    ("Toys & Games", "SKU-TOY-615", 55.00, 0.00),
]


def generate_daily_file(target_date: str, output_dir: str, corrupt: bool = False) -> str:
    os.makedirs(output_dir, exist_ok=True)
    suffix = "_corrupted" if corrupt else ""
    filename = f"sales_{target_date}{suffix}.csv"
    filepath = os.path.join(output_dir, filename)

    date_clean = target_date.replace("-", "")

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "TRANSACTION_ID", "TRANSACTION_DATE", "STORE_ID", "STORE_NAME",
            "PRODUCT_CATEGORY", "PRODUCT_SKU", "QUANTITY", "UNIT_PRICE",
            "DISCOUNT_AMOUNT", "TOTAL_AMOUNT"
        ])

        total_rev = 0.0
        for i, (cat, sku, price, discount) in enumerate(PRODUCTS, start=1):
            txn_id = f"TXN-{date_clean}-{i:04d}"
            store_id, store_name = STORES[(i - 1) % len(STORES)]
            qty = 1 if i % 3 != 0 else 2
            
            # If corrupt flag is set, inject invalid data into the 3rd row
            if corrupt and i == 3:
                writer.writerow([
                    txn_id, "BAD_DATE_FORMAT", store_id, store_name,
                    cat, sku, "NOT_AN_INT", price, discount, "NOT_A_PRICE"
                ])
                continue

            line_total = round((qty * price) - discount, 2)
            total_rev += line_total
            writer.writerow([
                txn_id, target_date, store_id, store_name,
                cat, sku, qty, f"{price:.2f}", f"{discount:.2f}", f"{line_total:.2f}"
            ])

    print(f"Generated: {filepath} ({len(PRODUCTS)} rows, Revenue: ${total_rev:,.2f})")
    return filepath


def main():
    parser = argparse.ArgumentParser(description="Generate mock daily sales CSV files")
    parser.add_argument("--date", type=str, default="2026-10-20", help="Target date YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=1, help="Number of consecutive daily files to generate")
    parser.add_argument("--output-dir", type=str, default="sftp_root/incoming", help="Output directory")
    parser.add_argument("--corrupt", action="store_true", help="Generate a corrupted file for failure testing")
    args = parser.parse_args()

    base_dt = datetime.strptime(args.date, "%Y-%m-%d")
    for d in range(args.days):
        cur_date = (base_dt + timedelta(days=d)).strftime("%Y-%m-%d")
        generate_daily_file(cur_date, args.output_dir, corrupt=args.corrupt)


if __name__ == "__main__":
    main()
