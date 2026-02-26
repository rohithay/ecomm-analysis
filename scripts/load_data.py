#!/usr/bin/env python3
"""
scripts/load_data.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Downloads the Olist Brazilian E-Commerce dataset from
Kaggle and loads all CSVs into a DuckDB database.

Prerequisites:
  1. pip install kaggle duckdb pandas
  2. Set KAGGLE_USERNAME and KAGGLE_KEY environment
     variables (from https://www.kaggle.com/settings/account)
  3. OR place ~/.kaggle/kaggle.json

Usage:
  python scripts/load_data.py [--db dev.duckdb]

The script is idempotent: running it again will drop and
recreate all raw tables.
"""

import argparse
import os
import zipfile
from pathlib import Path

import duckdb
import pandas as pd


KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
DATA_DIR = Path("data/raw")

# CSV filename → DuckDB table name
TABLE_MAP = {
    "olist_orders_dataset.csv":                   "orders",
    "olist_customers_dataset.csv":                "customers",
    "olist_order_items_dataset.csv":              "order_items",
    "olist_products_dataset.csv":                 "products",
    "olist_sellers_dataset.csv":                  "sellers",
    "olist_order_reviews_dataset.csv":            "order_reviews",
    "olist_order_payments_dataset.csv":           "order_payments",
    "product_category_name_translation.csv":      "product_category_name_translation",
    "olist_geolocation_dataset.csv":              "geolocation",
}


def download_data():
    """Download and extract the Kaggle dataset."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Check if data is already present
    if all((DATA_DIR / csv).exists() for csv in TABLE_MAP):
        print("✓ Data already downloaded, skipping.")
        return

    print(f"⬇  Downloading {KAGGLE_DATASET} from Kaggle...")
    os.system(
        f"kaggle datasets download -d {KAGGLE_DATASET} "
        f"--path {DATA_DIR} --unzip"
    )
    print("✓ Download complete.")


def load_to_duckdb(db_path: str):
    """Load all CSVs into DuckDB, replacing existing tables."""
    con = duckdb.connect(db_path)

    for csv_file, table_name in TABLE_MAP.items():
        csv_path = DATA_DIR / csv_file
        if not csv_path.exists():
            print(f"⚠  {csv_file} not found, skipping {table_name}.")
            continue

        df = pd.read_csv(csv_path, low_memory=False)
        row_count = len(df)

        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(
            f"CREATE TABLE {table_name} AS SELECT * FROM df"
        )
        print(f"✓ Loaded {table_name:<45} ({row_count:>7,} rows)")

    con.close()


def main():
    parser = argparse.ArgumentParser(description="Load Olist data into DuckDB")
    parser.add_argument(
        "--db",
        default=os.environ.get("DUCKDB_PATH", "dev.duckdb"),
        help="Path to DuckDB file (default: dev.duckdb)",
    )
    args = parser.parse_args()

    print(f"\n🦆 Loading data into: {args.db}\n")
    download_data()
    load_to_duckdb(args.db)
    print(f"\n✅ All tables loaded into {args.db}")


if __name__ == "__main__":
    main()
