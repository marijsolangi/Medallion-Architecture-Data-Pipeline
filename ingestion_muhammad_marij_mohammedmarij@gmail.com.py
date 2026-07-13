#!/usr/bin/env python3
"""
================================================================================
CRM & ERP Data Ingestion Script - Bronze Layer
================================================================================
Author: Muhammad Marij
Email: mohammedmarij@gmail.com
Date: 2026-06-15

Purpose:
--------
This script ingests raw CSV data from CRM and ERP systems into PostgreSQL
as part of a Medallion Architecture (Bronze -> Silver -> Gold).

The Bronze Layer stores RAW, UNTRANSFORMED data exactly as it appears in the
source CSV files. This ensures data lineage is preserved and provides a
foundation for all downstream transformations.

Architecture:
-------------
Source CSVs          Bronze Layer (PostgreSQL)
-----------          ------------------------
CRM:
  cust_info.csv  ->  bronze.crm_cust_info
  prd_info.csv   ->  bronze.crm_prd_info
  sales_details.csv -> bronze.crm_sales_details

ERP:
  CUST_AZ12.csv  ->  bronze.erp_cust_az12
  LOC_A101.csv   ->  bronze.erp_loc_a101
  PX_CAT_G1V2.csv -> bronze.erp_px_cat_g1v2

Prerequisites:
--------------
- Python 3.8+
- PostgreSQL running locally (or accessible)
- Required packages: pandas, sqlalchemy, psycopg2-binary

Usage:
------
1. Place all 6 CSV files in a 'datasets/' folder with this structure:
   datasets/
   ├── source_crm/
   │   ├── cust_info.csv
   │   ├── prd_info.csv
   │   └── sales_details.csv
   └── source_erp/
       ├── CUST_AZ12.csv
       ├── LOC_A101.csv
       └── PX_CAT_G1V2.csv

2. Update the DATABASE_CONFIG below with your credentials.

3. Run: python ingestion_muhammad_marij_mohammedmarij@gmail.com.py
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine

# =============================================================================
# CONFIGURATION - UPDATE THESE VALUES
# =============================================================================

DATABASE_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "medallion_db",
    "user": "postgres",
    "password": "your_password_here"  # <-- CHANGE THIS
}

# Path to the datasets folder (relative to script location)
DATASETS_PATH = Path("./datasets")

# Logging configuration
LOG_DIR = Path("./logs")
LOG_DIR.mkdir(exist_ok=True)

# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging() -> logging.Logger:
    """
    Configure dual-output logging (file + console) with timestamps.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"bronze_ingestion_{timestamp}.log"

    logger = logging.getLogger("BronzeIngestion")
    logger.setLevel(logging.DEBUG)

    # Prevent duplicate handlers on re-runs
    if logger.handlers:
        logger.handlers.clear()

    # File handler - detailed logs
    fh = logging.FileHandler(log_file, mode='a')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))

    # Console handler - high-level progress
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))

    logger.addHandler(fh)
    logger.addHandler(ch)

    logger.info("=" * 70)
    logger.info("BRONZE LAYER DATA INGESTION - Session Started")
    logger.info(f"Log file: {log_file}")
    logger.info("=" * 70)

    return logger


# =============================================================================
# DATABASE CONNECTION
# =============================================================================

def get_db_engine() -> Engine:
    """
    Create a SQLAlchemy engine for PostgreSQL connection.

    Returns:
        Engine: SQLAlchemy engine instance
    """
    connection_string = (
        f"postgresql+psycopg2://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}"
        f"@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}"
        f"/{DATABASE_CONFIG['database']}"
    )
    return create_engine(connection_string)


def create_bronze_schema(engine: Engine, logger: logging.Logger) -> None:
    """
    Create the 'bronze' schema if it doesn't exist.

    The Bronze schema is the landing zone for all raw data.
    """
    logger.info("Creating Bronze schema if not exists...")
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
        conn.commit()
    logger.info("Bronze schema ready.")


# =============================================================================
# DATA VALIDATION
# =============================================================================

def validate_dataframe(
    df: pd.DataFrame, 
    table_name: str, 
    logger: logging.Logger
) -> Tuple[pd.DataFrame, Dict]:
    """
    Perform basic validation checks on a DataFrame before loading.

    Checks performed:
    1. Row count
    2. Column count and names
    3. Missing/null values per column
    4. Duplicate rows
    5. Empty strings

    Args:
        df: DataFrame to validate
        table_name: Name of the target table (for logging)
        logger: Logger instance

    Returns:
        Tuple of (validated DataFrame, validation report dict)
    """
    logger.info(f"--- Validating data for '{table_name}' ---")

    report = {
        "table": table_name,
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "columns": list(df.columns),
        "missing_values": {},
        "duplicate_rows": 0,
        "empty_strings": {},
        "warnings": []
    }

    # 1. Check for completely empty DataFrame
    if df.empty:
        logger.error(f"CRITICAL: DataFrame for '{table_name}' is completely empty!")
        report["warnings"].append("DataFrame is empty")
        return df, report

    # 2. Check for missing/null values per column
    null_counts = df.isnull().sum()
    for col in df.columns:
        null_count = null_counts[col]
        null_pct = (null_count / len(df)) * 100
        report["missing_values"][col] = {
            "count": int(null_count),
            "percentage": round(null_pct, 2)
        }
        if null_count > 0:
            logger.warning(
                f"  Column '{col}': {null_count} null values ({null_pct:.1f}%)"
            )

    # 3. Check for duplicate rows
    duplicate_count = df.duplicated().sum()
    report["duplicate_rows"] = int(duplicate_count)
    if duplicate_count > 0:
        logger.warning(
            f"  Found {duplicate_count} duplicate rows in '{table_name}'"
        )
    else:
        logger.info(f"  No duplicate rows found.")

    # 4. Check for empty strings in object columns
    for col in df.select_dtypes(include=['object']).columns:
        empty_count = (df[col].astype(str).str.strip() == '').sum()
        report["empty_strings"][col] = int(empty_count)
        if empty_count > 0:
            logger.warning(
                f"  Column '{col}': {empty_count} empty string values"
            )

    # 5. Data type inference summary
    logger.info(f"  Columns and inferred types:")
    for col, dtype in df.dtypes.items():
        logger.info(f"    - {col}: {dtype}")

    logger.info(
        f"  Validation complete: {len(df)} rows, {len(df.columns)} columns"
    )

    return df, report


# =============================================================================
# TABLE CREATION & DATA LOADING
# =============================================================================

def load_csv_to_bronze(
    engine: Engine,
    csv_path: Path,
    table_name: str,
    logger: logging.Logger,
    if_exists: str = "replace"
) -> Optional[Dict]:
    """
    Load a single CSV file into a Bronze layer table.

    Args:
        engine: SQLAlchemy engine
        csv_path: Path to the CSV file
        table_name: Full table name (e.g., 'bronze.crm_cust_info')
        logger: Logger instance
        if_exists: 'replace', 'append', or 'fail'

    Returns:
        Validation report dict, or None if failed
    """
    logger.info(f"\n{'='*50}")
    logger.info(f"Processing: {csv_path.name} -> {table_name}")
    logger.info(f"{'='*50}")

    try:
        # Read CSV
        logger.info(f"Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded {len(df)} rows from CSV.")

        # Validate
        df, report = validate_dataframe(df, table_name, logger)

        # Load to PostgreSQL
        logger.info(f"Loading data into '{table_name}'...")
        df.to_sql(
            name=table_name.split('.')[-1],
            schema=table_name.split('.')[0] if '.' in table_name else None,
            con=engine,
            if_exists=if_exists,
            index=False,
            method='multi',
            chunksize=1000
        )

        logger.info(f"SUCCESS: Loaded {len(df)} rows into '{table_name}'")
        return report

    except FileNotFoundError:
        logger.error(f"ERROR: File not found: {csv_path}")
        return None
    except Exception as e:
        logger.error(f"ERROR loading '{table_name}': {str(e)}")
        return None


def create_all_bronze_tables(engine: Engine, logger: logging.Logger) -> List[Dict]:
    """
    Create all Bronze layer tables and load data from CSV files.

    Returns:
        List of validation reports for each table
    """
    # Define the mapping of CSV files to Bronze tables
    # Format: (csv_relative_path, full_table_name)
    file_mappings: List[Tuple[str, str]] = [
        # CRM Data
        ("source_crm/cust_info.csv", "bronze.crm_cust_info"),
        ("source_crm/prd_info.csv", "bronze.crm_prd_info"),
        ("source_crm/sales_details.csv", "bronze.crm_sales_details"),
        # ERP Data
        ("source_erp/CUST_AZ12.csv", "bronze.erp_cust_az12"),
        ("source_erp/LOC_A101.csv", "bronze.erp_loc_a101"),
        ("source_erp/PX_CAT_G1V2.csv", "bronze.erp_px_cat_g1v2"),
    ]

    reports = []

    for csv_rel_path, table_name in file_mappings:
        csv_path = DATASETS_PATH / csv_rel_path
        report = load_csv_to_bronze(engine, csv_path, table_name, logger)
        if report:
            reports.append(report)

    return reports


# =============================================================================
# POST-LOAD VERIFICATION
# =============================================================================

def verify_bronze_tables(engine: Engine, logger: logging.Logger) -> None:
    """
    Run verification queries to confirm all Bronze tables were loaded correctly.
    """
    logger.info("\n" + "=" * 70)
    logger.info("POST-LOAD VERIFICATION")
    logger.info("=" * 70)

    tables = [
        "bronze.crm_cust_info",
        "bronze.crm_prd_info",
        "bronze.crm_sales_details",
        "bronze.erp_cust_az12",
        "bronze.erp_loc_a101",
        "bronze.erp_px_cat_g1v2",
    ]

    with engine.connect() as conn:
        for table in tables:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                logger.info(f"  {table}: {count} rows")
            except Exception as e:
                logger.error(f"  {table}: ERROR - {str(e)}")

    logger.info("Verification complete.")


def print_summary(reports: List[Dict], logger: logging.Logger) -> None:
    """
    Print a formatted summary of all ingestion operations.
    """
    logger.info("\n" + "=" * 70)
    logger.info("INGESTION SUMMARY REPORT")
    logger.info("=" * 70)

    total_rows = sum(r.get("total_rows", 0) for r in reports)

    for report in reports:
        logger.info(f"\nTable: {report['table']}")
        logger.info(f"  Rows: {report['total_rows']}")
        logger.info(f"  Columns: {report['total_columns']} ({', '.join(report['columns'])})")

        # Report nulls
        cols_with_nulls = [
            col for col, info in report["missing_values"].items() 
            if info["count"] > 0
        ]
        if cols_with_nulls:
            logger.info(f"  Columns with NULLs: {', '.join(cols_with_nulls)}")
        else:
            logger.info(f"  Columns with NULLs: None")

        # Report duplicates
        if report["duplicate_rows"] > 0:
            logger.warning(f"  Duplicate rows: {report['duplicate_rows']}")

    logger.info(f"\n{'='*70}")
    logger.info(f"TOTAL ROWS INGESTED: {total_rows}")
    logger.info(f"{'='*70}")


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """
    Main entry point for Bronze layer data ingestion.
    """
    logger = setup_logging()

    try:
        # Step 1: Connect to database
        logger.info("Connecting to PostgreSQL database...")
        engine = get_db_engine()

        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.scalar()
            logger.info(f"Connected to: {version}")

        # Step 2: Create Bronze schema
        create_bronze_schema(engine, logger)

        # Step 3: Load all CSV files
        logger.info("\nStarting CSV ingestion into Bronze layer...")
        reports = create_all_bronze_tables(engine, logger)

        # Step 4: Verify loads
        verify_bronze_tables(engine, logger)

        # Step 5: Print summary
        if reports:
            print_summary(reports, logger)

        logger.info("\n" + "=" * 70)
        logger.info("BRONZE LAYER INGESTION COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)

    except Exception as e:
        logger.critical(f"FATAL ERROR: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
