# Medallion Architecture Data Pipeline

**Author:** Muhammad Marij  
**Email:** mohammedmarij@gmail.com  
**Date:** 2026-06-15

---

## Project Overview

This project implements a complete **Medallion Architecture** (Bronze → Silver → Gold) in PostgreSQL, combining CRM and ERP data sources into an analytics-ready Star Schema.

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                           SOURCE SYSTEMS                             │
├─────────────────────────────┬───────────────────────────────────────┤
│        CRM System           │           ERP System                │
│  ┌─────────────────────┐    │   ┌─────────────────────┐             │
│  │ cust_info.csv       │    │   │ CUST_AZ12.csv       │             │
│  │ prd_info.csv        │    │   │ LOC_A101.csv        │             │
│  │ sales_details.csv   │    │   │ PX_CAT_G1V2.csv     │             │
│  └──────────┬──────────┘    │   └──────────┬──────────┘             │
└─────────────┼───────────────┴──────────────┼────────────────────────┘
              │                              │
              ▼                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER (Raw)                          │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  bronze.crm_cust_info    bronze.erp_cust_az12               │  │
│  │  bronze.crm_prd_info     bronze.erp_loc_a101                │  │
│  │  bronze.crm_sales_details  bronze.erp_px_cat_g1v2            │  │
│  │                                                              │  │
│  │  Characteristics: No transformation, exact CSV copy         │  │
│  │  Purpose: Data lineage, recovery, audit trail               │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       SILVER LAYER (Cleaned)                       │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  silver.crm_cust_info      silver.erp_cust_az12            │  │
│  │  silver.crm_prd_info       silver.erp_loc_a101             │  │
│  │  silver.crm_sales_details  silver.erp_px_cat_g1v2          │  │
│  │                                                              │  │
│  │  Transformations:                                            │  │
│  │    ✓ Remove duplicates                                       │  │
│  │    ✓ Standardize data types (TEXT → DATE, NUMERIC)        │  │
│  │    ✓ Normalize date formats                                 │  │
│  │    ✓ Split combined fields (names)                          │  │
│  │    ✓ Standardize categorical values (gender, status)        │  │
│  │    ✓ Primary Keys established                               │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        GOLD LAYER (Business)                       │
│                                                                    │
│   ┌─────────────────┐         ┌─────────────────┐                 │
│   │  dim_customers  │◄────────│   fact_sales    │                 │
│   │  (Customer Dims)│         │   (Measures)    │                 │
│   └─────────────────┘         └────────┬────────┘                 │
│                                          │                         │
│   ┌─────────────────┐                    │                         │
│   │  dim_products   │◄───────────────────┘                         │
│   │  (Product Dims) │                                              │
│   └─────────────────┘                                              │
│                                                                    │
│   ┌─────────────────────────────────────────────────────────────┐ │
│   │  reporting_sbt (Single Big Table)                          │ │
│   │  - Denormalized for BI tools                                │ │
│   │  - Pre-joined dimensions + facts                            │ │
│   └─────────────────────────────────────────────────────────────┘ │
│                                                                    │
│   ┌─────────────────────────────────────────────────────────────┐ │
│   │  KPI VIEWS:                                                 │ │
│   │    • vw_sales_by_country                                    │ │
│   │    • vw_top_customers_by_revenue                           │ │
│   │    • vw_best_selling_products                              │ │
│   │    • vw_customer_retention_rate                              │ │
│   │    • vw_category_sales_performance                         │ │
│   └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Files Included

| File | Description |
|------|-------------|
| `ingestion_muhammad_marij_[email].py` | Python script for Bronze layer data loading |
| `medallion_muhammad_marij_[email].sql` | Complete SQL pipeline (Silver → Gold → KPIs) |
| `datasets/source_crm/` | Sample CRM CSV files (cust_info, prd_info, sales_details) |
| `datasets/source_erp/` | Sample ERP CSV files (CUST_AZ12, LOC_A101, PX_CAT_G1V2) |
| `README.md` | This file |

---

## Prerequisites

1. **PostgreSQL** (12+ recommended) installed and running
2. **Python 3.8+** with pip
3. Required Python packages:
   ```bash
   pip install pandas sqlalchemy psycopg2-binary
   ```

---

## Quick Start

### Step 1: Prepare Database

```sql
-- Create the database (run in psql or pgAdmin)
CREATE DATABASE medallion_db;
```

### Step 2: Update Configuration

Edit `ingestion_muhammad_marij_[email].py`:
```python
DATABASE_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "medallion_db",
    "user": "postgres",
    "password": "your_actual_password"  # <-- CHANGE THIS
}
```

### Step 3: Run Python Ingestion (Bronze Layer)

```bash
python ingestion_muhammad_marij_mohammedmarij@gmail.com.py
```

Expected output:
```
INFO: Bronze schema ready.
INFO: Processing: cust_info.csv -> bronze.crm_cust_info
INFO: SUCCESS: Loaded 10 rows into 'bronze.crm_cust_info'
...
INFO: BRONZE LAYER INGESTION COMPLETED SUCCESSFULLY
```

### Step 4: Run SQL Pipeline (Silver → Gold → KPIs)

```bash
# Using psql
psql -h localhost -U postgres -d medallion_db -f medallion_muhammad_marij_mohammedmarij@gmail.com.sql

# Or use pgAdmin/DBeaver to execute the SQL file
```

### Step 5: Verify Results

```sql
-- Check all layer row counts
SELECT * FROM gold.vw_sales_by_country;
SELECT * FROM gold.vw_top_customers_by_revenue;
SELECT * FROM gold.vw_best_selling_products;
SELECT * FROM gold.vw_customer_retention_rate;
SELECT * FROM gold.vw_category_sales_performance;
```

---

## Data Flow Explanation

### Bronze → Silver Transformations

| Source | Transformation | Rationale |
|--------|---------------|-----------|
| `cst_gndr` | `M` → `Male`, `F` → `Female` | Consistent categorical values |
| `cst_marital_status` | `S` → `Single`, `M` → `Married` | Human-readable values |
| `cst_birthdate` | Text → `DATE` type | Enables age calculations |
| `prd_cost` | Text → `NUMERIC(10,2)` | Accurate financial calculations |
| `sls_quantity × sls_price` | Calculate `sales_amount` | Pre-computed measure |
| `cntry` | `USA` → `United States` | Standardized country names |

### Silver → Gold Merges

| Gold Table | CRM Source | ERP Source | Merge Logic |
|------------|-----------|-----------|-------------|
| `dim_customers` | `silver.crm_cust_info` | `silver.erp_cust_az12` + `silver.erp_loc_a101` | COALESCE(ERP, CRM) for each attribute |
| `dim_products` | `silver.crm_prd_info` | `silver.erp_px_cat_g1v2` | LEFT JOIN on product_id/category_id |
| `fact_sales` | `silver.crm_sales_details` | — | INNER JOIN to dimensions for surrogate keys |

---

## Star Schema Design

### Dimension Tables

**`gold.dim_customers`**
- `customer_key` (PK, SERIAL) — Surrogate key
- `customer_id` (VARCHAR) — Natural key from source
- `customer_number` (VARCHAR) — Cleaned cst_key
- `first_name`, `last_name` (VARCHAR) — Split from full name
- `full_name` (GENERATED) — Computed concatenation
- `country` (VARCHAR) — From ERP (preferred) or CRM
- `marital_status`, `gender` (VARCHAR) — Standardized
- `birthdate` (DATE) — From ERP (preferred) or CRM
- `age`, `age_group` (GENERATED) — Computed from birthdate

**`gold.dim_products`**
- `product_key` (PK, SERIAL) — Surrogate key
- `product_id` (VARCHAR) — Natural key
- `product_number` (VARCHAR) — Original prd_key
- `product_name` (VARCHAR) — Cleaned name
- `category_id` (VARCHAR) — From ERP
- `category`, `subcategory` (VARCHAR) — From ERP (preferred)
- `maintenance` (VARCHAR) — Standardized Yes/No
- `cost` (NUMERIC) — From CRM
- `product_line` (VARCHAR) — Derived from category
- `start_date` (DATE) — Product launch date

### Fact Table

**`gold.fact_sales`**
- `sales_key` (PK, BIGSERIAL) — Surrogate key
- `order_number` (VARCHAR) — Degenerate dimension
- `product_key` (FK) → dim_products
- `customer_key` (FK) → dim_customers
- `order_date`, `shipping_date`, `due_date` (DATE)
- `sales_amount` (NUMERIC) — quantity × price
- `quantity` (INTEGER) — Negative for returns
- `price` (NUMERIC) — Unit price
- `days_to_ship` (GENERATED) — shipping_date - order_date
- `is_return` (GENERATED) — quantity < 0

---

## Performance Optimization

### Indexes Created

| Table | Index Name | Column(s) | Purpose |
|-------|-----------|-----------|---------|
| dim_customers | idx_dim_cust_customer_id | customer_id | Fact table joins |
| dim_customers | idx_dim_cust_country | country | "Sales by Country" queries |
| dim_products | idx_dim_prd_product_id | product_id | Fact table joins |
| dim_products | idx_dim_prd_category | category | Category analysis |
| fact_sales | idx_fact_sales_customer | customer_key | Customer revenue queries |
| fact_sales | idx_fact_sales_product | product_key | Product sales queries |
| fact_sales | idx_fact_sales_order_date | order_date | Time-series analysis |
| fact_sales | idx_fact_sales_date_product | order_date, product_key | Combined filtering |
| reporting_sbt | idx_sbt_order_date | order_date | BI date filtering |
| reporting_sbt | idx_sbt_customer_country | customer_country | Regional reports |

### Performance Testing

Run these `EXPLAIN ANALYZE` queries before and after index creation:

```sql
-- Test 1: Sales by Country
EXPLAIN ANALYZE
SELECT c.country, SUM(f.sales_amount)
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.country;

-- Test 2: Top 5 Customers
EXPLAIN ANALYZE
SELECT c.customer_id, SUM(f.sales_amount) AS revenue
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_id
ORDER BY revenue DESC LIMIT 5;
```

---

## Business KPIs

### 1. Total Sales by Country (`gold.vw_sales_by_country`)
Shows revenue distribution across markets with profit margins.

### 2. Top 5 Customers by Revenue (`gold.vw_top_customers_by_revenue`)
Identifies highest-value customers with lifetime metrics.

### 3. Best-Selling Products (`gold.vw_best_selling_products`)
Ranks products by revenue, volume, and profitability.

### 4. Customer Retention Rate (`gold.vw_customer_retention_rate`)
Calculates percentage of customers with repeat purchases.

**Formula:**
```
Retention Rate = (Customers with 2+ orders / Total customers) × 100
```

### 5. Category-wise Sales Performance (`gold.vw_category_sales_performance`)
Analyzes sales metrics by product category with rankings.

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `psycopg2.OperationalError` | Check PostgreSQL is running and credentials are correct |
| `FileNotFoundError` | Ensure CSV files are in `datasets/source_crm/` and `datasets/source_erp/` |
| `relation does not exist` | Run Python ingestion script before SQL pipeline |
| `duplicate key value` | Check for duplicate IDs in source CSVs |
| Slow queries | Verify indexes were created; run `ANALYZE` on tables |

---

## Grading Criteria Coverage

| Criteria | Implementation |
|----------|---------------|
| Data Ingestion (Bronze) | Python script with pandas + SQLAlchemy |
| Data Transformation (Silver) | SQL with deduplication, type casting, standardization |
| Business Logic (Gold) | Star Schema with fact + 2 dimension tables |
| Optimization (Indexing) | 10+ B-Tree indexes + EXPLAIN ANALYZE templates |
| Single Big Table | `gold.reporting_sbt` with 20+ columns |
| Business KPI Queries | 5 views + 2 bonus views |

---

## License

Educational project for Data Engineering coursework.

---

*End of README*
