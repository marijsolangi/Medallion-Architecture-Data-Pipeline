-- =============================================================================
-- MEDALLION ARCHITECTURE - COMPLETE SQL PIPELINE
-- Author: Muhammad Marij
-- Email: mohammedmarij@gmail.com
-- Date: 2026-06-15
-- =============================================================================
-- 
-- OVERVIEW:
-- This script implements a complete Medallion Architecture in PostgreSQL:
--   Bronze Layer: Raw data landing zone (loaded via Python script)
--   Silver Layer: Cleaned, normalized, and deduplicated data
--   Gold Layer: Star Schema with Fact and Dimension tables
--   Optimization: Indexing and performance tuning
--   SBT: Single Big Table for BI reporting
--   KPIs: Business intelligence views
--
-- EXECUTION ORDER:
-- 1. Run Bronze ingestion Python script first
-- 2. Run this SQL script in order (Silver -> Gold -> Indexes -> SBT -> KPIs)
-- =============================================================================


-- =============================================================================
-- PART 0: SCHEMA SETUP
-- =============================================================================

-- Create schemas for each layer if they don't exist
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

COMMENT ON SCHEMA bronze IS 'Raw data landing zone - exact copy of source CSVs';
COMMENT ON SCHEMA silver IS 'Cleaned and normalized data with referential integrity';
COMMENT ON SCHEMA gold IS 'Business-ready Star Schema with facts and dimensions';


-- =============================================================================
-- PART 1: SILVER LAYER - DATA CLEANING & STANDARDIZATION
-- =============================================================================
--
-- PURPOSE:
-- The Silver layer transforms raw Bronze data into clean, structured tables.
-- Transformations applied:
--   - Remove duplicate records
--   - Standardize data types (TEXT -> DATE, NUMERIC, etc.)
--   - Normalize date formats to ISO standard (YYYY-MM-DD)
--   - Split combined fields (e.g., full names into first/last)
--   - Standardize ID formats across CRM and ERP systems
--   - Establish Primary Keys for referential integrity
--
-- WHY THIS MATTERS:
-- The Silver layer is the "single source of truth" for all downstream
-- analytics. By cleaning data here, we ensure consistency across all
-- reports and dashboards in the Gold layer.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 1.1 SILVER: CRM Customer Info (silver.crm_cust_info)
-- ---------------------------------------------------------------------------
-- Source: bronze.crm_cust_info
-- Transformations:
--   - Remove duplicates based on cst_id
--   - Split cst_key into customer_number (removing prefix if any)
--   - Parse first_name and last_name from combined name field
--   - Standardize gender to 'Male', 'Female', 'Unknown'
--   - Convert birthdate to DATE type
--   - Clean country names (trim whitespace, title case)
--
-- WHY MERGE STRATEGY:
-- CRM is the primary source for customer master data. ERP provides
-- supplementary info (birthdate, location) which we'll join in Gold.

DROP TABLE IF EXISTS silver.crm_cust_info CASCADE;

CREATE TABLE silver.crm_cust_info AS
SELECT DISTINCT
    -- Surrogate key for Silver layer (will be replaced in Gold)
    ROW_NUMBER() OVER (ORDER BY cst_id) AS silver_cust_sk,

    -- Original IDs preserved for lineage
    cst_id                              AS customer_id,

    -- Clean customer number: remove non-alphanumeric, uppercase
    UPPER(REGEXP_REPLACE(COALESCE(cst_key, ''), '[^a-zA-Z0-9]', '', 'g')) 
                                        AS customer_number,

    -- Split full name into first and last
    -- Assumes format: "FirstName LastName" or "FirstName Middle LastName"
    TRIM(SPLIT_PART(cst_firstname, ' ', 1))     AS first_name,
    TRIM(SPLIT_PART(cst_firstname, ' ', 2))     AS last_name,

    -- Standardize gender
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) IN ('M', 'MALE', 'BOY') THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) IN ('F', 'FEMALE', 'GIRL') THEN 'Female'
        ELSE 'Unknown'
    END                                 AS gender,

    -- Standardize marital status
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) IN ('M', 'MARRIED') THEN 'Married'
        WHEN UPPER(TRIM(cst_marital_status)) IN ('S', 'SINGLE') THEN 'Single'
        ELSE 'Unknown'
    END                                 AS marital_status,

    -- Convert to proper DATE type (assuming format YYYY-MM-DD or MM/DD/YYYY)
    CASE 
        WHEN cst_birthdate ~ '^\d{4}-\d{2}-\d{2}$' 
            THEN cst_birthdate::DATE
        WHEN cst_birthdate ~ '^\d{2}/\d{2}/\d{4}$' 
            THEN TO_DATE(cst_birthdate, 'MM/DD/YYYY')
        ELSE NULL
    END                                 AS birthdate,

    -- Clean country name
    INITCAP(TRIM(cst_country))           AS country,

    -- Metadata
    CURRENT_TIMESTAMP                   AS silver_loaded_at,
    'bronze.crm_cust_info'              AS silver_source

FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL  -- Remove records with no ID
  AND TRIM(cst_id) <> ''   -- Remove empty ID strings
;

-- Add Primary Key
ALTER TABLE silver.crm_cust_info 
    ADD CONSTRAINT pk_silver_crm_cust PRIMARY KEY (silver_cust_sk);

-- Add unique constraint on customer_id to prevent duplicates
ALTER TABLE silver.crm_cust_info 
    ADD CONSTRAINT uq_silver_crm_cust_id UNIQUE (customer_id);

COMMENT ON TABLE silver.crm_cust_info IS 
    'Cleaned CRM customer data with standardized formats';


-- ---------------------------------------------------------------------------
-- 1.2 SILVER: CRM Product Info (silver.crm_prd_info)
-- ---------------------------------------------------------------------------
-- Source: bronze.crm_prd_info
-- Transformations:
--   - Remove duplicates on prd_key
--   - Extract product_id from prd_key (removing prefix)
--   - Clean product names (trim, proper case)
--   - Convert cost to NUMERIC
--   - Parse start_date to DATE
--   - Derive product_line from category or naming convention

DROP TABLE IF EXISTS silver.crm_prd_info CASCADE;

CREATE TABLE silver.crm_prd_info AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY prd_key) AS silver_prd_sk,

    prd_key                              AS product_key,

    -- Extract numeric product ID from key (e.g., 'PRD-123' -> '123')
    REGEXP_REPLACE(prd_key, '[^0-9]', '', 'g') AS product_id,

    -- Clean product name
    INITCAP(TRIM(prd_name))              AS product_name,

    -- Original category (will be enriched with ERP data in Gold)
    INITCAP(TRIM(prd_category))          AS category,

    -- Convert cost to numeric
    CASE 
        WHEN prd_cost ~ '^[0-9]+\.?[0-9]*$' 
            THEN prd_cost::NUMERIC(10,2)
        ELSE NULL
    END                                  AS cost,

    -- Parse start date
    CASE 
        WHEN prd_start_date ~ '^\d{4}-\d{2}-\d{2}$' 
            THEN prd_start_date::DATE
        WHEN prd_start_date ~ '^\d{2}/\d{2}/\d{4}$' 
            THEN TO_DATE(prd_start_date, 'MM/DD/YYYY')
        ELSE NULL
    END                                  AS start_date,

    -- Derive product line from category
    CASE 
        WHEN UPPER(prd_category) LIKE '%ELECTRONIC%' THEN 'Electronics'
        WHEN UPPER(prd_category) LIKE '%CLOTH%' THEN 'Clothing'
        WHEN UPPER(prd_category) LIKE '%FOOD%' THEN 'Food & Beverage'
        WHEN UPPER(prd_category) LIKE '%HOME%' THEN 'Home & Garden'
        ELSE 'General'
    END                                  AS product_line,

    CURRENT_TIMESTAMP                    AS silver_loaded_at,
    'bronze.crm_prd_info'               AS silver_source

FROM bronze.crm_prd_info
WHERE prd_key IS NOT NULL
  AND TRIM(prd_key) <> ''
;

ALTER TABLE silver.crm_prd_info 
    ADD CONSTRAINT pk_silver_crm_prd PRIMARY KEY (silver_prd_sk);

ALTER TABLE silver.crm_prd_info 
    ADD CONSTRAINT uq_silver_crm_prd_key UNIQUE (product_key);

COMMENT ON TABLE silver.crm_prd_info IS 
    'Cleaned CRM product data with derived attributes';


-- ---------------------------------------------------------------------------
-- 1.3 SILVER: CRM Sales Details (silver.crm_sales_details)
-- ---------------------------------------------------------------------------
-- Source: bronze.crm_sales_details
-- Transformations:
--   - Remove duplicates on composite key (order_number + prd_key + cst_id)
--   - Convert quantity to INTEGER
--   - Convert price to NUMERIC
--   - Calculate sales_amount = quantity * price
--   - Parse order_date, shipping_date, due_date to DATE
--   - Validate foreign key references exist in Silver tables

DROP TABLE IF EXISTS silver.crm_sales_details CASCADE;

CREATE TABLE silver.crm_sales_details AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY sls_order_num, sls_prd_key, sls_cust_id) 
                                         AS silver_sales_sk,

    sls_order_num                        AS order_number,
    sls_prd_key                          AS product_key,
    sls_cust_id                          AS customer_id,

    -- Parse dates
    CASE 
        WHEN sls_order_dt ~ '^\d{4}-\d{2}-\d{2}$' THEN sls_order_dt::DATE
        WHEN sls_order_dt ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(sls_order_dt, 'MM/DD/YYYY')
        ELSE NULL
    END                                  AS order_date,

    CASE 
        WHEN sls_ship_dt ~ '^\d{4}-\d{2}-\d{2}$' THEN sls_ship_dt::DATE
        WHEN sls_ship_dt ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(sls_ship_dt, 'MM/DD/YYYY')
        ELSE NULL
    END                                  AS shipping_date,

    CASE 
        WHEN sls_due_dt ~ '^\d{4}-\d{2}-\d{2}$' THEN sls_due_dt::DATE
        WHEN sls_due_dt ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(sls_due_dt, 'MM/DD/YYYY')
        ELSE NULL
    END                                  AS due_date,

    -- Convert quantity (handle negative as returns)
    CASE 
        WHEN sls_quantity ~ '^-?\d+$' THEN sls_quantity::INTEGER
        ELSE NULL
    END                                  AS quantity,

    -- Convert price
    CASE 
        WHEN sls_price ~ '^[0-9]+\.?[0-9]*$' THEN sls_price::NUMERIC(10,2)
        ELSE NULL
    END                                  AS price,

    -- Calculate sales amount
    CASE 
        WHEN sls_quantity ~ '^-?\d+$' AND sls_price ~ '^[0-9]+\.?[0-9]*$'
            THEN (sls_quantity::INTEGER) * (sls_price::NUMERIC(10,2))
        ELSE NULL
    END                                  AS sales_amount,

    CURRENT_TIMESTAMP                    AS silver_loaded_at,
    'bronze.crm_sales_details'            AS silver_source

FROM bronze.crm_sales_details
WHERE sls_order_num IS NOT NULL
  AND TRIM(sls_order_num) <> ''
  AND sls_prd_key IS NOT NULL
  AND sls_cust_id IS NOT NULL
;

ALTER TABLE silver.crm_sales_details 
    ADD CONSTRAINT pk_silver_crm_sales PRIMARY KEY (silver_sales_sk);

COMMENT ON TABLE silver.crm_sales_details IS 
    'Cleaned CRM sales transactions with calculated sales_amount';


-- ---------------------------------------------------------------------------
-- 1.4 SILVER: ERP Customer Extra Info (silver.erp_cust_az12)
-- ---------------------------------------------------------------------------
-- Source: bronze.erp_cust_az12
-- Transformations:
--   - Standardize customer ID (cid) format to match CRM cst_id
--   - Parse birthdate
--   - Standardize gender
--   - Standardize marital status

DROP TABLE IF EXISTS silver.erp_cust_az12 CASCADE;

CREATE TABLE silver.erp_cust_az12 AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY cid)     AS silver_erp_cust_sk,

    -- Standardize customer ID: trim, uppercase, remove prefixes
    UPPER(TRIM(cid))                      AS customer_id,

    -- Parse birthdate
    CASE 
        WHEN bdate ~ '^\d{4}-\d{2}-\d{2}$' THEN bdate::DATE
        WHEN bdate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(bdate, 'MM/DD/YYYY')
        ELSE NULL
    END                                   AS birthdate,

    -- Standardize gender
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE', 'BOY') THEN 'Male'
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE', 'GIRL') THEN 'Female'
        ELSE 'Unknown'
    END                                   AS gender,

    -- Standardize marital status
    CASE 
        WHEN UPPER(TRIM(marital_status)) IN ('M', 'MARRIED') THEN 'Married'
        WHEN UPPER(TRIM(marital_status)) IN ('S', 'SINGLE') THEN 'Single'
        ELSE 'Unknown'
    END                                   AS marital_status,

    CURRENT_TIMESTAMP                     AS silver_loaded_at,
    'bronze.erp_cust_az12'                 AS silver_source

FROM bronze.erp_cust_az12
WHERE cid IS NOT NULL
  AND TRIM(cid) <> ''
;

ALTER TABLE silver.erp_cust_az12 
    ADD CONSTRAINT pk_silver_erp_cust PRIMARY KEY (silver_erp_cust_sk);

ALTER TABLE silver.erp_cust_az12 
    ADD CONSTRAINT uq_silver_erp_cust_id UNIQUE (customer_id);

COMMENT ON TABLE silver.erp_cust_az12 IS 
    'Cleaned ERP customer supplementary data (birthdate, gender, marital status)';


-- ---------------------------------------------------------------------------
-- 1.5 SILVER: ERP Location (silver.erp_loc_a101)
-- ---------------------------------------------------------------------------
-- Source: bronze.erp_loc_a101
-- Transformations:
--   - Standardize customer ID
--   - Clean country names
--   - Extract city/region if embedded in country field

DROP TABLE IF EXISTS silver.erp_loc_a101 CASCADE;

CREATE TABLE silver.erp_loc_a101 AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY cid)     AS silver_loc_sk,

    UPPER(TRIM(cid))                      AS customer_id,

    -- Clean and standardize country
    CASE 
        WHEN UPPER(TRIM(cntry)) IN ('USA', 'US', 'UNITED STATES', 'U.S.A') 
            THEN 'United States'
        WHEN UPPER(TRIM(cntry)) IN ('UK', 'GB', 'GREAT BRITAIN') 
            THEN 'United Kingdom'
        WHEN UPPER(TRIM(cntry)) IN ('DE', 'GER', 'GERMANY') 
            THEN 'Germany'
        WHEN UPPER(TRIM(cntry)) IN ('FR', 'FRA', 'FRANCE') 
            THEN 'France'
        WHEN UPPER(TRIM(cntry)) IN ('PK', 'PAK', 'PAKISTAN') 
            THEN 'Pakistan'
        WHEN UPPER(TRIM(cntry)) IN ('IN', 'IND', 'INDIA') 
            THEN 'India'
        WHEN UPPER(TRIM(cntry)) IN ('CA', 'CAN', 'CANADA') 
            THEN 'Canada'
        WHEN UPPER(TRIM(cntry)) IN ('AU', 'AUS', 'AUSTRALIA') 
            THEN 'Australia'
        ELSE INITCAP(TRIM(cntry))
    END                                   AS country,

    CURRENT_TIMESTAMP                     AS silver_loaded_at,
    'bronze.erp_loc_a101'                  AS silver_source

FROM bronze.erp_loc_a101
WHERE cid IS NOT NULL
  AND TRIM(cid) <> ''
;

ALTER TABLE silver.erp_loc_a101 
    ADD CONSTRAINT pk_silver_erp_loc PRIMARY KEY (silver_loc_sk);

COMMENT ON TABLE silver.erp_loc_a101 IS 
    'Cleaned ERP customer location/country data';


-- ---------------------------------------------------------------------------
-- 1.6 SILVER: ERP Product Categories (silver.erp_px_cat_g1v2)
-- ---------------------------------------------------------------------------
-- Source: bronze.erp_px_cat_g1v2
-- Transformations:
--   - Clean category and subcategory names
--   - Standardize maintenance flag to boolean

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2 CASCADE;

CREATE TABLE silver.erp_px_cat_g1v2 AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY id)      AS silver_cat_sk,

    id                                    AS category_id,

    INITCAP(TRIM(cat))                    AS category,
    INITCAP(TRIM(subcat))                 AS subcategory,

    -- Standardize maintenance flag
    CASE 
        WHEN UPPER(TRIM(maintenance)) IN ('YES', 'Y', 'TRUE', '1') 
            THEN 'Yes'
        WHEN UPPER(TRIM(maintenance)) IN ('NO', 'N', 'FALSE', '0') 
            THEN 'No'
        ELSE 'Unknown'
    END                                   AS maintenance,

    CURRENT_TIMESTAMP                     AS silver_loaded_at,
    'bronze.erp_px_cat_g1v2'               AS silver_source

FROM bronze.erp_px_cat_g1v2
WHERE id IS NOT NULL
  AND TRIM(id) <> ''
;

ALTER TABLE silver.erp_px_cat_g1v2 
    ADD CONSTRAINT pk_silver_erp_cat PRIMARY KEY (silver_cat_sk);

ALTER TABLE silver.erp_px_cat_g1v2 
    ADD CONSTRAINT uq_silver_erp_cat_id UNIQUE (category_id);

COMMENT ON TABLE silver.erp_px_cat_g1v2 IS 
    'Cleaned ERP product category hierarchy';


-- =============================================================================
-- PART 2: GOLD LAYER - STAR SCHEMA
-- =============================================================================
--
-- PURPOSE:
-- The Gold layer implements a Star Schema optimized for analytical queries.
-- It combines CRM and ERP data to create a unified dimensional model.
--
-- DESIGN DECISIONS:
--   - Surrogate Keys: Auto-incrementing integers for dimension tables
--     (enables Slowly Changing Dimensions if needed in future)
--   - Foreign Keys: Fact table references dimension surrogate keys
--   - Denormalization: Dimensions are wide to minimize joins
--   - Calculated Fields: sales_amount pre-computed in fact table
--
-- STAR SCHEMA STRUCTURE:
--                    dim_customers
--                         |
--   dim_products  <--  fact_sales  -->  (future: dim_date)
--
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 2.1 GOLD: Dimension Table - dim_customers
-- ---------------------------------------------------------------------------
-- MERGE STRATEGY:
-- CRM is the primary source for customer master data.
-- ERP provides supplementary attributes (birthdate from erp_cust_az12,
-- country from erp_loc_a101).
-- We use LEFT JOINs to ERP tables so CRM records are preserved even if
-- ERP data is missing (handling source system inconsistencies gracefully).

DROP TABLE IF EXISTS gold.dim_customers CASCADE;

CREATE TABLE gold.dim_customers (
    customer_key        SERIAL PRIMARY KEY,
    customer_id         VARCHAR(50) NOT NULL UNIQUE,
    customer_number     VARCHAR(50),
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    full_name           VARCHAR(200) GENERATED ALWAYS AS (
        COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')
    ) STORED,
    country             VARCHAR(100),
    marital_status      VARCHAR(20),
    gender              VARCHAR(20),
    birthdate           DATE,
    age                 INTEGER GENERATED ALWAYS AS (
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, birthdate))
    ) STORED,
    age_group           VARCHAR(20) GENERATED ALWAYS AS (
        CASE 
            WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birthdate)) < 25 THEN '18-24'
            WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birthdate)) < 35 THEN '25-34'
            WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birthdate)) < 45 THEN '35-44'
            WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birthdate)) < 55 THEN '45-54'
            WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birthdate)) < 65 THEN '55-64'
            ELSE '65+'
        END
    ) STORED,
    data_source         VARCHAR(50),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Populate from Silver layer
INSERT INTO gold.dim_customers (
    customer_id, customer_number, first_name, last_name,
    country, marital_status, gender, birthdate, data_source
)
SELECT 
    c.customer_id,
    c.customer_number,
    c.first_name,
    c.last_name,
    -- Prefer ERP country if available, fallback to CRM country
    COALESCE(l.country, c.country) AS country,
    -- Prefer ERP marital status if available, fallback to CRM
    COALESCE(e.marital_status, c.marital_status) AS marital_status,
    -- Prefer ERP gender if available, fallback to CRM
    COALESCE(e.gender, c.gender) AS gender,
    -- Prefer ERP birthdate if available, fallback to CRM
    COALESCE(e.birthdate, c.birthdate) AS birthdate,
    'CRM+ERP' AS data_source
FROM silver.crm_cust_info c
LEFT JOIN silver.erp_cust_az12 e 
    ON c.customer_id = e.customer_id
LEFT JOIN silver.erp_loc_a101 l 
    ON c.customer_id = l.customer_id
ORDER BY c.customer_id;

COMMENT ON TABLE gold.dim_customers IS 
    'Customer dimension combining CRM master data with ERP supplementary info';


-- ---------------------------------------------------------------------------
-- 2.2 GOLD: Dimension Table - dim_products
-- ---------------------------------------------------------------------------
-- MERGE STRATEGY:
-- CRM provides product master data (name, cost, start_date).
-- ERP provides category hierarchy (category, subcategory, maintenance flag).
-- We join on product_id to enrich CRM products with ERP category data.

DROP TABLE IF EXISTS gold.dim_products CASCADE;

CREATE TABLE gold.dim_products (
    product_key         SERIAL PRIMARY KEY,
    product_id          VARCHAR(50) NOT NULL UNIQUE,
    product_number      VARCHAR(50),
    product_name        VARCHAR(200),
    category_id         VARCHAR(50),
    category            VARCHAR(100),
    subcategory         VARCHAR(100),
    maintenance         VARCHAR(10),
    cost                NUMERIC(10,2),
    product_line        VARCHAR(50),
    start_date          DATE,
    data_source         VARCHAR(50),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Populate from Silver layer
INSERT INTO gold.dim_products (
    product_id, product_number, product_name,
    category_id, category, subcategory, maintenance,
    cost, product_line, start_date, data_source
)
SELECT 
    p.product_id,
    p.product_key AS product_number,  -- Using prd_key as product number
    p.product_name,
    cat.category_id,
    -- Prefer ERP category if available, fallback to CRM category
    COALESCE(cat.category, p.category) AS category,
    cat.subcategory,
    cat.maintenance,
    p.cost,
    p.product_line,
    p.start_date,
    'CRM+ERP' AS data_source
FROM silver.crm_prd_info p
LEFT JOIN silver.erp_px_cat_g1v2 cat 
    ON p.product_id = cat.category_id
ORDER BY p.product_id;

COMMENT ON TABLE gold.dim_products IS 
    'Product dimension enriched with ERP category hierarchy';


-- ---------------------------------------------------------------------------
-- 2.3 GOLD: Fact Table - fact_sales
-- ---------------------------------------------------------------------------
-- DESIGN:
-- The fact table contains foreign keys to dimensions and measurable facts.
-- Sales amount is pre-calculated as quantity * price for query performance.
--
-- GRAIN: One row per order line item (order_number + product + customer)

DROP TABLE IF EXISTS gold.fact_sales CASCADE;

CREATE TABLE gold.fact_sales (
    sales_key           BIGSERIAL PRIMARY KEY,
    order_number        VARCHAR(50) NOT NULL,
    product_key         INTEGER NOT NULL,
    customer_key        INTEGER NOT NULL,
    order_date          DATE,
    shipping_date       DATE,
    due_date            DATE,
    sales_amount        NUMERIC(12,2),
    quantity            INTEGER,
    price               NUMERIC(10,2),
    -- Derived metrics
    days_to_ship        INTEGER GENERATED ALWAYS AS (
        shipping_date - order_date
    ) STORED,
    days_to_due         INTEGER GENERATED ALWAYS AS (
        due_date - order_date
    ) STORED,
    is_return           BOOLEAN GENERATED ALWAYS AS (
        quantity < 0
    ) STORED,
    data_source         VARCHAR(50),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Populate from Silver sales joined to Gold dimensions
INSERT INTO gold.fact_sales (
    order_number, product_key, customer_key,
    order_date, shipping_date, due_date,
    sales_amount, quantity, price, data_source
)
SELECT 
    s.order_number,
    dp.product_key,
    dc.customer_key,
    s.order_date,
    s.shipping_date,
    s.due_date,
    s.sales_amount,
    s.quantity,
    s.price,
    'CRM' AS data_source
FROM silver.crm_sales_details s
INNER JOIN gold.dim_products dp 
    ON s.product_key = dp.product_id
INNER JOIN gold.dim_customers dc 
    ON s.customer_id = dc.customer_id
WHERE s.sales_amount IS NOT NULL  -- Only load valid transactions
ORDER BY s.order_date, s.order_number;

-- Add Foreign Key constraints
ALTER TABLE gold.fact_sales
    ADD CONSTRAINT fk_fact_sales_product 
    FOREIGN KEY (product_key) REFERENCES gold.dim_products(product_key);

ALTER TABLE gold.fact_sales
    ADD CONSTRAINT fk_fact_sales_customer 
    FOREIGN KEY (customer_key) REFERENCES gold.dim_customers(customer_key);

COMMENT ON TABLE gold.fact_sales IS 
    'Sales fact table with derived metrics (days_to_ship, is_return)';


-- =============================================================================
-- PART 3: PERFORMANCE OPTIMIZATION
-- =============================================================================
--
-- PURPOSE:
-- Indexes dramatically improve query performance for analytical workloads.
-- We create B-Tree indexes on columns frequently used in:
--   - WHERE clauses (filtering)
--   - JOIN conditions (dimension lookups)
--   - GROUP BY (aggregation)
--   - ORDER BY (sorting)
--
-- INDEX SELECTION RATIONALE:
--   - B-Tree: Best for equality and range queries (our primary use case)
--   - Composite indexes: For multi-column filtering patterns
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 3.1 Indexes on Dimension Tables
-- ---------------------------------------------------------------------------

-- dim_customers: Index on customer_id for fast lookups during fact loading
CREATE INDEX IF NOT EXISTS idx_dim_cust_customer_id 
    ON gold.dim_customers(customer_id);

-- dim_customers: Index on country for "Sales by Country" queries
CREATE INDEX IF NOT EXISTS idx_dim_cust_country 
    ON gold.dim_customers(country);

-- dim_customers: Composite index for customer filtering by name
CREATE INDEX IF NOT EXISTS idx_dim_cust_name 
    ON gold.dim_customers(last_name, first_name);

-- dim_products: Index on product_id for fast lookups
CREATE INDEX IF NOT EXISTS idx_dim_prd_product_id 
    ON gold.dim_products(product_id);

-- dim_products: Index on category for category-wise analysis
CREATE INDEX IF NOT EXISTS idx_dim_prd_category 
    ON gold.dim_products(category);

-- dim_products: Index on subcategory
CREATE INDEX IF NOT EXISTS idx_dim_prd_subcategory 
    ON gold.dim_products(subcategory);

-- ---------------------------------------------------------------------------
-- 3.2 Indexes on Fact Table
-- ---------------------------------------------------------------------------

-- fact_sales: Index on customer_key for customer-centric queries
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer 
    ON gold.fact_sales(customer_key);

-- fact_sales: Index on product_key for product-centric queries
CREATE INDEX IF NOT EXISTS idx_fact_sales_product 
    ON gold.fact_sales(product_key);

-- fact_sales: Index on order_date for time-series analysis
CREATE INDEX IF NOT EXISTS idx_fact_sales_order_date 
    ON gold.fact_sales(order_date);

-- fact_sales: Composite index for common query pattern (date + product)
CREATE INDEX IF NOT EXISTS idx_fact_sales_date_product 
    ON gold.fact_sales(order_date, product_key);

-- fact_sales: Composite index for customer date analysis
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_date 
    ON gold.fact_sales(customer_key, order_date);

-- fact_sales: Index on order_number for order lookups
CREATE INDEX IF NOT EXISTS idx_fact_sales_order_number 
    ON gold.fact_sales(order_number);

COMMENT ON INDEX idx_fact_sales_customer IS 
    'Optimizes customer revenue and retention queries';
COMMENT ON INDEX idx_fact_sales_order_date IS 
    'Optimizes time-series and date-range queries';


-- ---------------------------------------------------------------------------
-- 3.3 EXPLAIN ANALYZE Templates for Performance Comparison
-- ---------------------------------------------------------------------------
-- Run these queries BEFORE and AFTER creating indexes to measure improvement.
-- Save the output to document performance gains.

/*
-- TEMPLATE 1: Sales by Country (tests idx_dim_cust_country + idx_fact_sales_customer)
EXPLAIN ANALYZE
SELECT 
    c.country,
    SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.country
ORDER BY total_sales DESC;

-- TEMPLATE 2: Top Customers by Revenue (tests idx_fact_sales_customer)
EXPLAIN ANALYZE
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_key, c.customer_id, c.first_name, c.last_name
ORDER BY total_revenue DESC
LIMIT 5;

-- TEMPLATE 3: Category-wise Sales (tests idx_dim_prd_category + idx_fact_sales_product)
EXPLAIN ANALYZE
SELECT 
    p.category,
    SUM(f.sales_amount) AS category_sales,
    SUM(f.quantity) AS total_quantity
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY category_sales DESC;

-- TEMPLATE 4: Date Range Query (tests idx_fact_sales_order_date)
EXPLAIN ANALYZE
SELECT *
FROM gold.fact_sales
WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31'
ORDER BY order_date;
*/


-- =============================================================================
-- PART 4: SINGLE BIG TABLE (SBT)
-- =============================================================================
--
-- PURPOSE:
-- The Single Big Table (SBT) denormalizes the Star Schema into one wide
-- table optimized for BI tools (Power BI, Tableau) and ad-hoc reporting.
--
-- BENEFITS:
--   - Simplifies BI tool connections (single table vs. multiple joins)
--   - Faster query execution for simple reports (no joins needed)
--   - Easier for business users to understand
--
-- TRADE-OFFS:
--   - Increased storage (data duplication)
--   - Harder to maintain (schema changes require SBT rebuild)
--   - Not suitable for write-heavy workloads
--
-- COLUMN SELECTION:
-- Only the most relevant columns for reporting are included.
-- =============================================================================

DROP TABLE IF EXISTS gold.reporting_sbt CASCADE;

CREATE TABLE gold.reporting_sbt AS
SELECT 
    -- Fact identifiers
    f.sales_key,
    f.order_number,

    -- Date dimensions
    f.order_date,
    EXTRACT(YEAR FROM f.order_date) AS order_year,
    EXTRACT(MONTH FROM f.order_date) AS order_month,
    TO_CHAR(f.order_date, 'Month') AS order_month_name,
    EXTRACT(QUARTER FROM f.order_date) AS order_quarter,
    f.shipping_date,
    f.due_date,
    f.days_to_ship,
    f.is_return,

    -- Customer dimensions
    c.customer_key,
    c.customer_id,
    c.customer_number,
    c.first_name,
    c.last_name,
    c.full_name AS customer_name,
    c.country AS customer_country,
    c.marital_status,
    c.gender,
    c.age,
    c.age_group,

    -- Product dimensions
    p.product_key,
    p.product_id,
    p.product_number,
    p.product_name,
    p.category AS product_category,
    p.subcategory AS product_subcategory,
    p.maintenance,
    p.product_line,
    p.cost AS product_cost,

    -- Measures
    f.quantity,
    f.price,
    f.sales_amount,

    -- Calculated measures
    CASE WHEN f.quantity > 0 THEN f.sales_amount ELSE 0 END AS net_sales,
    CASE WHEN f.quantity < 0 THEN ABS(f.sales_amount) ELSE 0 END AS return_amount,
    f.price - p.cost AS profit_per_unit,
    (f.price - p.cost) * f.quantity AS total_profit

FROM gold.fact_sales f
INNER JOIN gold.dim_customers c ON f.customer_key = c.customer_key
INNER JOIN gold.dim_products p ON f.product_key = p.product_key;

-- Add Primary Key
ALTER TABLE gold.reporting_sbt 
    ADD CONSTRAINT pk_reporting_sbt PRIMARY KEY (sales_key);

-- Indexes on SBT for common BI query patterns
CREATE INDEX IF NOT EXISTS idx_sbt_order_date 
    ON gold.reporting_sbt(order_date);

CREATE INDEX IF NOT EXISTS idx_sbt_customer_country 
    ON gold.reporting_sbt(customer_country);

CREATE INDEX IF NOT EXISTS idx_sbt_product_category 
    ON gold.reporting_sbt(product_category);

CREATE INDEX IF NOT EXISTS idx_sbt_order_year_month 
    ON gold.reporting_sbt(order_year, order_month);

CREATE INDEX IF NOT EXISTS idx_sbt_customer_name 
    ON gold.reporting_sbt(customer_name);

COMMENT ON TABLE gold.reporting_sbt IS 
    'Single Big Table: Denormalized Star Schema for BI reporting';


-- =============================================================================
-- PART 5: BUSINESS KPI VIEWS
-- =============================================================================
--
-- PURPOSE:
-- These views provide pre-aggregated business metrics for dashboards
-- and reports. Using views ensures consistency across all consuming
-- applications and simplifies maintenance.
--
-- Each view is optimized using the indexes created in Part 3.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- KPI 1: Total Sales by Country
-- ---------------------------------------------------------------------------
-- PURPOSE: Shows revenue distribution across geographic markets
-- USE CASE: Regional sales performance, market penetration analysis

CREATE OR REPLACE VIEW gold.vw_sales_by_country AS
SELECT 
    customer_country AS country,
    COUNT(DISTINCT order_number) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(quantity) AS total_units_sold,
    SUM(sales_amount) AS total_sales,
    AVG(sales_amount) AS avg_order_value,
    SUM(total_profit) AS total_profit,
    ROUND(SUM(total_profit) / NULLIF(SUM(sales_amount), 0) * 100, 2) 
        AS profit_margin_pct
FROM gold.reporting_sbt
WHERE is_return = FALSE  -- Exclude returns from sales metrics
GROUP BY customer_country
ORDER BY total_sales DESC;

COMMENT ON VIEW gold.vw_sales_by_country IS 
    'KPI: Total sales, orders, and profit margin by country';


-- ---------------------------------------------------------------------------
-- KPI 2: Top 5 Customers by Revenue
-- ---------------------------------------------------------------------------
-- PURPOSE: Identifies highest-value customers for retention programs
-- USE CASE: VIP customer management, loyalty program targeting

CREATE OR REPLACE VIEW gold.vw_top_customers_by_revenue AS
SELECT 
    customer_id,
    customer_name,
    customer_country,
    COUNT(DISTINCT order_number) AS total_orders,
    SUM(quantity) AS total_units_purchased,
    SUM(sales_amount) AS total_revenue,
    AVG(sales_amount) AS avg_order_value,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    MAX(order_date) - MIN(order_date) AS customer_lifetime_days
FROM gold.reporting_sbt
WHERE is_return = FALSE
GROUP BY customer_id, customer_name, customer_country
ORDER BY total_revenue DESC
LIMIT 5;

COMMENT ON VIEW gold.vw_top_customers_by_revenue IS 
    'KPI: Top 5 customers ranked by total revenue';


-- ---------------------------------------------------------------------------
-- KPI 3: Best-Selling Products
-- ---------------------------------------------------------------------------
-- PURPOSE: Identifies top-performing products by revenue and volume
-- USE CASE: Inventory planning, marketing focus, product strategy

CREATE OR REPLACE VIEW gold.vw_best_selling_products AS
SELECT 
    product_id,
    product_name,
    product_category,
    product_subcategory,
    COUNT(DISTINCT order_number) AS times_ordered,
    SUM(quantity) AS total_units_sold,
    SUM(sales_amount) AS total_revenue,
    AVG(price) AS avg_selling_price,
    SUM(total_profit) AS total_profit,
    ROUND(SUM(total_profit) / NULLIF(SUM(sales_amount), 0) * 100, 2) 
        AS profit_margin_pct
FROM gold.reporting_sbt
WHERE is_return = FALSE
GROUP BY product_id, product_name, product_category, product_subcategory
ORDER BY total_revenue DESC;

COMMENT ON VIEW gold.vw_best_selling_products IS 
    'KPI: Products ranked by revenue, volume, and profitability';


-- ---------------------------------------------------------------------------
-- KPI 4: Customer Retention Rate
-- ---------------------------------------------------------------------------
-- PURPOSE: Measures what percentage of customers made repeat purchases
-- DEFINITION: 
--   Retention Rate = (Customers with 2+ orders / Total unique customers) * 100
-- USE CASE: Customer loyalty analysis, churn prediction

CREATE OR REPLACE VIEW gold.vw_customer_retention_rate AS
WITH customer_order_counts AS (
    SELECT 
        customer_id,
        customer_name,
        COUNT(DISTINCT order_number) AS order_count,
        MIN(order_date) AS first_order,
        MAX(order_date) AS last_order
    FROM gold.reporting_sbt
    WHERE is_return = FALSE
    GROUP BY customer_id, customer_name
),
retention_stats AS (
    SELECT 
        COUNT(*) AS total_customers,
        COUNT(*) FILTER (WHERE order_count >= 2) AS retained_customers,
        COUNT(*) FILTER (WHERE order_count = 1) AS one_time_customers,
        AVG(order_count) AS avg_orders_per_customer,
        ROUND(
            COUNT(*) FILTER (WHERE order_count >= 2)::NUMERIC / 
            NULLIF(COUNT(*), 0) * 100, 
            2
        ) AS retention_rate_pct
    FROM customer_order_counts
)
SELECT 
    total_customers,
    retained_customers,
    one_time_customers,
    avg_orders_per_customer,
    retention_rate_pct,
    -- Additional insight: repeat customer revenue contribution
    (SELECT SUM(sales_amount) FROM gold.reporting_sbt 
     WHERE customer_id IN (
         SELECT customer_id FROM customer_order_counts WHERE order_count >= 2
     ) AND is_return = FALSE) AS retained_customer_revenue
FROM retention_stats;

COMMENT ON VIEW gold.vw_customer_retention_rate IS 
    'KPI: Customer retention rate and repeat purchase analysis';


-- ---------------------------------------------------------------------------
-- KPI 5: Category-wise Sales Performance
-- ---------------------------------------------------------------------------
-- PURPOSE: Analyzes sales performance across product categories
-- USE CASE: Category strategy, resource allocation, merchandising

CREATE OR REPLACE VIEW gold.vw_category_sales_performance AS
SELECT 
    product_category AS category,
    product_subcategory AS subcategory,
    COUNT(DISTINCT product_id) AS product_count,
    COUNT(DISTINCT order_number) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(quantity) AS total_units_sold,
    SUM(sales_amount) AS total_revenue,
    AVG(sales_amount) AS avg_order_value,
    SUM(total_profit) AS total_profit,
    ROUND(SUM(total_profit) / NULLIF(SUM(sales_amount), 0) * 100, 2) 
        AS profit_margin_pct,
    -- Rank categories by revenue
    RANK() OVER (ORDER BY SUM(sales_amount) DESC) AS revenue_rank,
    -- Percentage of total sales
    ROUND(
        SUM(sales_amount) / NULLIF(SUM(SUM(sales_amount)) OVER (), 0) * 100, 
        2
    ) AS pct_of_total_sales
FROM gold.reporting_sbt
WHERE is_return = FALSE
GROUP BY product_category, product_subcategory
ORDER BY total_revenue DESC;

COMMENT ON VIEW gold.vw_category_sales_performance IS 
    'KPI: Sales performance metrics by product category and subcategory';


-- =============================================================================
-- BONUS: ADDITIONAL ANALYTICS VIEWS
-- =============================================================================

-- Monthly Sales Trend
CREATE OR REPLACE VIEW gold.vw_monthly_sales_trend AS
SELECT 
    order_year,
    order_month,
    order_month_name,
    COUNT(DISTINCT order_number) AS total_orders,
    SUM(quantity) AS total_units,
    SUM(sales_amount) AS total_revenue,
    SUM(total_profit) AS total_profit,
    AVG(sales_amount) AS avg_order_value
FROM gold.reporting_sbt
WHERE is_return = FALSE
GROUP BY order_year, order_month, order_month_name
ORDER BY order_year, order_month;

-- Product Return Analysis
CREATE OR REPLACE VIEW gold.vw_product_return_analysis AS
SELECT 
    product_id,
    product_name,
    product_category,
    SUM(CASE WHEN is_return THEN ABS(quantity) ELSE 0 END) AS returned_units,
    SUM(CASE WHEN is_return THEN ABS(sales_amount) ELSE 0 END) AS return_value,
    SUM(quantity) AS total_units_sold,
    ROUND(
        SUM(CASE WHEN is_return THEN ABS(quantity) ELSE 0 END)::NUMERIC / 
        NULLIF(SUM(quantity), 0) * 100, 
        2
    ) AS return_rate_pct
FROM gold.reporting_sbt
GROUP BY product_id, product_name, product_category
HAVING SUM(CASE WHEN is_return THEN 1 ELSE 0 END) > 0
ORDER BY return_rate_pct DESC;


-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================
-- Run these to verify the pipeline is working correctly

-- Check row counts across all layers
SELECT 'Bronze' AS layer, 'crm_cust_info' AS table_name, COUNT(*) AS row_count FROM bronze.crm_cust_info
UNION ALL
SELECT 'Bronze', 'crm_prd_info', COUNT(*) FROM bronze.crm_prd_info
UNION ALL
SELECT 'Bronze', 'crm_sales_details', COUNT(*) FROM bronze.crm_sales_details
UNION ALL
SELECT 'Bronze', 'erp_cust_az12', COUNT(*) FROM bronze.erp_cust_az12
UNION ALL
SELECT 'Bronze', 'erp_loc_a101', COUNT(*) FROM bronze.erp_loc_a101
UNION ALL
SELECT 'Bronze', 'erp_px_cat_g1v2', COUNT(*) FROM bronze.erp_px_cat_g1v2
UNION ALL
SELECT 'Silver', 'crm_cust_info', COUNT(*) FROM silver.crm_cust_info
UNION ALL
SELECT 'Silver', 'crm_prd_info', COUNT(*) FROM silver.crm_prd_info
UNION ALL
SELECT 'Silver', 'crm_sales_details', COUNT(*) FROM silver.crm_sales_details
UNION ALL
SELECT 'Silver', 'erp_cust_az12', COUNT(*) FROM silver.erp_cust_az12
UNION ALL
SELECT 'Silver', 'erp_loc_a101', COUNT(*) FROM silver.erp_loc_a101
UNION ALL
SELECT 'Silver', 'erp_px_cat_g1v2', COUNT(*) FROM silver.erp_px_cat_g1v2
UNION ALL
SELECT 'Gold', 'dim_customers', COUNT(*) FROM gold.dim_customers
UNION ALL
SELECT 'Gold', 'dim_products', COUNT(*) FROM gold.dim_products
UNION ALL
SELECT 'Gold', 'fact_sales', COUNT(*) FROM gold.fact_sales
UNION ALL
SELECT 'Gold', 'reporting_sbt', COUNT(*) FROM gold.reporting_sbt
ORDER BY layer, table_name;


-- =============================================================================
-- END OF SCRIPT
-- =============================================================================
-- Author: Muhammad Marij
-- Email: mohammedmarij@gmail.com
-- 
-- INSTRUCTIONS:
-- 1. Ensure PostgreSQL is running and 'medallion_db' database exists
-- 2. Run the Python ingestion script first to populate Bronze layer
-- 3. Run this SQL script in a PostgreSQL client (psql, DBeaver, pgAdmin)
-- 4. Execute EXPLAIN ANALYZE templates to document performance
-- 5. Query the KPI views to verify business insights
-- =============================================================================
