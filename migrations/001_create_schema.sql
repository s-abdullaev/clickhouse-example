-- ============================================================================
-- Star Schema for Retail Data Warehouse
-- Source: "Designing Data-Intensive Applications" by Martin Kleppmann, Ch. 3
--
-- ClickHouse-specific design decisions:
--   - MergeTree engine: the base engine for all analytical tables
--   - LowCardinality(String): dictionary-encodes low-cardinality string columns
--     (state, category, weekday) for better compression and faster scans
--   - PARTITION BY on fact table: splits data into monthly parts on disk,
--     allowing ClickHouse to skip entire partitions during queries
--   - ORDER BY (primary index): ClickHouse stores data sorted by this key,
--     enabling sparse indexing and fast range scans
--   - Nullable(T): used only for FK columns that can legally be absent
-- ============================================================================

CREATE DATABASE IF NOT EXISTS retail_dw;

-- ----------------------------------------------------------------------------
-- Dimension: Products  (what was sold)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS retail_dw.dim_product
(
    product_sk  UInt32                   COMMENT 'Surrogate key',
    sku         String                   COMMENT 'Natural key (Stock Keeping Unit)',
    description String,
    brand       LowCardinality(String),
    category    LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY product_sk
COMMENT 'Product dimension — describes the item sold';

-- ----------------------------------------------------------------------------
-- Dimension: Stores  (where it was sold)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS retail_dw.dim_store
(
    store_sk UInt32                   COMMENT 'Surrogate key',
    state    LowCardinality(String)   COMMENT 'US state abbreviation',
    city     String
)
ENGINE = MergeTree()
ORDER BY store_sk
COMMENT 'Store dimension — physical retail location';

-- ----------------------------------------------------------------------------
-- Dimension: Dates  (when it was sold)
-- date_key format: YYMMDD as a 6-digit integer (e.g. 140102 = 2014-01-02)
-- Storing date attributes separately avoids runtime date-parsing functions
-- during aggregation — a classic dimensional modeling pattern.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS retail_dw.dim_date
(
    date_key   UInt32                   COMMENT 'Surrogate key: YYMMDD integer',
    year       UInt16,
    month      LowCardinality(String)   COMMENT 'Three-letter lowercase: jan, feb, ...',
    day        UInt8,
    weekday    LowCardinality(String)   COMMENT 'Three-letter lowercase: mon, tue, ...',
    is_holiday UInt8                    COMMENT '1 = public holiday, 0 = regular day'
)
ENGINE = MergeTree()
ORDER BY date_key
COMMENT 'Date dimension — calendar attributes for time-based roll-ups';

-- ----------------------------------------------------------------------------
-- Dimension: Customers  (who bought — loyalty card holders only)
-- Anonymous purchases appear in fact_sales with customer_sk = NULL.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS retail_dw.dim_customer
(
    customer_sk   UInt32  COMMENT 'Surrogate key',
    name          String,
    date_of_birth Date
)
ENGINE = MergeTree()
ORDER BY customer_sk
COMMENT 'Customer dimension — identified shoppers (loyalty programme members)';

-- ----------------------------------------------------------------------------
-- Dimension: Promotions  (was a promotion running?)
-- coupon_type is NULL when no physical coupon was involved.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS retail_dw.dim_promotion
(
    promotion_sk UInt32                  COMMENT 'Surrogate key',
    name         String,
    ad_type      LowCardinality(String)  COMMENT 'Advertising channel',
    coupon_type  Nullable(String)        COMMENT 'Coupon format, or NULL if no coupon'
)
ENGINE = MergeTree()
ORDER BY promotion_sk
COMMENT 'Promotion dimension — active marketing campaign at time of sale';

-- ----------------------------------------------------------------------------
-- Fact: Sales  (one row per product line per transaction)
--
-- PARTITION BY intDiv(date_key, 100):
--   intDiv(140102, 100) = 1401  →  January 2014 partition
--   intDiv(140201, 100) = 1402  →  February 2014 partition
--   Queries filtered by date skip irrelevant monthly partitions entirely.
--
-- ORDER BY (date_key, product_sk, store_sk):
--   ClickHouse builds a sparse primary index on this tuple.
--   Most OLAP queries filter or GROUP BY date + product/store — perfect fit.
--
-- Nullable FK columns model "not applicable" cleanly:
--   promotion_sk = NULL  →  no promotion was active
--   customer_sk  = NULL  →  anonymous (non-loyalty-card) purchase
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS retail_dw.fact_sales
(
    date_key       UInt32           COMMENT 'FK → dim_date',
    product_sk     UInt32           COMMENT 'FK → dim_product',
    store_sk       UInt32           COMMENT 'FK → dim_store',
    promotion_sk   Nullable(UInt32) COMMENT 'FK → dim_promotion (NULL = no active promo)',
    customer_sk    Nullable(UInt32) COMMENT 'FK → dim_customer (NULL = anonymous)',
    quantity       UInt32           COMMENT 'Number of units sold',
    net_price      Float64          COMMENT 'Full list price per unit',
    discount_price Float64          COMMENT 'Actual price paid per unit (≤ net_price)'
)
ENGINE = MergeTree()
PARTITION BY intDiv(date_key, 100)
ORDER BY (date_key, product_sk, store_sk)
COMMENT 'Sales fact table — central table of the star schema';
