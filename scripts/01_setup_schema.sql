-- =============================================================================
-- Dynamic Pricing Data Model — Schema Setup
-- Purpose: Creates the database, schema, and all tables for a Two-Tower
--          dynamic pricing system on Snowflake.
-- Tables:  DIM_PRODUCT (200 products), DIM_STORE (50 stores),
--          FACT_SALES_DAILY (~2.5M daily sales records)
-- Usage:   Run this script end-to-end in a Snowflake worksheet or via SnowSQL.
-- =============================================================================

-- ─── Database & Schema ───────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS DYNAMIC_PRICING;
CREATE SCHEMA IF NOT EXISTS DYNAMIC_PRICING.PRICING_MODEL;
USE SCHEMA DYNAMIC_PRICING.PRICING_MODEL;

-- ─── Dimension Tables ────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE DIM_PRODUCT (
    product_id   INT            PRIMARY KEY,
    category     VARCHAR(100)   NOT NULL,
    subcategory  VARCHAR(100),
    brand        VARCHAR(100)   NOT NULL,
    cost         NUMBER(10,2)   NOT NULL,
    base_price   NUMBER(10,2)   NOT NULL,
    size         VARCHAR(50),
    weight       NUMBER(10,2),
    launch_date  DATE
);

CREATE OR REPLACE TABLE DIM_STORE (
    store_id    INT            PRIMARY KEY,
    region      VARCHAR(100)   NOT NULL,
    city        VARCHAR(100)   NOT NULL,
    store_type  VARCHAR(50)    NOT NULL,
    lat         NUMBER(9,6)    NOT NULL,
    lon         NUMBER(9,6)    NOT NULL
);

-- ─── Fact Tables ─────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE FACT_SALES_DAILY (
    product_id             INT           NOT NULL,
    store_id               INT           NOT NULL,
    date                   DATE          NOT NULL,
    selling_price          NUMBER(10,2)  NOT NULL,
    cost_at_sale           NUMBER(10,2)  NOT NULL,
    units_sold             INT           NOT NULL,
    revenue                NUMBER(12,2)  NOT NULL,
    inventory_start_of_day INT           NOT NULL,
    returns_units          INT           DEFAULT 0,

    PRIMARY KEY (product_id, store_id, date),

    FOREIGN KEY (product_id) REFERENCES DIM_PRODUCT(product_id),
    FOREIGN KEY (store_id)   REFERENCES DIM_STORE(store_id)
)
CLUSTER BY (date, product_id);
