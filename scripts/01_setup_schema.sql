-- =============================================================================
-- Dynamic Pricing Data Model — Schema Setup
-- Purpose: Creates the database, schema, and all tables for a GNN-based
--          dynamic pricing system on Snowflake.
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

CREATE OR REPLACE TABLE DIM_CALENDAR (
    date          DATE          PRIMARY KEY,
    day_of_week   VARCHAR(10)   NOT NULL,
    week          INT           NOT NULL,
    month         INT           NOT NULL,
    holiday_flag  BOOLEAN       NOT NULL DEFAULT FALSE,
    season        VARCHAR(20)   NOT NULL
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
    FOREIGN KEY (store_id)   REFERENCES DIM_STORE(store_id),
    FOREIGN KEY (date)       REFERENCES DIM_CALENDAR(date)
)
CLUSTER BY (date, product_id);

CREATE OR REPLACE TABLE FACT_COMPETITOR_PRICE_DAILY (
    competitor_id    INT           NOT NULL,
    product_id       INT           NOT NULL,
    store_id         INT           NOT NULL,
    date             DATE          NOT NULL,
    competitor_price NUMBER(10,2)  NOT NULL,

    PRIMARY KEY (competitor_id, product_id, store_id, date),

    FOREIGN KEY (product_id) REFERENCES DIM_PRODUCT(product_id),
    FOREIGN KEY (store_id)   REFERENCES DIM_STORE(store_id),
    FOREIGN KEY (date)       REFERENCES DIM_CALENDAR(date)
);

CREATE OR REPLACE TABLE FACT_EVENTS (
    event_id            INT           PRIMARY KEY,
    store_id            INT           NOT NULL,
    date                DATE          NOT NULL,
    event_type          VARCHAR(100)  NOT NULL,
    event_intensity     NUMBER(3,2)   NOT NULL,
    event_duration_days INT           NOT NULL DEFAULT 1,

    FOREIGN KEY (store_id) REFERENCES DIM_STORE(store_id),
    FOREIGN KEY (date)     REFERENCES DIM_CALENDAR(date)
);

-- ─── Graph Edge Tables ───────────────────────────────────────────────────────

CREATE OR REPLACE TABLE GRAPH_PRODUCT_PRODUCT_EDGE (
    src_product_id  INT          NOT NULL,
    dst_product_id  INT          NOT NULL,
    edge_type       VARCHAR(50)  NOT NULL,
    weight          NUMBER(5,4)  NOT NULL,
    snapshot_date   DATE         NOT NULL,

    PRIMARY KEY (src_product_id, dst_product_id, edge_type, snapshot_date),

    FOREIGN KEY (src_product_id) REFERENCES DIM_PRODUCT(product_id),
    FOREIGN KEY (dst_product_id) REFERENCES DIM_PRODUCT(product_id)
);

CREATE OR REPLACE TABLE GRAPH_STORE_STORE_EDGE (
    src_store_id   INT          NOT NULL,
    dst_store_id   INT          NOT NULL,
    weight         NUMBER(5,4)  NOT NULL,
    snapshot_date  DATE         NOT NULL,

    PRIMARY KEY (src_store_id, dst_store_id, snapshot_date),

    FOREIGN KEY (src_store_id) REFERENCES DIM_STORE(store_id),
    FOREIGN KEY (dst_store_id) REFERENCES DIM_STORE(store_id)
);

CREATE OR REPLACE TABLE GRAPH_PRODUCT_STORE_EDGE (
    product_id     INT          NOT NULL,
    store_id       INT          NOT NULL,
    weight         NUMBER(5,4)  NOT NULL,
    snapshot_date  DATE         NOT NULL,

    PRIMARY KEY (product_id, store_id, snapshot_date),

    FOREIGN KEY (product_id) REFERENCES DIM_PRODUCT(product_id),
    FOREIGN KEY (store_id)   REFERENCES DIM_STORE(store_id)
);
