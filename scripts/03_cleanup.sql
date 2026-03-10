-- =============================================================================
-- Dynamic Pricing Lab — Cleanup
-- Purpose: Drops all resources created during the Two-Tower and XGBoost GPU
--          scaling demos so the account is returned to its pre-lab state.
-- Usage:   Run this script end-to-end in a Snowflake worksheet or via SnowSQL.
--          Uncomment the final DROP DATABASE if you want a full teardown.
-- =============================================================================

USE SCHEMA DYNAMIC_PRICING.PRICING_MODEL;

-- ─── Services (SPCS) ──────────────────────────────────────────────────────────
DROP SERVICE IF EXISTS DYNAMIC_PRICING.PRICING_MODEL.TWO_TOWER_INFERENCE;

-- ─── Model Monitors ───────────────────────────────────────────────────────────
DROP MODEL MONITOR IF EXISTS DYNAMIC_PRICING.PRICING_MODEL.PRICING_MONITOR;

-- ─── Registered Models ────────────────────────────────────────────────────────
DROP MODEL IF EXISTS DYNAMIC_PRICING.PRICING_MODEL.TWO_TOWER_PRICING;
DROP MODEL IF EXISTS DYNAMIC_PRICING.PRICING_MODEL.XGBOOST_PRICING;

-- ─── Monitoring & Inference Tables ────────────────────────────────────────────
DROP TABLE IF EXISTS DYNAMIC_PRICING.PRICING_MODEL.INFERENCE_LOG;
DROP TABLE IF EXISTS DYNAMIC_PRICING.PRICING_MODEL.MONITORING_BASELINE;

-- ─── Feature Store ────────────────────────────────────────────────────────────
DROP SCHEMA IF EXISTS DYNAMIC_PRICING.FS_SCHEMA CASCADE;

-- ─── Feature Store Datasets ───────────────────────────────────────────────────
DROP DATASET IF EXISTS DYNAMIC_PRICING.PRICING_MODEL.TT_TRAINING_DATA;
DROP DATASET IF EXISTS DYNAMIC_PRICING.PRICING_MODEL.XGBOOST_TRAINING_DATA;

-- ─── Fact & Dimension Tables ──────────────────────────────────────────────────
DROP TABLE IF EXISTS DYNAMIC_PRICING.PRICING_MODEL.FACT_SALES_DAILY;
DROP TABLE IF EXISTS DYNAMIC_PRICING.PRICING_MODEL.DIM_STORE;
DROP TABLE IF EXISTS DYNAMIC_PRICING.PRICING_MODEL.DIM_PRODUCT;

-- ─── Schema ───────────────────────────────────────────────────────────────────
DROP SCHEMA IF EXISTS DYNAMIC_PRICING.PRICING_MODEL;

-- ─── Database (uncomment to fully remove everything) ──────────────────────────
DROP DATABASE IF EXISTS DYNAMIC_PRICING;
