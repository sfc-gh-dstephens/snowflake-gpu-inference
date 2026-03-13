# Snowflake Distributed GPU Training & Inference

Data model and infrastructure setup for a Two-Tower dynamic pricing system running GPU inference on Snowflake.

## Overview

Synthetic retail dataset (200 products, 50 stores, ~2.5M daily sales records) across five categories: Electronics, Apparel, Home Goods, Food & Beverage, and Personal Care. Designed for training and serving Two-Tower and XGBoost pricing models via Snowpark Container Services (SPCS) with GPU compute.

## Scripts

| File | Description |
|------|-------------|
| `scripts/01_setup_schema.sql` | Creates the `DYNAMIC_PRICING` database, schema, dimension tables (`DIM_PRODUCT`, `DIM_STORE`), fact table (`FACT_SALES_DAILY`), and a GPU compute pool (`PRICING_GPU_POOL`). |
| `scripts/02_populate_data.sql` | Populates all tables with synthetic data: 200 products, 50 US stores, and ~2.5M fact rows generated from a 365-day date spine with randomized pricing, inventory, and sales signals. |
| `scripts/03_cleanup.sql` | Tears down all resources (services, model monitors, registered models, feature store, tables, schema, database) to restore the account to its pre-lab state. |

## Notebooks

| File | Description |
|------|-------------|
| `notebooks/pytorch_online_inference.ipynb` | End-to-end PyTorch Two-Tower model workflow: sets up a Feature Store with online-enabled feature views, trains a Two-Tower pricing model using `PyTorchDistributor` across multiple GPUs, registers it as a CustomModel, deploys to SPCS with a public HTTPS inference endpoint, simulates 30 days of inference traffic (with intentional drift), and configures a Model Monitor for drift and performance tracking. |
| `notebooks/xgboost_gpu_scaling.ipynb` | GPU-accelerated XGBoost pricing model: retrieves features from the Feature Store, trains with `XGBEstimator` using automatic GPU scaling, evaluates with RMSE/MAE/R² metrics, generates SHAP explanations, and registers the model with built-in Shapley explainability for warehouse and SPCS inference. |