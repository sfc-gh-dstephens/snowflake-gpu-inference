-- =============================================================================
-- Dynamic Pricing Data Model — Data Population
-- Purpose: Populates all tables with realistic synthetic data for a GNN-based
--          dynamic pricing system.
-- Scale:   ~200 products, 50 stores, 365 days, ~2.5M+ fact rows
-- Domain:  General retail (electronics, apparel, home goods, food, personal care)
-- Usage:   Run after 01_setup_schema.sql on the same Snowflake account.
-- =============================================================================

USE SCHEMA DYNAMIC_PRICING.PRICING_MODEL;

-- ─── DIM_CALENDAR (365 rows: 2025-01-01 to 2025-12-31) ──────────────────────

INSERT INTO DIM_CALENDAR (date, day_of_week, week, month, holiday_flag, season)
SELECT
    dt,
    DAYNAME(dt),
    WEEKOFYEAR(dt),
    MONTH(dt),
    CASE
        WHEN dt IN (
            '2025-01-01',  -- New Year's Day
            '2025-01-20',  -- MLK Day
            '2025-02-17',  -- Presidents' Day
            '2025-05-26',  -- Memorial Day
            '2025-07-04',  -- Independence Day
            '2025-09-01',  -- Labor Day
            '2025-10-13',  -- Columbus Day
            '2025-11-11',  -- Veterans Day
            '2025-11-27',  -- Thanksgiving
            '2025-12-25'   -- Christmas
        ) THEN TRUE
        ELSE FALSE
    END AS holiday_flag,
    CASE
        WHEN MONTH(dt) IN (3, 4, 5)   THEN 'Spring'
        WHEN MONTH(dt) IN (6, 7, 8)   THEN 'Summer'
        WHEN MONTH(dt) IN (9, 10, 11) THEN 'Fall'
        ELSE 'Winter'
    END AS season
FROM (
    SELECT DATEADD(day, ROW_NUMBER() OVER (ORDER BY 1) - 1, '2025-01-01'::DATE) AS dt
    FROM TABLE(GENERATOR(ROWCOUNT => 365))
);

-- ─── DIM_PRODUCT (200 rows) ──────────────────────────────────────────────────

INSERT INTO DIM_PRODUCT (product_id, category, subcategory, brand, cost, base_price, size, weight, launch_date)
WITH product_defs AS (
    -- Electronics (products 1-40)
    SELECT 1  AS product_id, 'Electronics' AS category, 'Laptops'      AS subcategory, 'TechPro'     AS brand, 450.00 AS cost, 699.99 AS base_price, '15 inch'  AS size, 4.50  AS weight, '2024-03-15'::DATE AS launch_date UNION ALL
    SELECT 2,  'Electronics', 'Laptops',      'TechPro',     520.00, 799.99, '14 inch',  3.80, '2024-06-01' UNION ALL
    SELECT 3,  'Electronics', 'Laptops',      'NovaByte',    380.00, 599.99, '13 inch',  3.20, '2024-01-10' UNION ALL
    SELECT 4,  'Electronics', 'Laptops',      'NovaByte',    620.00, 999.99, '16 inch',  5.10, '2024-09-20' UNION ALL
    SELECT 5,  'Electronics', 'Laptops',      'ZenithPC',    490.00, 749.99, '15 inch',  4.20, '2024-04-05' UNION ALL
    SELECT 6,  'Electronics', 'Laptops',      'ZenithPC',    350.00, 549.99, '14 inch',  3.50, '2023-11-15' UNION ALL
    SELECT 7,  'Electronics', 'Laptops',      'CoreLogic',   700.00, 1099.99,'17 inch',  5.80, '2024-07-01' UNION ALL
    SELECT 8,  'Electronics', 'Laptops',      'CoreLogic',   420.00, 649.99, '13 inch',  3.10, '2024-02-28' UNION ALL
    SELECT 9,  'Electronics', 'Phones',       'TechPro',     340.00, 599.99, '6.1 inch', 0.38, '2024-09-15' UNION ALL
    SELECT 10, 'Electronics', 'Phones',       'TechPro',     480.00, 849.99, '6.7 inch', 0.42, '2024-09-15' UNION ALL
    SELECT 11, 'Electronics', 'Phones',       'NovaByte',    280.00, 499.99, '6.4 inch', 0.40, '2024-08-01' UNION ALL
    SELECT 12, 'Electronics', 'Phones',       'NovaByte',    200.00, 349.99, '6.1 inch', 0.36, '2024-03-20' UNION ALL
    SELECT 13, 'Electronics', 'Phones',       'ZenithPC',    360.00, 649.99, '6.5 inch', 0.41, '2024-10-01' UNION ALL
    SELECT 14, 'Electronics', 'Phones',       'ZenithPC',    150.00, 249.99, '5.8 inch', 0.34, '2023-12-01' UNION ALL
    SELECT 15, 'Electronics', 'Phones',       'CoreLogic',   420.00, 749.99, '6.7 inch', 0.44, '2024-11-01' UNION ALL
    SELECT 16, 'Electronics', 'Phones',       'CoreLogic',   260.00, 449.99, '6.2 inch', 0.38, '2024-05-15' UNION ALL
    SELECT 17, 'Electronics', 'Headphones',   'TechPro',     80.00,  149.99, NULL,        0.25, '2024-01-15' UNION ALL
    SELECT 18, 'Electronics', 'Headphones',   'NovaByte',    45.00,  79.99,  NULL,        0.22, '2024-04-01' UNION ALL
    SELECT 19, 'Electronics', 'Headphones',   'SoundWave',   120.00, 229.99, NULL,        0.28, '2024-06-15' UNION ALL
    SELECT 20, 'Electronics', 'Headphones',   'SoundWave',   65.00,  119.99, NULL,        0.20, '2024-02-01' UNION ALL
    SELECT 21, 'Electronics', 'Tablets',      'TechPro',     300.00, 499.99, '11 inch',  1.05, '2024-05-01' UNION ALL
    SELECT 22, 'Electronics', 'Tablets',      'NovaByte',    220.00, 379.99, '10 inch',  0.95, '2024-07-15' UNION ALL
    SELECT 23, 'Electronics', 'Tablets',      'ZenithPC',    350.00, 599.99, '12.4 inch',1.20, '2024-08-20' UNION ALL
    SELECT 24, 'Electronics', 'Tablets',      'CoreLogic',   180.00, 299.99, '8.3 inch', 0.68, '2024-03-01' UNION ALL
    SELECT 25, 'Electronics', 'Accessories',  'TechPro',     12.00,  24.99,  NULL,        0.10, '2024-01-01' UNION ALL
    SELECT 26, 'Electronics', 'Accessories',  'TechPro',     18.00,  34.99,  NULL,        0.15, '2024-01-01' UNION ALL
    SELECT 27, 'Electronics', 'Accessories',  'NovaByte',    8.00,   14.99,  NULL,        0.08, '2024-02-15' UNION ALL
    SELECT 28, 'Electronics', 'Accessories',  'NovaByte',    22.00,  44.99,  NULL,        0.20, '2024-06-01' UNION ALL
    SELECT 29, 'Electronics', 'Accessories',  'SoundWave',   15.00,  29.99,  NULL,        0.12, '2024-04-01' UNION ALL
    SELECT 30, 'Electronics', 'Accessories',  'SoundWave',   35.00,  69.99,  NULL,        0.30, '2024-07-01' UNION ALL
    SELECT 31, 'Electronics', 'Accessories',  'CoreLogic',   10.00,  19.99,  NULL,        0.09, '2024-03-01' UNION ALL
    SELECT 32, 'Electronics', 'Accessories',  'CoreLogic',   25.00,  49.99,  NULL,        0.18, '2024-05-01' UNION ALL
    SELECT 33, 'Electronics', 'Laptops',      'TechPro',     580.00, 899.99, '15 inch',  4.30, '2024-11-01' UNION ALL
    SELECT 34, 'Electronics', 'Phones',       'SoundWave',   190.00, 329.99, '6.0 inch', 0.35, '2024-06-01' UNION ALL
    SELECT 35, 'Electronics', 'Headphones',   'CoreLogic',   55.00,  99.99,  NULL,        0.21, '2024-08-01' UNION ALL
    SELECT 36, 'Electronics', 'Tablets',      'SoundWave',   250.00, 429.99, '10.5 inch',0.98, '2024-09-01' UNION ALL
    SELECT 37, 'Electronics', 'Accessories',  'ZenithPC',    14.00,  27.99,  NULL,        0.11, '2024-02-01' UNION ALL
    SELECT 38, 'Electronics', 'Accessories',  'ZenithPC',    30.00,  59.99,  NULL,        0.25, '2024-08-15' UNION ALL
    SELECT 39, 'Electronics', 'Phones',       'SoundWave',   310.00, 549.99, '6.3 inch', 0.39, '2024-10-15' UNION ALL
    SELECT 40, 'Electronics', 'Headphones',   'ZenithPC',    95.00,  179.99, NULL,        0.26, '2024-05-01' UNION ALL

    -- Apparel (products 41-80)
    SELECT 41, 'Apparel', 'T-Shirts',    'UrbanThread', 5.00,  19.99,  'M',   0.20, '2024-02-01' UNION ALL
    SELECT 42, 'Apparel', 'T-Shirts',    'UrbanThread', 6.00,  24.99,  'L',   0.22, '2024-02-01' UNION ALL
    SELECT 43, 'Apparel', 'T-Shirts',    'PeakWear',    4.50,  17.99,  'S',   0.18, '2024-03-15' UNION ALL
    SELECT 44, 'Apparel', 'T-Shirts',    'PeakWear',    7.00,  29.99,  'XL',  0.24, '2024-04-01' UNION ALL
    SELECT 45, 'Apparel', 'T-Shirts',    'CoastLine',   5.50,  22.99,  'M',   0.20, '2024-01-15' UNION ALL
    SELECT 46, 'Apparel', 'T-Shirts',    'CoastLine',   8.00,  34.99,  'L',   0.23, '2024-06-01' UNION ALL
    SELECT 47, 'Apparel', 'T-Shirts',    'NorthEdge',   6.50,  27.99,  'M',   0.21, '2024-05-01' UNION ALL
    SELECT 48, 'Apparel', 'T-Shirts',    'NorthEdge',   4.00,  14.99,  'S',   0.17, '2024-01-01' UNION ALL
    SELECT 49, 'Apparel', 'Jeans',       'UrbanThread', 15.00, 49.99,  '32',  0.80, '2024-01-15' UNION ALL
    SELECT 50, 'Apparel', 'Jeans',       'UrbanThread', 18.00, 59.99,  '34',  0.85, '2024-03-01' UNION ALL
    SELECT 51, 'Apparel', 'Jeans',       'PeakWear',    12.00, 39.99,  '30',  0.75, '2024-02-01' UNION ALL
    SELECT 52, 'Apparel', 'Jeans',       'PeakWear',    20.00, 69.99,  '36',  0.90, '2024-05-15' UNION ALL
    SELECT 53, 'Apparel', 'Jeans',       'CoastLine',   16.00, 54.99,  '32',  0.82, '2024-04-01' UNION ALL
    SELECT 54, 'Apparel', 'Jeans',       'NorthEdge',   22.00, 79.99,  '34',  0.88, '2024-06-01' UNION ALL
    SELECT 55, 'Apparel', 'Jeans',       'NorthEdge',   14.00, 44.99,  '30',  0.78, '2024-01-01' UNION ALL
    SELECT 56, 'Apparel', 'Jeans',       'CoastLine',   25.00, 89.99,  '36',  0.92, '2024-08-01' UNION ALL
    SELECT 57, 'Apparel', 'Jackets',     'UrbanThread', 30.00, 89.99,  'M',   1.20, '2024-09-01' UNION ALL
    SELECT 58, 'Apparel', 'Jackets',     'PeakWear',    45.00, 129.99, 'L',   1.40, '2024-09-15' UNION ALL
    SELECT 59, 'Apparel', 'Jackets',     'CoastLine',   35.00, 99.99,  'M',   1.25, '2024-10-01' UNION ALL
    SELECT 60, 'Apparel', 'Jackets',     'NorthEdge',   55.00, 159.99, 'XL',  1.60, '2024-08-15' UNION ALL
    SELECT 61, 'Apparel', 'Jackets',     'UrbanThread', 40.00, 119.99, 'L',   1.35, '2024-10-15' UNION ALL
    SELECT 62, 'Apparel', 'Jackets',     'PeakWear',    25.00, 74.99,  'S',   1.10, '2024-07-01' UNION ALL
    SELECT 63, 'Apparel', 'Shoes',       'UrbanThread', 25.00, 69.99,  '10',  0.70, '2024-03-01' UNION ALL
    SELECT 64, 'Apparel', 'Shoes',       'PeakWear',    35.00, 99.99,  '11',  0.85, '2024-04-15' UNION ALL
    SELECT 65, 'Apparel', 'Shoes',       'CoastLine',   20.00, 54.99,  '9',   0.65, '2024-02-15' UNION ALL
    SELECT 66, 'Apparel', 'Shoes',       'NorthEdge',   45.00, 129.99, '10',  0.90, '2024-06-01' UNION ALL
    SELECT 67, 'Apparel', 'Shoes',       'UrbanThread', 30.00, 84.99,  '11',  0.75, '2024-07-15' UNION ALL
    SELECT 68, 'Apparel', 'Shoes',       'PeakWear',    18.00, 49.99,  '8',   0.60, '2024-01-01' UNION ALL
    SELECT 69, 'Apparel', 'Activewear',  'PeakWear',    10.00, 34.99,  'M',   0.30, '2024-01-15' UNION ALL
    SELECT 70, 'Apparel', 'Activewear',  'PeakWear',    12.00, 44.99,  'L',   0.35, '2024-03-01' UNION ALL
    SELECT 71, 'Apparel', 'Activewear',  'NorthEdge',   14.00, 49.99,  'M',   0.32, '2024-04-01' UNION ALL
    SELECT 72, 'Apparel', 'Activewear',  'NorthEdge',   8.00,  29.99,  'S',   0.28, '2024-02-01' UNION ALL
    SELECT 73, 'Apparel', 'Activewear',  'CoastLine',   11.00, 39.99,  'L',   0.33, '2024-05-15' UNION ALL
    SELECT 74, 'Apparel', 'Activewear',  'UrbanThread', 9.00,  32.99,  'M',   0.30, '2024-06-01' UNION ALL
    SELECT 75, 'Apparel', 'T-Shirts',    'UrbanThread', 9.00,  39.99,  'M',   0.21, '2024-07-01' UNION ALL
    SELECT 76, 'Apparel', 'Jeans',       'UrbanThread', 28.00, 99.99,  '34',  0.95, '2024-09-01' UNION ALL
    SELECT 77, 'Apparel', 'Jackets',     'CoastLine',   50.00, 149.99, 'L',   1.50, '2024-11-01' UNION ALL
    SELECT 78, 'Apparel', 'Shoes',       'CoastLine',   40.00, 109.99, '10',  0.80, '2024-08-01' UNION ALL
    SELECT 79, 'Apparel', 'Activewear',  'PeakWear',    15.00, 54.99,  'XL',  0.38, '2024-07-15' UNION ALL
    SELECT 80, 'Apparel', 'Activewear',  'NorthEdge',   13.00, 47.99,  'L',   0.34, '2024-09-01' UNION ALL

    -- Home Goods (products 81-120)
    SELECT 81,  'Home Goods', 'Furniture',    'HomeNest',    120.00, 249.99, '3-seat',    35.00, '2024-01-15' UNION ALL
    SELECT 82,  'Home Goods', 'Furniture',    'HomeNest',    80.00,  179.99, '2-seat',    25.00, '2024-03-01' UNION ALL
    SELECT 83,  'Home Goods', 'Furniture',    'CraftWood',   150.00, 329.99, 'Queen',     40.00, '2024-04-01' UNION ALL
    SELECT 84,  'Home Goods', 'Furniture',    'CraftWood',   60.00,  139.99, 'Side table',8.00,  '2024-02-15' UNION ALL
    SELECT 85,  'Home Goods', 'Furniture',    'ModernLoft',  200.00, 449.99, 'King',      50.00, '2024-06-01' UNION ALL
    SELECT 86,  'Home Goods', 'Furniture',    'ModernLoft',  90.00,  199.99, 'Desk',      20.00, '2024-05-01' UNION ALL
    SELECT 87,  'Home Goods', 'Furniture',    'RusticHome',  110.00, 239.99, 'Bookshelf', 30.00, '2024-07-01' UNION ALL
    SELECT 88,  'Home Goods', 'Furniture',    'RusticHome',  70.00,  159.99, 'Console',   18.00, '2024-08-15' UNION ALL
    SELECT 89,  'Home Goods', 'Kitchen',      'HomeNest',    25.00,  59.99,  '12-piece',  3.50,  '2024-01-01' UNION ALL
    SELECT 90,  'Home Goods', 'Kitchen',      'HomeNest',    40.00,  89.99,  '16-piece',  5.00,  '2024-03-15' UNION ALL
    SELECT 91,  'Home Goods', 'Kitchen',      'CraftWood',   15.00,  34.99,  '6-piece',   2.00,  '2024-02-01' UNION ALL
    SELECT 92,  'Home Goods', 'Kitchen',      'CraftWood',   55.00,  119.99, '20-piece',  7.00,  '2024-05-01' UNION ALL
    SELECT 93,  'Home Goods', 'Kitchen',      'ModernLoft',  30.00,  69.99,  '10-piece',  4.00,  '2024-04-15' UNION ALL
    SELECT 94,  'Home Goods', 'Kitchen',      'RusticHome',  20.00,  49.99,  '8-piece',   3.00,  '2024-06-01' UNION ALL
    SELECT 95,  'Home Goods', 'Bedding',      'HomeNest',    35.00,  79.99,  'Queen',     2.50,  '2024-01-15' UNION ALL
    SELECT 96,  'Home Goods', 'Bedding',      'HomeNest',    45.00,  99.99,  'King',      3.00,  '2024-02-01' UNION ALL
    SELECT 97,  'Home Goods', 'Bedding',      'CraftWood',   28.00,  64.99,  'Full',      2.20,  '2024-03-01' UNION ALL
    SELECT 98,  'Home Goods', 'Bedding',      'ModernLoft',  50.00,  109.99, 'King',      3.20,  '2024-04-01' UNION ALL
    SELECT 99,  'Home Goods', 'Bedding',      'RusticHome',  30.00,  69.99,  'Queen',     2.40,  '2024-05-15' UNION ALL
    SELECT 100, 'Home Goods', 'Bedding',      'RusticHome',  22.00,  49.99,  'Twin',      1.80,  '2024-06-01' UNION ALL
    SELECT 101, 'Home Goods', 'Lighting',     'HomeNest',    18.00,  44.99,  NULL,        1.50,  '2024-01-01' UNION ALL
    SELECT 102, 'Home Goods', 'Lighting',     'ModernLoft',  35.00,  79.99,  NULL,        2.00,  '2024-02-15' UNION ALL
    SELECT 103, 'Home Goods', 'Lighting',     'CraftWood',   22.00,  54.99,  NULL,        1.80,  '2024-03-01' UNION ALL
    SELECT 104, 'Home Goods', 'Lighting',     'RusticHome',  28.00,  64.99,  NULL,        1.60,  '2024-04-15' UNION ALL
    SELECT 105, 'Home Goods', 'Decor',        'HomeNest',    8.00,   19.99,  NULL,        0.50,  '2024-01-01' UNION ALL
    SELECT 106, 'Home Goods', 'Decor',        'ModernLoft',  15.00,  34.99,  NULL,        0.80,  '2024-02-01' UNION ALL
    SELECT 107, 'Home Goods', 'Decor',        'CraftWood',   12.00,  29.99,  NULL,        0.65,  '2024-03-15' UNION ALL
    SELECT 108, 'Home Goods', 'Decor',        'RusticHome',  10.00,  24.99,  NULL,        0.55,  '2024-04-01' UNION ALL
    SELECT 109, 'Home Goods', 'Furniture',    'HomeNest',    95.00,  209.99, 'Armchair',  15.00, '2024-09-01' UNION ALL
    SELECT 110, 'Home Goods', 'Kitchen',      'ModernLoft',  45.00,  99.99,  '14-piece',  6.00,  '2024-08-01' UNION ALL
    SELECT 111, 'Home Goods', 'Bedding',      'ModernLoft',  40.00,  89.99,  'Queen',     2.60,  '2024-07-01' UNION ALL
    SELECT 112, 'Home Goods', 'Lighting',     'HomeNest',    42.00,  94.99,  NULL,        2.50,  '2024-09-15' UNION ALL
    SELECT 113, 'Home Goods', 'Decor',        'CraftWood',   18.00,  42.99,  NULL,        0.70,  '2024-10-01' UNION ALL
    SELECT 114, 'Home Goods', 'Furniture',    'CraftWood',   130.00, 289.99, 'Dining',    28.00, '2024-10-15' UNION ALL
    SELECT 115, 'Home Goods', 'Kitchen',      'RusticHome',  35.00,  79.99,  '12-piece',  4.50,  '2024-11-01' UNION ALL
    SELECT 116, 'Home Goods', 'Decor',        'HomeNest',    20.00,  47.99,  NULL,        0.90,  '2024-08-15' UNION ALL
    SELECT 117, 'Home Goods', 'Lighting',     'RusticHome',  15.00,  37.99,  NULL,        1.20,  '2024-07-15' UNION ALL
    SELECT 118, 'Home Goods', 'Bedding',      'CraftWood',   55.00,  119.99, 'King',      3.50,  '2024-11-15' UNION ALL
    SELECT 119, 'Home Goods', 'Furniture',    'ModernLoft',  160.00, 349.99, 'Sectional', 45.00, '2024-12-01' UNION ALL
    SELECT 120, 'Home Goods', 'Kitchen',      'HomeNest',    60.00,  134.99, '24-piece',  8.50,  '2024-12-15' UNION ALL

    -- Food & Beverage (products 121-160)
    SELECT 121, 'Food & Beverage', 'Snacks',      'FreshField',  1.20,  3.49,  '8 oz',   0.50, '2024-01-01' UNION ALL
    SELECT 122, 'Food & Beverage', 'Snacks',      'FreshField',  1.80,  4.99,  '12 oz',  0.75, '2024-01-01' UNION ALL
    SELECT 123, 'Food & Beverage', 'Snacks',      'GreenBasket', 0.90,  2.49,  '6 oz',   0.38, '2024-02-01' UNION ALL
    SELECT 124, 'Food & Beverage', 'Snacks',      'GreenBasket', 2.20,  5.99,  '16 oz',  1.00, '2024-03-01' UNION ALL
    SELECT 125, 'Food & Beverage', 'Snacks',      'NutriPrime',  1.50,  3.99,  '10 oz',  0.63, '2024-01-15' UNION ALL
    SELECT 126, 'Food & Beverage', 'Snacks',      'NutriPrime',  2.50,  6.99,  '20 oz',  1.25, '2024-04-01' UNION ALL
    SELECT 127, 'Food & Beverage', 'Snacks',      'HarvestGold', 1.00,  2.99,  '7 oz',   0.44, '2024-02-15' UNION ALL
    SELECT 128, 'Food & Beverage', 'Snacks',      'HarvestGold', 1.70,  4.49,  '11 oz',  0.69, '2024-05-01' UNION ALL
    SELECT 129, 'Food & Beverage', 'Beverages',   'FreshField',  0.80,  2.29,  '12 oz',  0.75, '2024-01-01' UNION ALL
    SELECT 130, 'Food & Beverage', 'Beverages',   'FreshField',  1.20,  3.49,  '20 oz',  1.25, '2024-01-01' UNION ALL
    SELECT 131, 'Food & Beverage', 'Beverages',   'GreenBasket', 0.60,  1.79,  '12 oz',  0.75, '2024-02-01' UNION ALL
    SELECT 132, 'Food & Beverage', 'Beverages',   'GreenBasket', 1.50,  4.29,  '1 L',    2.20, '2024-03-01' UNION ALL
    SELECT 133, 'Food & Beverage', 'Beverages',   'NutriPrime',  1.00,  2.99,  '16 oz',  1.00, '2024-01-15' UNION ALL
    SELECT 134, 'Food & Beverage', 'Beverages',   'HarvestGold', 0.70,  1.99,  '12 oz',  0.75, '2024-02-15' UNION ALL
    SELECT 135, 'Food & Beverage', 'Dairy',       'FreshField',  1.80,  4.49,  '1 gal',  8.60, '2024-01-01' UNION ALL
    SELECT 136, 'Food & Beverage', 'Dairy',       'GreenBasket', 2.20,  5.49,  '1 gal',  8.60, '2024-01-01' UNION ALL
    SELECT 137, 'Food & Beverage', 'Dairy',       'NutriPrime',  1.50,  3.99,  '0.5 gal',4.30, '2024-02-01' UNION ALL
    SELECT 138, 'Food & Beverage', 'Dairy',       'HarvestGold', 1.00,  2.79,  '1 qt',   2.15, '2024-01-15' UNION ALL
    SELECT 139, 'Food & Beverage', 'Dairy',       'FreshField',  2.50,  6.49,  '2 lb',   2.00, '2024-03-01' UNION ALL
    SELECT 140, 'Food & Beverage', 'Dairy',       'GreenBasket', 1.30,  3.49,  '1 lb',   1.00, '2024-04-01' UNION ALL
    SELECT 141, 'Food & Beverage', 'Bakery',      'FreshField',  1.00,  2.99,  '1 loaf', 1.50, '2024-01-01' UNION ALL
    SELECT 142, 'Food & Beverage', 'Bakery',      'GreenBasket', 1.50,  4.29,  '1 loaf', 1.50, '2024-01-01' UNION ALL
    SELECT 143, 'Food & Beverage', 'Bakery',      'HarvestGold', 0.80,  2.29,  '6 pack', 1.00, '2024-02-01' UNION ALL
    SELECT 144, 'Food & Beverage', 'Bakery',      'NutriPrime',  2.00,  5.49,  '12 pack',2.00, '2024-03-15' UNION ALL
    SELECT 145, 'Food & Beverage', 'Frozen',      'FreshField',  2.50,  6.99,  '16 oz',  1.00, '2024-01-01' UNION ALL
    SELECT 146, 'Food & Beverage', 'Frozen',      'GreenBasket', 3.00,  7.99,  '24 oz',  1.50, '2024-02-01' UNION ALL
    SELECT 147, 'Food & Beverage', 'Frozen',      'NutriPrime',  2.00,  5.49,  '12 oz',  0.75, '2024-01-15' UNION ALL
    SELECT 148, 'Food & Beverage', 'Frozen',      'HarvestGold', 3.50,  8.99,  '32 oz',  2.00, '2024-03-01' UNION ALL
    SELECT 149, 'Food & Beverage', 'Snacks',      'FreshField',  3.00,  7.99,  '24 oz',  1.50, '2024-06-01' UNION ALL
    SELECT 150, 'Food & Beverage', 'Beverages',   'NutriPrime',  1.80,  4.99,  '6-pack', 4.50, '2024-05-01' UNION ALL
    SELECT 151, 'Food & Beverage', 'Dairy',       'NutriPrime',  2.80,  6.99,  '2 lb',   2.00, '2024-06-15' UNION ALL
    SELECT 152, 'Food & Beverage', 'Bakery',      'FreshField',  1.20,  3.49,  '8 pack', 1.20, '2024-07-01' UNION ALL
    SELECT 153, 'Food & Beverage', 'Frozen',      'FreshField',  4.00,  9.99,  '2 lb',   2.00, '2024-08-01' UNION ALL
    SELECT 154, 'Food & Beverage', 'Snacks',      'NutriPrime',  1.30,  3.69,  '9 oz',   0.56, '2024-07-15' UNION ALL
    SELECT 155, 'Food & Beverage', 'Beverages',   'HarvestGold', 2.00,  5.49,  '6-pack', 4.50, '2024-08-01' UNION ALL
    SELECT 156, 'Food & Beverage', 'Dairy',       'HarvestGold', 1.80,  4.49,  '1.5 lb', 1.50, '2024-09-01' UNION ALL
    SELECT 157, 'Food & Beverage', 'Bakery',      'GreenBasket', 2.50,  6.99,  '12 pack',2.50, '2024-10-01' UNION ALL
    SELECT 158, 'Food & Beverage', 'Frozen',      'GreenBasket', 3.50,  8.49,  '28 oz',  1.75, '2024-09-15' UNION ALL
    SELECT 159, 'Food & Beverage', 'Beverages',   'FreshField',  2.50,  6.99,  '12-pack', 9.00, '2024-10-15' UNION ALL
    SELECT 160, 'Food & Beverage', 'Snacks',      'GreenBasket', 2.80,  7.49,  '18 oz',  1.13, '2024-11-01' UNION ALL

    -- Personal Care (products 161-200)
    SELECT 161, 'Personal Care', 'Skincare',     'PureGlow',    5.00,  14.99,  '4 oz',   0.25, '2024-01-01' UNION ALL
    SELECT 162, 'Personal Care', 'Skincare',     'PureGlow',    8.00,  22.99,  '8 oz',   0.50, '2024-01-01' UNION ALL
    SELECT 163, 'Personal Care', 'Skincare',     'VitalEssence',6.00,  17.99,  '6 oz',   0.38, '2024-02-01' UNION ALL
    SELECT 164, 'Personal Care', 'Skincare',     'VitalEssence',10.00, 29.99,  '10 oz',  0.63, '2024-03-01' UNION ALL
    SELECT 165, 'Personal Care', 'Skincare',     'NaturaBio',   4.00,  11.99,  '3 oz',   0.19, '2024-01-15' UNION ALL
    SELECT 166, 'Personal Care', 'Skincare',     'NaturaBio',   7.00,  19.99,  '6 oz',   0.38, '2024-04-01' UNION ALL
    SELECT 167, 'Personal Care', 'Skincare',     'CleanSlate',  12.00, 34.99,  '12 oz',  0.75, '2024-02-15' UNION ALL
    SELECT 168, 'Personal Care', 'Skincare',     'CleanSlate',  3.50,  9.99,   '2 oz',   0.13, '2024-05-01' UNION ALL
    SELECT 169, 'Personal Care', 'Haircare',     'PureGlow',    4.00,  11.99,  '12 oz',  0.75, '2024-01-01' UNION ALL
    SELECT 170, 'Personal Care', 'Haircare',     'PureGlow',    6.00,  16.99,  '16 oz',  1.00, '2024-02-01' UNION ALL
    SELECT 171, 'Personal Care', 'Haircare',     'VitalEssence',3.50,  9.99,   '10 oz',  0.63, '2024-01-15' UNION ALL
    SELECT 172, 'Personal Care', 'Haircare',     'VitalEssence',5.00,  14.99,  '14 oz',  0.88, '2024-03-01' UNION ALL
    SELECT 173, 'Personal Care', 'Haircare',     'NaturaBio',   4.50,  12.99,  '12 oz',  0.75, '2024-02-15' UNION ALL
    SELECT 174, 'Personal Care', 'Haircare',     'CleanSlate',  7.00,  19.99,  '20 oz',  1.25, '2024-04-01' UNION ALL
    SELECT 175, 'Personal Care', 'Oral Care',    'PureGlow',    1.50,  4.49,   '6 oz',   0.38, '2024-01-01' UNION ALL
    SELECT 176, 'Personal Care', 'Oral Care',    'VitalEssence',2.00,  5.99,   '8 oz',   0.50, '2024-01-01' UNION ALL
    SELECT 177, 'Personal Care', 'Oral Care',    'NaturaBio',   1.20,  3.49,   '4 oz',   0.25, '2024-02-01' UNION ALL
    SELECT 178, 'Personal Care', 'Oral Care',    'CleanSlate',  2.50,  6.99,   '8 oz',   0.50, '2024-03-01' UNION ALL
    SELECT 179, 'Personal Care', 'Body Wash',    'PureGlow',    3.00,  8.99,   '16 oz',  1.00, '2024-01-01' UNION ALL
    SELECT 180, 'Personal Care', 'Body Wash',    'PureGlow',    4.50,  12.99,  '24 oz',  1.50, '2024-02-01' UNION ALL
    SELECT 181, 'Personal Care', 'Body Wash',    'VitalEssence',2.50,  7.49,   '12 oz',  0.75, '2024-01-15' UNION ALL
    SELECT 182, 'Personal Care', 'Body Wash',    'NaturaBio',   3.50,  9.99,   '18 oz',  1.13, '2024-03-01' UNION ALL
    SELECT 183, 'Personal Care', 'Body Wash',    'CleanSlate',  5.00,  14.99,  '32 oz',  2.00, '2024-04-15' UNION ALL
    SELECT 184, 'Personal Care', 'Body Wash',    'CleanSlate',  2.00,  5.99,   '8 oz',   0.50, '2024-02-01' UNION ALL
    SELECT 185, 'Personal Care', 'Deodorant',    'PureGlow',    2.00,  5.99,   '2.6 oz', 0.16, '2024-01-01' UNION ALL
    SELECT 186, 'Personal Care', 'Deodorant',    'VitalEssence',2.50,  7.49,   '3 oz',   0.19, '2024-02-01' UNION ALL
    SELECT 187, 'Personal Care', 'Deodorant',    'NaturaBio',   1.80,  4.99,   '2.6 oz', 0.16, '2024-01-15' UNION ALL
    SELECT 188, 'Personal Care', 'Deodorant',    'CleanSlate',  3.00,  8.99,   '3.4 oz', 0.21, '2024-03-01' UNION ALL
    SELECT 189, 'Personal Care', 'Skincare',     'PureGlow',    15.00, 42.99,  '16 oz',  1.00, '2024-06-01' UNION ALL
    SELECT 190, 'Personal Care', 'Haircare',     'NaturaBio',   8.00,  22.99,  '24 oz',  1.50, '2024-07-01' UNION ALL
    SELECT 191, 'Personal Care', 'Oral Care',    'PureGlow',    3.00,  8.49,   '3-pack', 0.75, '2024-08-01' UNION ALL
    SELECT 192, 'Personal Care', 'Body Wash',    'VitalEssence',6.00,  17.99,  '32 oz',  2.00, '2024-06-15' UNION ALL
    SELECT 193, 'Personal Care', 'Deodorant',    'VitalEssence',3.50,  9.99,   '4 oz',   0.25, '2024-07-15' UNION ALL
    SELECT 194, 'Personal Care', 'Skincare',     'CleanSlate',  9.00,  25.99,  '8 oz',   0.50, '2024-09-01' UNION ALL
    SELECT 195, 'Personal Care', 'Haircare',     'CleanSlate',  5.50,  15.99,  '16 oz',  1.00, '2024-10-01' UNION ALL
    SELECT 196, 'Personal Care', 'Oral Care',    'CleanSlate',  4.00,  11.99,  '6-pack', 1.50, '2024-08-15' UNION ALL
    SELECT 197, 'Personal Care', 'Body Wash',    'NaturaBio',   4.00,  11.49,  '20 oz',  1.25, '2024-09-15' UNION ALL
    SELECT 198, 'Personal Care', 'Deodorant',    'NaturaBio',   2.20,  6.49,   '3 oz',   0.19, '2024-10-15' UNION ALL
    SELECT 199, 'Personal Care', 'Skincare',     'VitalEssence',11.00, 31.99,  '10 oz',  0.63, '2024-11-01' UNION ALL
    SELECT 200, 'Personal Care', 'Haircare',     'PureGlow',    9.00,  24.99,  '20 oz',  1.25, '2024-11-15'
)
SELECT * FROM product_defs;

-- ─── DIM_STORE (50 rows) ────────────────────────────────────────────────────

INSERT INTO DIM_STORE (store_id, region, city, store_type, lat, lon)
VALUES
    -- Northeast (1-10)
    (1,  'Northeast', 'New York',      'retail',    40.712776, -74.005974),
    (2,  'Northeast', 'Boston',        'retail',    42.360081, -71.058884),
    (3,  'Northeast', 'Philadelphia',  'warehouse', 39.952583, -75.165222),
    (4,  'Northeast', 'Pittsburgh',    'retail',    40.440624, -79.995888),
    (5,  'Northeast', 'Hartford',      'outlet',    41.763710, -72.685097),
    (6,  'Northeast', 'Newark',        'retail',    40.735657, -74.172367),
    (7,  'Northeast', 'Providence',    'outlet',    41.824009, -71.412834),
    (8,  'Northeast', 'Buffalo',       'warehouse', 42.886448, -78.878372),
    (9,  'Northeast', 'Manhattan',     'retail',    40.776676, -73.971321),
    (10, 'Northeast', 'Brooklyn',      'online',    40.678177, -73.944160),

    -- Southeast (11-20)
    (11, 'Southeast', 'Atlanta',       'retail',    33.748992, -84.387985),
    (12, 'Southeast', 'Miami',         'retail',    25.761681, -80.191788),
    (13, 'Southeast', 'Charlotte',     'warehouse', 35.227085, -80.843124),
    (14, 'Southeast', 'Orlando',       'retail',    28.538335, -81.379234),
    (15, 'Southeast', 'Tampa',         'outlet',    27.950575, -82.457176),
    (16, 'Southeast', 'Nashville',     'retail',    36.162663, -86.781601),
    (17, 'Southeast', 'Raleigh',       'outlet',    35.779591, -78.638176),
    (18, 'Southeast', 'Jacksonville',  'warehouse', 30.332184, -81.655647),
    (19, 'Southeast', 'Richmond',      'retail',    37.540726, -77.436050),
    (20, 'Southeast', 'Charleston',    'online',    32.776474, -79.931053),

    -- Midwest (21-30)
    (21, 'Midwest', 'Chicago',        'retail',    41.878113, -87.629799),
    (22, 'Midwest', 'Detroit',        'retail',    42.331429, -83.045753),
    (23, 'Midwest', 'Minneapolis',    'warehouse', 44.977753, -93.265015),
    (24, 'Midwest', 'Columbus',       'retail',    39.961176, -82.998795),
    (25, 'Midwest', 'Indianapolis',   'outlet',    39.768402, -86.158066),
    (26, 'Midwest', 'Milwaukee',      'retail',    43.038902, -87.906471),
    (27, 'Midwest', 'Kansas City',    'outlet',    39.099728, -94.578568),
    (28, 'Midwest', 'St. Louis',      'warehouse', 38.627003, -90.199402),
    (29, 'Midwest', 'Cincinnati',     'retail',    39.103119, -84.512016),
    (30, 'Midwest', 'Cleveland',      'online',    41.499320, -81.694361),

    -- West (31-40)
    (31, 'West', 'Denver',          'retail',    39.739235, -104.990250),
    (32, 'West', 'Phoenix',         'retail',    33.448376, -112.074036),
    (33, 'West', 'Salt Lake City',  'warehouse', 40.760780, -111.891045),
    (34, 'West', 'Las Vegas',       'retail',    36.169941, -115.139832),
    (35, 'West', 'Albuquerque',     'outlet',    35.084385, -106.650421),
    (36, 'West', 'Tucson',          'retail',    32.222607, -110.974711),
    (37, 'West', 'El Paso',         'outlet',    31.761877, -106.485022),
    (38, 'West', 'Boise',           'warehouse', 43.615021, -116.202316),
    (39, 'West', 'Colorado Springs','retail',    38.833882, -104.821363),
    (40, 'West', 'Austin',          'online',    30.267153, -97.743057),

    -- Pacific (41-50)
    (41, 'Pacific', 'Los Angeles',    'retail',    34.052234, -118.243685),
    (42, 'Pacific', 'San Francisco',  'retail',    37.774929, -122.419418),
    (43, 'Pacific', 'Seattle',        'warehouse', 47.606209, -122.332069),
    (44, 'Pacific', 'Portland',       'retail',    45.505106, -122.675026),
    (45, 'Pacific', 'San Diego',      'outlet',    32.715736, -117.161087),
    (46, 'Pacific', 'Sacramento',     'retail',    38.581572, -121.494400),
    (47, 'Pacific', 'San Jose',       'outlet',    37.338207, -121.886330),
    (48, 'Pacific', 'Honolulu',       'warehouse', 21.306944, -157.858337),
    (49, 'Pacific', 'Anchorage',      'retail',    61.218056, -149.900278),
    (50, 'Pacific', 'Fresno',         'online',    36.737798, -119.787125);

-- ─── FACT_SALES_DAILY (~2.5M rows) ──────────────────────────────────────────
-- Generate sales for ~70% of product-store-date combos with realistic patterns.
-- Price varies ±15% around base_price; units follow category-dependent means;
-- inventory and returns are proportional.

INSERT INTO FACT_SALES_DAILY (
    product_id, store_id, date, selling_price, cost_at_sale,
    units_sold, revenue, inventory_start_of_day, returns_units
)
WITH all_combos AS (
    SELECT
        p.product_id,
        s.store_id,
        c.date,
        p.base_price,
        p.cost,
        p.category,
        c.season,
        c.holiday_flag,
        c.day_of_week,
        -- Random seed per combo for deterministic-ish generation
        ABS(HASH(p.product_id || '-' || s.store_id || '-' || c.date)) AS h
    FROM DIM_PRODUCT p
    CROSS JOIN DIM_STORE s
    CROSS JOIN DIM_CALENDAR c
),
filtered AS (
    -- Keep ~70% of combos (simulates not every product sold everywhere every day)
    SELECT *
    FROM all_combos
    WHERE MOD(h, 100) < 70
),
with_price AS (
    SELECT
        product_id,
        store_id,
        date,
        -- selling_price: base_price * (0.85 to 1.15) using hash for variation
        ROUND(base_price * (0.85 + (MOD(h, 3000) / 10000.0)), 2) AS selling_price,
        -- cost_at_sale: cost * (0.97 to 1.03)
        ROUND(cost * (0.97 + (MOD(h / 7, 600) / 10000.0)), 2) AS cost_at_sale,
        -- units_sold: category-dependent base + variation
        GREATEST(1, CASE
            WHEN category = 'Electronics'     THEN 2  + MOD(h / 13, 15)
            WHEN category = 'Apparel'         THEN 5  + MOD(h / 17, 25)
            WHEN category = 'Home Goods'      THEN 1  + MOD(h / 19, 10)
            WHEN category = 'Food & Beverage' THEN 15 + MOD(h / 23, 80)
            WHEN category = 'Personal Care'   THEN 8  + MOD(h / 29, 40)
            ELSE 3 + MOD(h / 11, 20)
        END
        -- Seasonal boost
        + CASE
            WHEN season = 'Winter' AND category IN ('Apparel', 'Home Goods') THEN MOD(h / 31, 10)
            WHEN season = 'Summer' AND category IN ('Food & Beverage', 'Personal Care') THEN MOD(h / 37, 12)
            ELSE 0
        END
        -- Holiday boost
        + CASE WHEN holiday_flag THEN MOD(h / 41, 15) ELSE 0 END
        -- Weekend boost
        + CASE WHEN day_of_week IN ('Sat', 'Sun') THEN MOD(h / 43, 8) ELSE 0 END
        ) AS units_sold,
        -- Inventory: 0-500 with ~5% stockout
        CASE
            WHEN MOD(h, 20) = 0 THEN 0
            ELSE 10 + MOD(h / 47, 490)
        END AS inventory_start_of_day,
        h
    FROM filtered
)
SELECT
    product_id,
    store_id,
    date,
    selling_price,
    cost_at_sale,
    units_sold,
    ROUND(selling_price * units_sold, 2) AS revenue,
    inventory_start_of_day,
    -- returns: ~3% of units_sold
    CASE WHEN MOD(h / 53, 33) = 0 THEN GREATEST(1, FLOOR(units_sold * 0.03)) ELSE 0 END AS returns_units
FROM with_price;

-- ─── FACT_COMPETITOR_PRICE_DAILY (~500K rows) ────────────────────────────────
-- 3 competitors, covering ~50% of products (more competitive categories)

INSERT INTO FACT_COMPETITOR_PRICE_DAILY (
    competitor_id, product_id, store_id, date, competitor_price
)
WITH competitor_products AS (
    -- Competitors focus on Electronics, Food & Beverage, Personal Care
    SELECT p.product_id, p.base_price
    FROM DIM_PRODUCT p
    WHERE p.category IN ('Electronics', 'Food & Beverage', 'Personal Care')
),
competitors AS (
    SELECT column1 AS competitor_id FROM VALUES (1), (2), (3)
),
combos AS (
    SELECT
        comp.competitor_id,
        cp.product_id,
        s.store_id,
        c.date,
        cp.base_price,
        ABS(HASH(comp.competitor_id || '-' || cp.product_id || '-' || s.store_id || '-' || c.date)) AS h
    FROM competitors comp
    CROSS JOIN competitor_products cp
    CROSS JOIN DIM_STORE s
    CROSS JOIN DIM_CALENDAR c
)
SELECT
    competitor_id,
    product_id,
    store_id,
    date,
    -- competitor_price: our base_price * (0.90 to 1.10)
    ROUND(base_price * (0.90 + (MOD(h, 2000) / 10000.0)), 2) AS competitor_price
FROM combos
WHERE MOD(h, 100) < 4;  -- ~4% sampling to get ~500K rows from huge cross join

-- ─── FACT_EVENTS (~500 rows) ─────────────────────────────────────────────────

INSERT INTO FACT_EVENTS (event_id, store_id, date, event_type, event_intensity, event_duration_days)
WITH event_types AS (
    SELECT column1 AS event_type FROM VALUES
        ('weather_shock'), ('sports_event'), ('concert'),
        ('festival'), ('local_holiday'), ('construction')
),
candidates AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY s.store_id, c.date, et.event_type) AS event_id,
        s.store_id,
        c.date,
        et.event_type,
        ABS(HASH(s.store_id || '-' || c.date || '-' || et.event_type)) AS h
    FROM DIM_STORE s
    CROSS JOIN DIM_CALENDAR c
    CROSS JOIN event_types et
)
SELECT
    event_id,
    store_id,
    date,
    event_type,
    ROUND(0.10 + (MOD(h, 8500) / 10000.0), 2) AS event_intensity,
    1 + MOD(h / 59, 5) AS event_duration_days
FROM candidates
WHERE MOD(h, 220) = 0  -- ~0.45% to get ~500 events
LIMIT 500;

-- ─── GRAPH_PRODUCT_PRODUCT_EDGE (~2000 rows) ─────────────────────────────────
-- Edge types: same_category, substitute, complement, cannibalization

INSERT INTO GRAPH_PRODUCT_PRODUCT_EDGE (src_product_id, dst_product_id, edge_type, weight, snapshot_date)
WITH

-- same_category: products in the same subcategory
same_cat AS (
    SELECT
        a.product_id AS src_product_id,
        b.product_id AS dst_product_id,
        'same_category' AS edge_type,
        ROUND(0.70 + (MOD(ABS(HASH(a.product_id || '-' || b.product_id)), 3000) / 10000.0), 4) AS weight,
        '2025-01-01'::DATE AS snapshot_date
    FROM DIM_PRODUCT a
    JOIN DIM_PRODUCT b
        ON a.subcategory = b.subcategory
        AND a.product_id < b.product_id
),

-- substitute: same subcategory + similar price band (within 30%)
substitute AS (
    SELECT
        a.product_id AS src_product_id,
        b.product_id AS dst_product_id,
        'substitute' AS edge_type,
        ROUND(0.50 + (MOD(ABS(HASH(a.product_id || '-' || b.product_id || '-sub')), 4000) / 10000.0), 4) AS weight,
        '2025-01-01'::DATE AS snapshot_date
    FROM DIM_PRODUCT a
    JOIN DIM_PRODUCT b
        ON a.subcategory = b.subcategory
        AND a.product_id < b.product_id
        AND a.brand != b.brand
        AND ABS(a.base_price - b.base_price) / GREATEST(a.base_price, b.base_price) < 0.30
),

-- complement: cross-category pairs (phone + accessories, apparel + personal care)
complement AS (
    SELECT
        a.product_id AS src_product_id,
        b.product_id AS dst_product_id,
        'complement' AS edge_type,
        ROUND(0.30 + (MOD(ABS(HASH(a.product_id || '-' || b.product_id || '-comp')), 4000) / 10000.0), 4) AS weight,
        '2025-01-01'::DATE AS snapshot_date
    FROM DIM_PRODUCT a
    JOIN DIM_PRODUCT b ON a.product_id < b.product_id
    WHERE (a.subcategory = 'Phones' AND b.subcategory = 'Accessories')
       OR (a.subcategory = 'Laptops' AND b.subcategory = 'Accessories')
       OR (a.subcategory = 'Tablets' AND b.subcategory = 'Accessories')
       OR (a.subcategory IN ('T-Shirts','Jeans','Jackets') AND b.subcategory = 'Shoes')
       OR (a.subcategory = 'Skincare' AND b.subcategory = 'Body Wash')
       OR (a.subcategory = 'Haircare' AND b.subcategory = 'Body Wash')
       OR (a.subcategory = 'Snacks' AND b.subcategory = 'Beverages')
       OR (a.subcategory = 'Bakery' AND b.subcategory = 'Dairy')
       OR (a.subcategory = 'Bedding' AND b.subcategory = 'Lighting')
       OR (a.subcategory = 'Furniture' AND b.subcategory = 'Decor')
),

-- cannibalization: same brand + same subcategory
cannibalize AS (
    SELECT
        a.product_id AS src_product_id,
        b.product_id AS dst_product_id,
        'cannibalization' AS edge_type,
        ROUND(0.40 + (MOD(ABS(HASH(a.product_id || '-' || b.product_id || '-cann')), 4000) / 10000.0), 4) AS weight,
        '2025-01-01'::DATE AS snapshot_date
    FROM DIM_PRODUCT a
    JOIN DIM_PRODUCT b
        ON a.brand = b.brand
        AND a.subcategory = b.subcategory
        AND a.product_id < b.product_id
),

all_edges AS (
    SELECT * FROM same_cat
    UNION ALL
    SELECT * FROM substitute
    UNION ALL
    SELECT * FROM complement
    UNION ALL
    SELECT * FROM cannibalize
)
SELECT * FROM all_edges;

-- ─── GRAPH_STORE_STORE_EDGE (~200 rows) ──────────────────────────────────────
-- Same-region pairs with weight inversely proportional to approximate distance

INSERT INTO GRAPH_STORE_STORE_EDGE (src_store_id, dst_store_id, weight, snapshot_date)
SELECT
    a.store_id AS src_store_id,
    b.store_id AS dst_store_id,
    -- Weight: inverse of haversine-ish distance, normalized to 0-1
    -- Same city ≈ 0.95+, same region near ≈ 0.5-0.8, same region far ≈ 0.2-0.5
    ROUND(GREATEST(0.10,
        1.0 - (SQRT(POWER(a.lat - b.lat, 2) + POWER(a.lon - b.lon, 2)) / 30.0)
    ), 4) AS weight,
    '2025-01-01'::DATE AS snapshot_date
FROM DIM_STORE a
JOIN DIM_STORE b
    ON a.region = b.region
    AND a.store_id < b.store_id;

-- ─── GRAPH_PRODUCT_STORE_EDGE (~5000 rows) ───────────────────────────────────
-- Top product-store pairs by sales volume share

INSERT INTO GRAPH_PRODUCT_STORE_EDGE (product_id, store_id, weight, snapshot_date)
WITH store_product_sales AS (
    SELECT
        product_id,
        store_id,
        SUM(units_sold) AS total_units
    FROM FACT_SALES_DAILY
    GROUP BY product_id, store_id
),
max_units AS (
    SELECT MAX(total_units) AS max_u FROM store_product_sales
),
ranked AS (
    SELECT
        s.product_id,
        s.store_id,
        ROUND(s.total_units / m.max_u, 4) AS weight,
        ROW_NUMBER() OVER (ORDER BY s.total_units DESC) AS rn
    FROM store_product_sales s
    CROSS JOIN max_units m
)
SELECT
    product_id,
    store_id,
    weight,
    '2025-01-01'::DATE AS snapshot_date
FROM ranked
WHERE rn <= 5000;
