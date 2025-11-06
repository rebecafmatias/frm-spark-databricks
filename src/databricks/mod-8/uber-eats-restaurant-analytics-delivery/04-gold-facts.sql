-- =====================================================================================================================
-- GOLD LAYER - FACT TABLES (KIMBALL DIMENSIONAL MODELING)
-- =====================================================================================================================
-- This file contains fact tables following Ralph Kimball's dimensional modeling methodology.
--
-- Fact Table Design Principles:
-- 1. Contains ONLY foreign keys to dimensions, metrics/measures, and degenerate dimensions
-- 2. NO denormalized attributes (names, types, categories, etc.)
-- 3. Descriptive attributes are retrieved via JOINs to dimension tables in analytics queries
-- 4. Optimized for aggregation and analytical queries
--
-- Reference: .claude/kb/lakeflow/quick-reference.md
-- =====================================================================================================================

-- =====================================================================================================================
-- FACT: fact_ratings
-- =====================================================================================================================
-- Purpose: Stores restaurant rating events with foreign key to restaurant dimension
-- Grain: One row per rating event
--
-- Design:
--   - rating_id: Degenerate dimension (operational transaction identifier)
--   - restaurant_id: Foreign key to dim_restaurant
--   - rating: Numeric measure (metric)
--   - rating_timestamp: Event timestamp
--   - ingestion_timestamp: Audit column
--
-- Query Pattern:
--   SELECT
--     dr.restaurant_name,
--     dr.cuisine_type,
--     dr.city,
--     AVG(fr.rating) as avg_rating,
--     COUNT(*) as total_ratings
--   FROM fact_ratings fr
--   INNER JOIN dim_restaurant dr ON fr.restaurant_id = dr.restaurant_id
--   GROUP BY dr.restaurant_name, dr.cuisine_type, dr.city
-- =====================================================================================================================
CREATE OR REFRESH LIVE TABLE fact_ratings
COMMENT "Restaurant rating facts - contains only foreign keys and metrics (Kimball methodology)"
TBLPROPERTIES (
  "quality" = "gold",
  "layer" = "fact",
  "grain" = "one_row_per_rating_event",
  "modeling" = "kimball_star_schema"
)
AS SELECT
  r.rating_id,                    -- Degenerate dimension (operational ID)
  rest.restaurant_id,             -- FK to dim_restaurant
  r.rating,                       -- Measure: rating value
  r.rating_timestamp,             -- Event timestamp
  r.ingestion_timestamp           -- Audit column
FROM LIVE.silver_ratings r
INNER JOIN LIVE.silver_restaurants rest
  ON r.restaurant_identifier = rest.cnpj
WHERE rest.restaurant_id IS NOT NULL
  AND r.rating IS NOT NULL;       -- Ensure metric quality


-- =====================================================================================================================
-- FACT: fact_inventory
-- =====================================================================================================================
-- Purpose: Stores current inventory snapshot with foreign keys to restaurant and product dimensions
-- Grain: One row per restaurant-product combination at a point in time
--
-- Design:
--   - stock_id: Degenerate dimension (operational transaction identifier)
--   - restaurant_id: Foreign key to dim_restaurant
--   - product_id: Foreign key to dim_product
--   - quantity_available: Numeric measure (metric)
--   - inventory_value: Numeric measure (derived metric)
--   - last_updated: State timestamp
--   - ingestion_timestamp: Audit column
--
-- Query Pattern:
--   SELECT
--     dr.restaurant_name,
--     dr.city,
--     dp.product_name,
--     dp.product_type,
--     fi.quantity_available,
--     fi.inventory_value
--   FROM fact_inventory fi
--   INNER JOIN dim_restaurant dr ON fi.restaurant_id = dr.restaurant_id
--   INNER JOIN dim_product dp ON fi.product_id = dp.product_id
--   WHERE dr.city = 'São Paulo'
-- =====================================================================================================================
CREATE OR REFRESH LIVE TABLE fact_inventory
COMMENT "Current inventory facts - contains only foreign keys and metrics (Kimball methodology)"
TBLPROPERTIES (
  "quality" = "gold",
  "layer" = "fact",
  "grain" = "one_row_per_restaurant_product_snapshot",
  "modeling" = "kimball_star_schema"
)
AS SELECT
  inv.stock_id,                              -- Degenerate dimension (operational ID)
  inv.restaurant_id,                         -- FK to dim_restaurant
  inv.product_id,                            -- FK to dim_product
  inv.quantity_available,                    -- Measure: quantity in stock
  (inv.quantity_available * prod.price) AS inventory_value,  -- Measure: calculated value
  inv.last_updated,                          -- State timestamp
  inv.ingestion_timestamp                    -- Audit column
FROM LIVE.silver_inventory inv
INNER JOIN LIVE.silver_products prod
  ON inv.restaurant_id = prod.restaurant_id
  AND inv.product_id = prod.product_id
INNER JOIN LIVE.silver_restaurants rest
  ON inv.restaurant_id = rest.restaurant_id
WHERE inv.quantity_available >= 0           -- Data quality check
  AND prod.price IS NOT NULL                -- Ensure price available for calculation
  AND rest.restaurant_id IS NOT NULL;       -- Ensure valid FK


-- =====================================================================================================================
-- KIMBALL DIMENSIONAL MODELING VERIFICATION CHECKLIST
-- =====================================================================================================================
-- ✅ Fact tables contain ONLY:
--    - Foreign keys to dimensions (restaurant_id, product_id)
--    - Numeric measures/metrics (rating, quantity_available, inventory_value)
--    - Degenerate dimensions (rating_id, stock_id)
--    - Timestamps (rating_timestamp, last_updated, ingestion_timestamp)
--
-- ✅ NO denormalized attributes:
--    - Removed: restaurant_name, cuisine_type, city, country
--    - Removed: product_name, product_type, price
--    - These attributes belong in dimension tables (dim_restaurant, dim_product)
--
-- ✅ Analytics queries use JOINs to dimensions:
--    - fact_ratings + dim_restaurant = Complete rating analytics
--    - fact_inventory + dim_restaurant + dim_product = Complete inventory analytics
--
-- ✅ Benefits:
--    - Reduced storage (no redundant data)
--    - Single source of truth for dimension attributes
--    - Easy dimension updates (SCD Type 1 in dimension tables)
--    - Optimized fact table for aggregations
--    - Conforms to Kimball star schema design
-- =====================================================================================================================
