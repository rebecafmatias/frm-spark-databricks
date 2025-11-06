CREATE OR REFRESH LIVE TABLE gold_restaurant_summary
COMMENT "Restaurant performance metrics - joins fact_ratings with dim_restaurant"
TBLPROPERTIES ("quality" = "gold", "layer" = "analytics")
AS SELECT
  fr.restaurant_id,
  dr.restaurant_name,
  dr.cuisine_type,
  dr.city,
  dr.country,
  COUNT(fr.rating_id) AS total_ratings,
  ROUND(AVG(fr.rating), 2) AS avg_rating,
  SUM(CASE WHEN fr.rating >= 4 THEN 1 ELSE 0 END) AS high_ratings_count,
  SUM(CASE WHEN fr.rating <= 2 THEN 1 ELSE 0 END) AS low_ratings_count,
  ROUND(SUM(CASE WHEN fr.rating >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(fr.rating_id), 2) AS high_ratings_pct,
  ROUND(SUM(CASE WHEN fr.rating <= 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(fr.rating_id), 2) AS low_ratings_pct
FROM LIVE.fact_ratings fr
INNER JOIN LIVE.dim_restaurant dr
  ON fr.restaurant_id = dr.restaurant_id
GROUP BY
  fr.restaurant_id,
  dr.restaurant_name,
  dr.cuisine_type,
  dr.city,
  dr.country;


CREATE OR REFRESH LIVE TABLE gold_product_summary
COMMENT "Product inventory and performance metrics - joins fact_inventory with dim_product"
TBLPROPERTIES ("quality" = "gold", "layer" = "analytics")
AS SELECT
  fi.product_id,
  dp.product_name,
  dp.product_type,
  COUNT(DISTINCT fi.restaurant_id) AS restaurants_offering,
  SUM(fi.quantity_available) AS total_quantity,
  ROUND(SUM(fi.inventory_value), 2) AS total_inventory_value,
  ROUND(AVG(fi.quantity_available), 2) AS avg_quantity_per_restaurant
FROM LIVE.fact_inventory fi
INNER JOIN LIVE.dim_product dp
  ON fi.product_id = dp.product_id
GROUP BY
  fi.product_id,
  dp.product_name,
  dp.product_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Analytics: Cuisine Performance

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE gold_cuisine_summary
COMMENT "Performance metrics by cuisine type - joins fact_ratings with dim_restaurant"
TBLPROPERTIES ("quality" = "gold", "layer" = "analytics")
AS SELECT
  dr.cuisine_type,
  COUNT(DISTINCT fr.restaurant_id) AS restaurant_count,
  COUNT(fr.rating_id) AS total_ratings,
  ROUND(AVG(fr.rating), 2) AS avg_rating,
  ROUND(SUM(CASE WHEN fr.rating >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(fr.rating_id), 2) AS satisfaction_rate
FROM LIVE.fact_ratings fr
INNER JOIN LIVE.dim_restaurant dr
  ON fr.restaurant_id = dr.restaurant_id
GROUP BY dr.cuisine_type
HAVING COUNT(fr.rating_id) >= 10
ORDER BY avg_rating DESC;


CREATE OR REFRESH LIVE TABLE gold_restaurant_inventory
COMMENT "Restaurant inventory status - joins fact_inventory with both dimensions"
TBLPROPERTIES ("quality" = "gold", "layer" = "analytics")
AS SELECT
  fi.restaurant_id,
  dr.restaurant_name,
  dr.city,
  dr.country,
  COUNT(DISTINCT fi.product_id) AS unique_products,
  SUM(fi.quantity_available) AS total_items_in_stock,
  ROUND(SUM(fi.inventory_value), 2) AS total_inventory_value,
  ROUND(AVG(fi.inventory_value), 2) AS avg_product_value
FROM LIVE.fact_inventory fi
INNER JOIN LIVE.dim_restaurant dr
  ON fi.restaurant_id = dr.restaurant_id
INNER JOIN LIVE.dim_product dp
  ON fi.product_id = dp.product_id
GROUP BY
  fi.restaurant_id,
  dr.restaurant_name,
  dr.city,
  dr.country;

CREATE OR REFRESH LIVE TABLE gold_city_summary
COMMENT "City-level performance metrics - multi-fact analysis"
TBLPROPERTIES ("quality" = "gold", "layer" = "analytics")
AS SELECT
  dr.city,
  dr.country,
  COUNT(DISTINCT dr.restaurant_id) AS restaurant_count,
  COUNT(DISTINCT fr.rating_id) AS total_ratings,
  ROUND(AVG(fr.rating), 2) AS avg_rating,
  ROUND(AVG(inv.total_inventory_value), 2) AS avg_inventory_value_per_restaurant
FROM LIVE.dim_restaurant dr
LEFT JOIN LIVE.fact_ratings fr
  ON dr.restaurant_id = fr.restaurant_id
LEFT JOIN (
  SELECT
    restaurant_id,
    SUM(inventory_value) AS total_inventory_value
  FROM LIVE.fact_inventory
  GROUP BY restaurant_id
) inv
  ON dr.restaurant_id = inv.restaurant_id
GROUP BY
  dr.city,
  dr.country
ORDER BY restaurant_count DESC;
