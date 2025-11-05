-- GOLD LAYER - User Segmentation for Marketing and Analytics
--
-- PURPOSE:
-- This module creates business-ready user segments for marketing campaigns and analytics.
-- It aggregates current user profiles by demographic and geographic dimensions.
--
-- WHAT IT DOES:
-- - Calculates age groups from birthday for demographic segmentation
-- - Groups users by city and age for geographic + demographic analysis
-- - Aggregates user counts and profiles per segment
-- - Provides marketing-ready data for campaign targeting
--
-- DATA FLOW:
--   silver_users_unified (current state)
--     -> Age calculation from birthday
--     -> Geographic + demographic grouping
--     -> Aggregation by segment
--     -> gold_user_segments (business analytics)
--
-- WHY FROM SCD TYPE 1?
-- Marketing needs CURRENT user state, not historical:
-- - Target users based on their latest profile information
-- - Segment size reflects active user base
-- - No need for temporal analysis (historical trends in separate table)
--
-- SEGMENTATION STRATEGY:
--
-- Age Groups:
-- - 18-25: Young Adults (students, early career)
-- - 26-35: Millennials (established career, families starting)
-- - 36-50: Gen X (peak earning, family-focused)
-- - 51-65: Baby Boomers (mature professionals)
-- - 65+: Seniors (retirees, different spending patterns)
-- - Unknown: No birthday data (still valuable segment)
--
-- Geographic: City-level granularity for local campaigns
--
-- Business Value:
-- - Targeted marketing campaigns by age + location
-- - Resource allocation (customer support by city)
-- - Product development (features for age groups)
-- - Market expansion (underserved segments)
--
-- KEY FEATURES:
-- - Age calculation: FLOOR(DATEDIFF(days, birthday, current_date()) / 365.25)
-- - Materialized view: Batch aggregation for cost efficiency
-- - Segment metrics: User counts for campaign sizing
--
-- LEARNING OBJECTIVES:
-- - Design user segmentation for marketing use cases
-- - Calculate age from birthday with date functions
-- - Aggregate current state data for business insights
-- - Create materialized views for batch analytics
--
-- OUTPUT SCHEMA:
-- - city: Geographic segment (for local campaigns)
-- - age_group: Demographic segment (for targeted messaging)
-- - user_count: Number of users in segment
-- - avg_age: Average age (for validation/insights)
-- - computed_at: When segment was calculated

CREATE OR REFRESH MATERIALIZED VIEW gold_user_segments
COMMENT 'User segmentation by city and age group for marketing campaigns and analytics'
TBLPROPERTIES (
  'quality' = 'gold',
  'layer' = 'analytics',
  'use_case' = 'marketing_segmentation',
  'refresh_schedule' = 'daily'
)
AS
WITH user_demographics AS (
  SELECT
    cpf,
    city,
    CASE
      WHEN birthday IS NULL THEN NULL
      ELSE FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25)
    END AS age,
    CASE
      WHEN birthday IS NULL THEN 'Unknown'
      WHEN FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25) BETWEEN 18 AND 25 THEN '18-25 (Young Adults)'
      WHEN FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25) BETWEEN 26 AND 35 THEN '26-35 (Millennials)'
      WHEN FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25) BETWEEN 36 AND 50 THEN '36-50 (Gen X)'
      WHEN FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25) BETWEEN 51 AND 65 THEN '51-65 (Boomers)'
      WHEN FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25) > 65 THEN '65+ (Seniors)'
      ELSE 'Unknown'
    END AS age_group,
    email,
    first_name,
    last_name
  FROM silver_users_unified
  WHERE cpf IS NOT NULL
)

SELECT
  COALESCE(city, 'Unknown') AS city,
  age_group,
  COUNT(DISTINCT cpf) AS user_count,
  ROUND(AVG(age), 1) AS avg_age,
  COUNT(DISTINCT CASE WHEN email IS NOT NULL THEN cpf END) AS users_with_email,
  COUNT(DISTINCT CASE WHEN first_name IS NOT NULL THEN cpf END) AS users_with_name,
  current_timestamp() AS computed_at
FROM user_demographics
GROUP BY
  COALESCE(city, 'Unknown'),
  age_group
ORDER BY
  user_count DESC;
