-- GOLD LAYER - User Demographics for Business Intelligence
--
-- PURPOSE:
-- This module creates demographic insights for business intelligence and strategic planning.
-- It aggregates user profiles by multiple dimensions for executive dashboards and analytics.
--
-- WHAT IT DOES:
-- - Analyzes job distribution across users for market positioning
-- - Tracks user distribution by city for geographic expansion
-- - Provides data completeness metrics for data quality monitoring
-- - Calculates platform-wide statistics for executive reporting
--
-- DATA FLOW:
--   silver_users_unified (current state)
--     -> Job category extraction
--     -> Geographic aggregation
--     -> Data quality metrics
--     -> gold_user_demographics (BI dashboards)
--
-- WHY FROM SCD TYPE 1?
-- Business intelligence needs CURRENT snapshot:
-- - Executive dashboards show "as of today" metrics
-- - Strategic planning uses current user base size
-- - Geographic expansion decisions based on active users
-- - No need for temporal trends (use separate historical tables)
--
-- BUSINESS VALUE:
--
-- Job Distribution:
-- - Target B2B customers (Corporate, Executivo roles)
-- - Identify high-value professions (Arquiteto, Diretor)
-- - Tailor marketing messages by profession
--
-- Geographic Distribution:
-- - Resource allocation (customer support by city)
-- - Market penetration analysis (user count per city)
-- - Expansion opportunities (underserved cities)
--
-- Data Quality Monitoring:
-- - Profile completeness (% with email, name, birthday)
-- - Data enrichment opportunities (missing fields)
-- - CRM sync health (MongoDB vs MSSQL coverage)
--
-- KEY FEATURES:
-- - Job categorization: Extract role type from job field
-- - Geographic aggregation: City-level granularity
-- - Completeness metrics: Track null percentages
-- - Materialized view: Daily batch refresh
--
-- LEARNING OBJECTIVES:
-- - Design BI-ready aggregations for executives
-- - Extract categories from text fields (job parsing)
-- - Calculate data quality metrics
-- - Build demographic insights from user profiles
--
-- OUTPUT SCHEMA:
-- - city: Geographic dimension
-- - total_users: User count per city
-- - users_with_email: Email coverage metric
-- - users_with_birthday: Birthday coverage metric
-- - users_with_job: Job coverage metric
-- - top_job_category: Most common job type in city
-- - profile_completeness_pct: Overall data quality score
-- - avg_user_age: Demographic indicator

CREATE OR REFRESH MATERIALIZED VIEW gold_user_demographics
COMMENT 'User demographic analysis for business intelligence and strategic planning'
TBLPROPERTIES (
  'quality' = 'gold',
  'layer' = 'analytics',
  'use_case' = 'business_intelligence',
  'refresh_schedule' = 'daily'
)
AS
WITH user_metrics AS (
  SELECT
    cpf,
    city,
    email,
    birthday,
    job,
    first_name,
    last_name,
    delivery_address,

    CASE
      WHEN birthday IS NULL THEN NULL
      ELSE FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25)
    END AS age,

    CASE
      WHEN job LIKE '%Executivo%' OR job LIKE '%Diretor%' OR job LIKE '%Gerente%' THEN 'Leadership'
      WHEN job LIKE '%Coordenador%' OR job LIKE '%Supervisor%' OR job LIKE '%Especialista%' THEN 'Management'
      WHEN job LIKE '%Analista%' OR job LIKE '%Consultor%' OR job LIKE '%Desenvolvedor%' THEN 'Professional'
      WHEN job LIKE '%Assistente%' OR job LIKE '%Associado%' OR job LIKE '%Junior%' THEN 'Entry-Level'
      WHEN job LIKE '%Designer%' OR job LIKE '%Arquiteto%' OR job LIKE '%Engenheiro%' THEN 'Technical'
      WHEN job IS NOT NULL THEN 'Other'
      ELSE 'Unknown'
    END AS job_category,

    CASE
      WHEN email IS NOT NULL
           AND first_name IS NOT NULL
           AND last_name IS NOT NULL
           AND birthday IS NOT NULL
           AND delivery_address IS NOT NULL
           AND job IS NOT NULL
      THEN 1 ELSE 0
    END AS is_complete_profile

  FROM silver_users_unified
  WHERE cpf IS NOT NULL
),

city_demographics AS (
  SELECT
    COALESCE(city, 'Unknown') AS city,

    COUNT(DISTINCT cpf) AS total_users,
    COUNT(DISTINCT CASE WHEN email IS NOT NULL THEN cpf END) AS users_with_email,
    COUNT(DISTINCT CASE WHEN birthday IS NOT NULL THEN cpf END) AS users_with_birthday,
    COUNT(DISTINCT CASE WHEN job IS NOT NULL THEN cpf END) AS users_with_job,
    COUNT(DISTINCT CASE WHEN first_name IS NOT NULL THEN cpf END) AS users_with_name,
    COUNT(DISTINCT CASE WHEN delivery_address IS NOT NULL THEN cpf END) AS users_with_address,
    SUM(is_complete_profile) AS complete_profiles,

    ROUND(AVG(age), 1) AS avg_user_age,

    MODE(job_category) AS top_job_category,

    ROUND(
      (COUNT(DISTINCT CASE WHEN email IS NOT NULL THEN cpf END) +
       COUNT(DISTINCT CASE WHEN first_name IS NOT NULL THEN cpf END) +
       COUNT(DISTINCT CASE WHEN birthday IS NOT NULL THEN cpf END) +
       COUNT(DISTINCT CASE WHEN job IS NOT NULL THEN cpf END) +
       COUNT(DISTINCT CASE WHEN delivery_address IS NOT NULL THEN cpf END)) * 100.0
      / (COUNT(DISTINCT cpf) * 5), 1
    ) AS profile_completeness_pct

  FROM user_metrics
  GROUP BY COALESCE(city, 'Unknown')
)

SELECT
  city,
  total_users,
  users_with_email,
  users_with_birthday,
  users_with_job,
  users_with_name,
  users_with_address,
  complete_profiles,
  avg_user_age,
  top_job_category,
  profile_completeness_pct,

  CASE
    WHEN profile_completeness_pct >= 90 THEN 'Excellent'
    WHEN profile_completeness_pct >= 75 THEN 'Good'
    WHEN profile_completeness_pct >= 50 THEN 'Fair'
    ELSE 'Poor'
  END AS data_quality_grade,

  current_timestamp() AS computed_at

FROM city_demographics
ORDER BY total_users DESC;
