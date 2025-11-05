-- GOLD LAYER - User Change Audit for Compliance (LGPD/GDPR)
--
-- PURPOSE:
-- This module creates a compliance audit trail tracking all changes to user personal data.
-- It analyzes the SCD Type 2 history to identify which fields changed and when.
--
-- WHAT IT DOES:
-- - Tracks changes to sensitive personal data fields (email, address, name, job)
-- - Calculates change frequency per user for anomaly detection
-- - Identifies most recent changes for audit reports
-- - Provides compliance-ready data for LGPD/GDPR requirements
--
-- DATA FLOW:
--   silver_users_history (SCD Type 2 full history)
--     -> Version comparison (current vs previous)
--     -> Change detection by field
--     -> Aggregation by user
--     -> gold_user_change_audit (compliance reporting)
--
-- WHY FROM SCD TYPE 2?
-- LGPD/GDPR requires complete audit trail:
-- - Track WHO changed WHAT and WHEN
-- - Reconstruct historical state for investigations
-- - Prove compliance with data retention policies
-- - Support data subject access requests (DSAR)
--
-- COMPLIANCE REQUIREMENTS (LGPD/GDPR):
-- - Right to be Informed: Users must know when data changes
-- - Right to Access: Users can request their change history
-- - Right to Rectification: Track corrections to personal data
-- - Data Protection by Design: Audit trail built into pipeline
--
-- CHANGE DETECTION STRATEGY:
-- Using LAG() window function to compare current with previous version:
-- - email_changed: Compare email with previous version
-- - address_changed: Compare delivery_address with previous version
-- - profile_changed: Compare first_name, last_name, job, company_name
-- - Any change: Union of all field changes
--
-- AUDIT METRICS:
-- - total_changes: How many times user data was modified
-- - email_changes: Specific to email field (high sensitivity)
-- - address_changes: Specific to address field (PII tracking)
-- - profile_changes: Specific to name/job fields (identity data)
-- - last_change_date: Most recent modification timestamp
-- - days_since_last_change: Freshness indicator
--
-- KEY FEATURES:
-- - Change detection: LAG() compares versions
-- - Materialized view: Batch compliance reporting
-- - Audit trail: Complete history preservation
-- - LGPD/GDPR ready: Meets regulatory requirements
--
-- LEARNING OBJECTIVES:
-- - Implement compliance audit trails with SCD Type 2
-- - Use LAG() for change detection across versions
-- - Design LGPD/GDPR-compliant data pipelines
-- - Calculate audit metrics for regulatory reporting
--
-- OUTPUT SCHEMA:
-- - user_id: User identifier
-- - total_changes: Total number of modifications
-- - email_changes: Number of email modifications
-- - address_changes: Number of address modifications
-- - profile_changes: Number of name/job modifications
-- - last_change_date: Timestamp of most recent change
-- - days_since_last_change: Data freshness metric
-- - current_version_start: When current version became active
-- - is_active: Whether user has active record (__CURRENT = true)

CREATE OR REFRESH MATERIALIZED VIEW gold_user_change_audit
COMMENT 'User change audit trail for LGPD/GDPR compliance - tracks all personal data modifications'
TBLPROPERTIES (
  'quality' = 'gold',
  'layer' = 'compliance',
  'use_case' = 'lgpd_gdpr_audit',
  'pii_tracking' = 'true',
  'refresh_schedule' = 'daily'
)
AS
WITH user_changes AS (
  SELECT
    cpf,
    __START_AT AS version_start,
    __END_AT AS version_end,
    CASE WHEN __END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current_version,
    email,
    delivery_address,
    first_name,
    last_name,
    job,
    company_name,

    LAG(email) OVER (PARTITION BY cpf ORDER BY __START_AT) AS prev_email,
    LAG(delivery_address) OVER (PARTITION BY cpf ORDER BY __START_AT) AS prev_address,
    LAG(first_name) OVER (PARTITION BY cpf ORDER BY __START_AT) AS prev_first_name,
    LAG(last_name) OVER (PARTITION BY cpf ORDER BY __START_AT) AS prev_last_name,
    LAG(job) OVER (PARTITION BY cpf ORDER BY __START_AT) AS prev_job,

    CASE
      WHEN LAG(email) OVER (PARTITION BY cpf ORDER BY __START_AT) IS NULL THEN FALSE
      WHEN email != LAG(email) OVER (PARTITION BY cpf ORDER BY __START_AT) THEN TRUE
      ELSE FALSE
    END AS email_changed,

    CASE
      WHEN LAG(delivery_address) OVER (PARTITION BY cpf ORDER BY __START_AT) IS NULL THEN FALSE
      WHEN delivery_address != LAG(delivery_address) OVER (PARTITION BY cpf ORDER BY __START_AT) THEN TRUE
      ELSE FALSE
    END AS address_changed,

    CASE
      WHEN LAG(first_name) OVER (PARTITION BY cpf ORDER BY __START_AT) IS NULL THEN FALSE
      WHEN (
        first_name != LAG(first_name) OVER (PARTITION BY cpf ORDER BY __START_AT) OR
        last_name != LAG(last_name) OVER (PARTITION BY cpf ORDER BY __START_AT) OR
        job != LAG(job) OVER (PARTITION BY cpf ORDER BY __START_AT)
      ) THEN TRUE
      ELSE FALSE
    END AS profile_changed

  FROM silver_users_history
  WHERE cpf IS NOT NULL
),

user_change_summary AS (
  SELECT
    cpf,

    COUNT(*) - 1 AS total_changes,
    SUM(CASE WHEN email_changed THEN 1 ELSE 0 END) AS email_changes,
    SUM(CASE WHEN address_changed THEN 1 ELSE 0 END) AS address_changes,
    SUM(CASE WHEN profile_changed THEN 1 ELSE 0 END) AS profile_changes,

    MAX(version_start) AS last_change_date,
    DATEDIFF(day, MAX(version_start), current_date()) AS days_since_last_change,

    MAX(CASE WHEN is_current_version THEN version_start END) AS current_version_start,
    MAX(CASE WHEN is_current_version THEN 1 ELSE 0 END) AS is_active

  FROM user_changes
  GROUP BY cpf
)

SELECT
  cpf,
  total_changes,
  email_changes,
  address_changes,
  profile_changes,
  last_change_date,
  days_since_last_change,
  current_version_start,
  CASE WHEN is_active = 1 THEN TRUE ELSE FALSE END AS is_active,

  CASE
    WHEN total_changes = 0 THEN 'No Changes'
    WHEN total_changes BETWEEN 1 AND 3 THEN 'Low Activity'
    WHEN total_changes BETWEEN 4 AND 10 THEN 'Moderate Activity'
    ELSE 'High Activity'
  END AS change_frequency_category,

  current_timestamp() AS audit_computed_at

FROM user_change_summary
ORDER BY total_changes DESC;
