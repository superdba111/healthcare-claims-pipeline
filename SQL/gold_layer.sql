-- ── GOLD LAYER TABLES ──────────────────────────────────────────

-- 1. Create Fact Table (Partitioned for performance)
CREATE TABLE IF NOT EXISTS healthcare_gold.fact_claims
WITH (
  format       = 'ICEBERG',
  location     = 's3://hc-pipeline-demo/gold/fact_claims/',
  partitioning = ARRAY['month(received_date)']
)
AS
SELECT
    CAST(claim_item_id AS BIGINT) AS claim_item_id,
    CAST(claimant_id   AS BIGINT) AS claimant_id,
    claim_code,
    provider_npi,
    CAST(charge_amt    AS DECIMAL(12,2)) AS charge_amt,
    CAST(allowed_amt   AS DECIMAL(12,2)) AS allowed_amt,
    ROUND(charge_amt - allowed_amt, 2)    AS discount_amt,
    CAST(units         AS INTEGER)        AS units,
    CAST(received_date AS DATE)           AS received_date,
    CAST(format_datetime(received_date, 'yyyyMMdd') AS INT) AS service_date_key
FROM healthcare_silver.claims_cleaned
WHERE is_valid = TRUE;

-- 2. Create Provider Dimension
CREATE TABLE IF NOT EXISTS healthcare_gold.dim_provider
WITH (format = 'ICEBERG', location = 's3://hc-pipeline-demo/gold/dim_provider/')
AS
SELECT DISTINCT
    provider_npi,
    service_provider AS provider_name,
    city,
    state
FROM healthcare_silver.claims_cleaned
WHERE provider_npi IS NOT NULL;

-- 3. Create Remittance Fact (One-to-Many relationship)
-- Required to handle multiple adjustments per claim line
CREATE TABLE IF NOT EXISTS healthcare_gold.fact_remittance (
    remittance_id    BIGINT,
    claim_id         VARCHAR(50),
    payment_amount   DECIMAL(12,2),
    carc_code        VARCHAR(10),
    adjustment_reason VARCHAR(500)
)
WITH (format = 'ICEBERG', location = 's3://hc-pipeline-demo/gold/fact_remittance/');