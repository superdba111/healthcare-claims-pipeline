-- vw_monthly_costs.sql --- Monthly charge summary by in-network status
CREATE OR REPLACE VIEW healthcare_gold.vw_monthly_costs AS
SELECT
  DATE_TRUNC('month', f.received_date) AS month,
  f.in_network,
  p.state,
  COUNT(*) AS claim_count,
  SUM(f.charge_amt) AS total_charged,
  SUM(f.allowed_amt) AS total_allowed,
  SUM(f.discount_amt) AS total_discount,
  ROUND(AVG(f.discount_amt / NULLIF(f.charge_amt,0)) * 100, 2) AS avg_discount_pct
FROM healthcare_gold.fact_claims f
JOIN healthcare_gold.dim_provider p USING (provider_npi)
GROUP BY 1, 2, 3;


-- vw_provider_kpis.sql --- Provider performance metrics
CREATE OR REPLACE VIEW healthcare_gold.vw_provider_kpis AS
SELECT
  p.provider_name,
  p.city, p.state,
  COUNT(*) AS total_claims,
  SUM(f.charge_amt) AS total_charged,
  ROUND(AVG(f.charge_amt), 2) AS avg_charge,
  SUM(CASE WHEN f.in_network THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS in_network_pct
FROM healthcare_gold.fact_claims f
JOIN healthcare_gold.dim_provider p USING (provider_npi)
GROUP BY 1, 2, 3;


-- Tracks the volume and value of COVID-19 related procedures (U0003, U0004, U0005, 87635, 87426)
CREATE OR REPLACE VIEW healthcare_gold.vw_covid19_tests AS
SELECT
  DATE_TRUNC('month', f.received_date) AS month,
  f.claim_code,
  dp.procedure_desc,
  COUNT(*) AS test_count,
  SUM(f.charge_amt) AS total_charge,
  SUM(f.allowed_amt) AS total_allowed,
  ROUND(AVG(f.discount_amt), 2) AS avg_discount_per_test
FROM healthcare_gold.fact_claims f
JOIN healthcare_gold.dim_procedure dp ON f.claim_code = dp.claim_code
WHERE f.claim_code IN ('U0003', 'U0004', 'U0005', '87635', '87426', 'G2023', 'G2024')
GROUP BY 1, 2, 3
ORDER BY month DESC, test_count DESC;


-- Identifies which labs performed most COVID tests and their network status
CREATE OR REPLACE VIEW healthcare_gold.vw_covid19_providers AS
SELECT
  p.provider_name,
  p.state,
  f.in_network,
  f.claim_code,
  COUNT(*) AS tests_performed,
  SUM(f.charge_amt) AS total_charged
FROM healthcare_gold.fact_claims f
JOIN healthcare_gold.dim_provider p ON f.provider_npi = p.provider_npi
WHERE f.claim_code LIKE 'U%' OR f.claim_code LIKE '87%' OR f.claim_code = 'G2023'
GROUP BY 1, 2, 3, 4
ORDER BY tests_performed DESC;

-- Compares costs and discounts between in-network and out-of-network claims
CREATE OR REPLACE VIEW healthcare_gold.vw_network_impact AS
SELECT
  f.in_network,
  COUNT(*) AS total_claims,
  SUM(f.charge_amt) AS total_charge,
  SUM(f.allowed_amt) AS total_allowed,
  SUM(f.discount_amt) AS total_discount,
  ROUND(AVG(f.discount_amt / NULLIF(f.charge_amt,0)) * 100, 2) AS avg_discount_rate
FROM healthcare_gold.fact_claims f
GROUP BY 1;

-- Identifies claims where the discount was unusually high (e.g., >50% of charge)
CREATE OR REPLACE VIEW healthcare_gold.vw_high_discount_alerts AS
SELECT
  f.claim_item_id,
  dp.procedure_desc,
  f.charge_amt,
  f.allowed_amt,
  f.discount_amt,
  ROUND(f.discount_amt / f.charge_amt * 100, 2) AS discount_pct,
  f.received_date,
  p.provider_name
FROM healthcare_gold.fact_claims f
JOIN healthcare_gold.dim_provider p ON f.provider_npi = p.provider_npi
JOIN healthcare_gold.dim_procedure dp ON f.claim_code = dp.claim_code
WHERE f.charge_amt > 0 
  AND (f.discount_amt / f.charge_amt) > 0.5  -- Discounts greater than 50%
ORDER BY discount_pct DESC;

-- High-level financial dashboard for executives
-- vw_financial_summary.sql --- High-level financial dashboard for executives
CREATE OR REPLACE VIEW healthcare_gold.vw_financial_summary AS
SELECT
  DATE_TRUNC('month', received_date) AS month,
  COUNT(DISTINCT claimant_id) AS unique_patients,
  SUM(units) AS total_service_units,
  SUM(charge_amt) AS gross_revenue,
  SUM(allowed_amt) AS net_revenue,
  SUM(charge_amt - allowed_amt) AS total_contractual_adjustments,
  ROUND(SUM(charge_amt - allowed_amt) / NULLIF(SUM(charge_amt), 0) * 100, 2) AS discount_rate_pct
FROM healthcare_gold.fact_claims
GROUP BY 1
ORDER BY 1 DESC;


-- Ranks CPT/HCPCS codes by volume and revenue
CREATE OR REPLACE VIEW healthcare_gold.vw_top_procedures AS
SELECT
  dp.claim_code,
  dp.procedure_desc,
  COUNT(*) AS times_billed,
  SUM(f.units) AS total_units,
  SUM(f.charge_amt) AS total_charges,
  RANK() OVER (ORDER BY COUNT(*) DESC) AS rank_by_volume
FROM healthcare_gold.fact_claims f
JOIN healthcare_gold.dim_procedure dp ON f.claim_code = dp.claim_code
GROUP BY 1, 2
ORDER BY times_billed DESC;

-- Since DIAG_CODE_2-5 were unpivoted to a bridge table (conceptually), 
-- this view simulates joining primary diagnosis to facts.
-- Note: You would need a dim_diagnosis table for this.
-- Alternative: Query Silver layer directly for diagnosis analysis
-- This works because Silver retains all DIAG_CODE columns
CREATE OR REPLACE VIEW healthcare_gold.vw_diagnosis_silver AS
SELECT
  CAST(claim_item_id AS BIGINT) AS claim_item_id,
  CAST(claimant_id AS BIGINT) AS claimant_id,
  claim_code,
  provider_npi,
  diag_code_1 AS primary_diagnosis,
  diag_code_2 AS secondary_diagnosis_1,
  diag_code_3 AS secondary_diagnosis_2,
  diag_code_4 AS secondary_diagnosis_3,
  diag_code_5 AS secondary_diagnosis_4,
  charge_amt,
  allowed_amt,
  received_date
FROM healthcare_silver.claims_silver;

*************now we use silver for above views, run this first then run above view *************************

CREATE EXTERNAL TABLE IF NOT EXISTS healthcare_silver.claims_silver (
  claimant_id STRING,
  claim_item_id STRING,
  type STRING,
  received_date STRING,
  charge_amt STRING,
  allowed_amt STRING,
  diag_code_1 STRING,
  diag_code_2 STRING,
  diag_code_3 STRING,
  diag_code_4 STRING,
  diag_code_5 STRING,
  claim_code STRING,
  proc_desc STRING,
  claim_code_modifier STRING,
  claim_code_modifier_2 STRING,
  units STRING,
  oi_in_network STRING,
  service_provider STRING,
  service_address_3 STRING,
  service_address_2 STRING,
  provider_npi STRING,
  city STRING,
  state STRING,
  is_valid BOOLEAN
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
LOCATION 's3://hc-pipeline-demo/silver/';

-- Tracks patient activity without exposing PII (CLAIMANT_ID masked for analysts)
CREATE OR REPLACE VIEW healthcare_gold.vw_patient_activity AS
SELECT
  MD5(TO_UTF8(CAST(f.claimant_id AS VARCHAR))) AS hashed_patient_id,
  COUNT(f.claim_item_id) AS total_claims,
  SUM(f.charge_amt) AS total_patient_charges,
  SUM(f.allowed_amt) AS total_patient_allowed,
  DATE_TRUNC('month', MIN(f.received_date)) AS first_service_date,
  DATE_TRUNC('month', MAX(f.received_date)) AS last_service_date
FROM healthcare_gold.fact_claims f
GROUP BY MD5(TO_UTF8(CAST(f.claimant_id AS VARCHAR)))
HAVING COUNT(*) > 1
ORDER BY total_patient_charges DESC;


-- Compares average costs by state
CREATE OR REPLACE VIEW healthcare_gold.vw_geo_cost_variation AS
SELECT
  p.state,
  COUNT(*) AS claim_count,
  ROUND(AVG(f.charge_amt), 2) AS avg_charge,
  ROUND(AVG(f.allowed_amt), 2) AS avg_allowed,
  ROUND(AVG(f.discount_amt), 2) AS avg_discount
FROM healthcare_gold.fact_claims f
JOIN healthcare_gold.dim_provider p ON f.provider_npi = p.provider_npi
GROUP BY p.state
HAVING COUNT(*) > 10
ORDER BY avg_charge DESC;

-- Classifies services as 'COVID', 'Mental Health', 'Therapy', 'Surgery', 'Lab'
CREATE OR REPLACE VIEW healthcare_gold.vw_service_type_breakdown AS
SELECT
  CASE 
    WHEN claim_code IN ('U0003', 'U0004', 'U0005', '87635', '87426', 'G2023', 'G2024') THEN 'COVID-19 Testing'
    WHEN claim_code BETWEEN '90832' AND '90899' THEN 'Psychiatry/Mental Health'
    WHEN claim_code BETWEEN '97110' AND '97599' THEN 'Therapy/Rehab'
    WHEN claim_code BETWEEN '10000' AND '69999' THEN 'Surgery/Procedure'
    ELSE 'Lab/Other'
  END AS service_category,
  DATE_TRUNC('month', received_date) AS month,
  COUNT(*) AS volume,
  SUM(charge_amt) AS revenue
FROM healthcare_gold.fact_claims
GROUP BY 1, 2
ORDER BY month, volume DESC;
-- Analyzes how many line items are billed per patient per day (helps detect billing patterns)
CREATE OR REPLACE VIEW healthcare_gold.vw_claim_density AS
SELECT
  claimant_id,
  received_date,
  COUNT(*) AS line_items_per_day,
  SUM(charge_amt) AS daily_charge
FROM healthcare_gold.fact_claims
GROUP BY 1, 2
HAVING COUNT(*) > 5  -- Days with unusually high billing
ORDER BY line_items_per_day DESC;

-- Flags claims where the payer allowed $0 (denials or non-covered services)
CREATE OR REPLACE VIEW healthcare_gold.vw_zero_allowed_audit AS
SELECT
  f.claim_item_id,
  dp.procedure_desc,
  f.charge_amt,
  f.allowed_amt,
  f.received_date,
  p.provider_name,
  f.in_network
FROM healthcare_gold.fact_claims f
JOIN healthcare_gold.dim_provider p ON f.provider_npi = p.provider_npi
JOIN healthcare_gold.dim_procedure dp ON f.claim_code = dp.claim_code
WHERE f.allowed_amt = 0 AND f.charge_amt > 0
ORDER BY f.charge_amt DESC;

-- Provides a BI-accessible view of potential duplicates identified earlier
CREATE OR REPLACE VIEW healthcare_gold.vw_duplicate_suspicions AS
SELECT
  claimant_id,
  received_date,
  claim_code,
  charge_amt,
  COUNT(*) AS duplicate_count
FROM healthcare_gold.fact_claims
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, charge_amt DESC;

-- Cumulative fiscal year summary
CREATE OR REPLACE VIEW healthcare_gold.vw_ytd_summary AS
SELECT
  claimant_id,
  EXTRACT(YEAR FROM received_date) AS year,
  SUM(charge_amt) AS ytd_charges,
  SUM(allowed_amt) AS ytd_allowed
FROM healthcare_gold.fact_claims
WHERE EXTRACT(MONTH FROM received_date) <= 6  -- Example: Q2 reporting
GROUP BY 1, 2;
