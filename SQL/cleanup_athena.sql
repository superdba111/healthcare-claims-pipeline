-- Drop Gold layer tables (order matters due to dependencies)
DROP VIEW IF EXISTS healthcare_gold.vw_financial_summary;
DROP VIEW IF EXISTS healthcare_gold.vw_monthly_costs;
DROP VIEW IF EXISTS healthcare_gold.vw_provider_kpis;
DROP VIEW IF EXISTS healthcare_gold.vw_covid19_tests;
DROP VIEW IF EXISTS healthcare_gold.vw_network_impact;
DROP VIEW IF EXISTS healthcare_gold.vw_patient_activity;
DROP VIEW IF EXISTS healthcare_gold.vw_geo_cost_variation;
DROP VIEW IF EXISTS healthcare_gold.vw_top_procedures;
DROP VIEW IF EXISTS healthcare_gold.vw_high_discount_alerts;
DROP VIEW IF EXISTS healthcare_gold.vw_zero_allowed_audit;
DROP VIEW IF EXISTS healthcare_gold.vw_duplicate_suspicions;
DROP VIEW IF EXISTS healthcare_gold.vw_ytd_summary;
DROP VIEW IF EXISTS healthcare_gold.vw_service_type_breakdown;
DROP VIEW IF EXISTS healthcare_gold.vw_claim_density;

-- Drop fact tables
DROP TABLE IF EXISTS healthcare_gold.fact_claims;
DROP TABLE IF EXISTS healthcare_gold.fact_remittance;

-- Drop dimension tables (reverse order of dependencies)
DROP TABLE IF EXISTS healthcare_gold.dim_procedure;
DROP TABLE IF EXISTS healthcare_gold.dim_member;
DROP TABLE IF EXISTS healthcare_gold.dim_payer;
DROP TABLE IF EXISTS healthcare_gold.dim_provider;
DROP TABLE IF EXISTS healthcare_gold.dim_date;

-- Verify clean slate
SHOW TABLES IN healthcare_gold;
SHOW VIEWS IN healthcare_gold;