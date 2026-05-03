-- ── SEMANTIC LAYER / REPORTING VIEWS ──────────────────────────

-- 1. Monthly Revenue Cycle KPI View (Executive Layer)
CREATE OR REPLACE VIEW healthcare_gold.vw_revenue_cycle_kpi AS
SELECT
    pa.payer_name,
    d.year,
    d.quarter,
    COUNT(f.claim_item_id)             AS total_claims,
    SUM(f.charge_amt)                  AS gross_charges,
    SUM(f.allowed_amt)                 AS total_allowed,
    ROUND(SUM(f.allowed_amt) / 
          NULLIF(SUM(f.charge_amt),0) * 100, 2) AS payment_to_charge_pct
FROM healthcare_gold.fact_claims f
JOIN healthcare_gold.dim_date d    ON f.service_date_key = d.date_key
LEFT JOIN healthcare_gold.dim_payer pa ON f.payer_key = pa.payer_key
GROUP BY 1, 2, 3;

-- 2. Provider Performance View (Billing Manager Layer)
CREATE OR REPLACE VIEW healthcare_gold.vw_provider_performance AS
SELECT
    pr.provider_name,
    pr.specialty,
    COUNT(*)                           AS volume,
    SUM(f.charge_amt)                  AS total_billed,
    SUM(CASE WHEN f.in_network THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS in_network_rate
FROM healthcare_gold.fact_claims f
JOIN healthcare_gold.dim_provider pr ON f.provider_npi = pr.provider_npi
GROUP BY 1, 2;