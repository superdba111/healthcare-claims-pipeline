-- List all tables and views in your healthcare_gold database
SHOW TABLES IN healthcare_gold;

-- Or using information schema
SELECT table_name, table_type 
FROM information_schema.tables 
WHERE table_schema = 'healthcare_gold'
ORDER BY table_name;

-- Quick count of all your views
SELECT 'vw_financial_summary' AS view_name, COUNT(*) AS row_count FROM healthcare_gold.vw_financial_summary
UNION ALL
SELECT 'vw_monthly_costs', COUNT(*) FROM healthcare_gold.vw_monthly_costs
UNION ALL
SELECT 'vw_provider_kpis', COUNT(*) FROM healthcare_gold.vw_provider_kpis
UNION ALL
SELECT 'vw_covid19_tests', COUNT(*) FROM healthcare_gold.vw_covid19_tests
UNION ALL
SELECT 'vw_network_impact', COUNT(*) FROM healthcare_gold.vw_network_impact
UNION ALL
SELECT 'vw_top_procedures', COUNT(*) FROM healthcare_gold.vw_top_procedures
UNION ALL
SELECT 'vw_patient_activity', COUNT(*) FROM healthcare_gold.vw_patient_activity
UNION ALL
SELECT 'vw_geo_cost_variation', COUNT(*) FROM healthcare_gold.vw_geo_cost_variation
ORDER BY view_name;

-- Get a sample of each key view
SELECT * FROM healthcare_gold.vw_financial_summary LIMIT 5;
SELECT * FROM healthcare_gold.vw_monthly_costs LIMIT 5;
SELECT * FROM healthcare_gold.vw_provider_kpis LIMIT 5;
SELECT * FROM healthcare_gold.vw_top_procedures LIMIT 5;

-- Monthly revenue with month-over-month growth
SELECT 
    month,
    gross_revenue,
    net_revenue,
    unique_patients,
    LAG(gross_revenue) OVER (ORDER BY month) AS previous_month_revenue,
    ROUND((gross_revenue - LAG(gross_revenue) OVER (ORDER BY month)) / 
          NULLIF(LAG(gross_revenue) OVER (ORDER BY month), 0) * 100, 2) AS mom_growth_pct
FROM healthcare_gold.vw_financial_summary
ORDER BY month;

-- YTD summary by year
SELECT 
    EXTRACT(YEAR FROM month) AS year,
    SUM(gross_revenue) AS ytd_gross_revenue,
    SUM(net_revenue) AS ytd_net_revenue,
    SUM(unique_patients) AS ytd_unique_patients,
    ROUND(AVG(discount_rate_pct), 2) AS avg_discount_rate
FROM healthcare_gold.vw_financial_summary
GROUP BY EXTRACT(YEAR FROM month)
ORDER BY year;

-- Detailed COVID test analysis by month and test type
SELECT 
    month,
    claim_code,
    test_count,
    total_charge,
    total_allowed,
    avg_discount_per_test,
    ROUND(total_charge / NULLIF(test_count, 0), 2) AS avg_charge_per_test
FROM healthcare_gold.vw_covid19_tests
ORDER BY month DESC, test_count DESC;

-- Which labs performed the most COVID tests?
SELECT 
    provider_name,
    state,
    in_network,
    tests_performed,
    total_charged,
    ROUND(total_charged / SUM(total_charged) OVER () * 100, 2) AS market_share_pct
FROM healthcare_gold.vw_covid19_providers
ORDER BY tests_performed DESC
LIMIT 20;

-- Provider performance metrics
SELECT 
    provider_name,
    state,
    total_claims,
    total_charged,
    avg_charge,
    in_network_pct,
    -- Calculate average discount rate
    ROUND(total_charged / NULLIF(total_claims, 0), 2) AS revenue_per_claim
FROM healthcare_gold.vw_provider_kpis
WHERE total_claims >= 10  -- Minimum claim threshold
ORDER BY total_charged DESC
LIMIT 20;

-- Which states have the most providers?
SELECT 
    state,
    COUNT(DISTINCT provider_name) AS provider_count,
    SUM(total_claims) AS total_claims,
    SUM(total_charged) AS total_revenue,
    ROUND(AVG(in_network_pct), 2) AS avg_in_network_pct
FROM healthcare_gold.vw_provider_kpis
GROUP BY state
ORDER BY provider_count DESC;

-- Which states have the most providers?
SELECT 
    state,
    COUNT(DISTINCT provider_name) AS provider_count,
    SUM(total_claims) AS total_claims,
    SUM(total_charged) AS total_revenue,
    ROUND(AVG(in_network_pct), 2) AS avg_in_network_pct
FROM healthcare_gold.vw_provider_kpis
GROUP BY state
ORDER BY provider_count DESC;

-- Procedures with high average charge but low volume (potential outlier detection)
SELECT 
    procedure_desc,
    times_billed,
    total_charges,
    ROUND(total_charges / NULLIF(times_billed, 0), 2) AS avg_charge_per_claim
FROM healthcare_gold.vw_top_procedures
WHERE times_billed BETWEEN 5 AND 100  -- Not too rare, not too common
  AND total_charges / NULLIF(times_billed, 0) > 500  -- High average charge
ORDER BY avg_charge_per_claim DESC
LIMIT 20;

-- Compare states across multiple metrics
SELECT 
    state,
    claim_count,
    ROUND(avg_charge, 2) AS avg_charge,
    ROUND(avg_allowed, 2) AS avg_allowed,
    ROUND(avg_discount, 2) AS avg_discount,
    ROUND((avg_charge - avg_allowed) / NULLIF(avg_charge, 0) * 100, 2) AS discount_rate_pct,
    RANK() OVER (ORDER BY avg_charge DESC) AS cost_rank
FROM healthcare_gold.vw_geo_cost_variation
WHERE claim_count >= 10  -- Minimum sample size
ORDER BY avg_charge DESC;

-- Group states into regions for broader analysis
SELECT 
    CASE 
        WHEN state IN ('NY', 'NJ', 'CT', 'MA', 'PA', 'MD', 'DC') THEN 'Northeast'
        WHEN state IN ('CA', 'WA', 'OR', 'NV', 'AZ', 'CO', 'UT') THEN 'West'
        WHEN state IN ('TX', 'FL', 'GA', 'NC', 'SC', 'VA', 'TN') THEN 'South'
        WHEN state IN ('IL', 'OH', 'MI', 'IN', 'WI', 'MN', 'MO') THEN 'Midwest'
        ELSE 'Other'
    END AS region,
    COUNT(DISTINCT state) AS state_count,
    SUM(claim_count) AS total_claims,
    ROUND(AVG(avg_charge), 2) AS avg_charge,
    ROUND(AVG(avg_discount), 2) AS avg_discount
FROM healthcare_gold.vw_geo_cost_variation
GROUP BY 1
ORDER BY avg_charge DESC;

-- Detailed view of high-discount claims
SELECT 
    procedure_desc,
    charge_amt,
    allowed_amt,
    discount_amt,
    discount_pct,
    provider_name,
    received_date
FROM healthcare_gold.vw_high_discount_alerts
WHERE discount_pct > 70  -- Extreme discounts
ORDER BY discount_pct DESC
LIMIT 50;

-- Claims denied or not covered by payer
SELECT 
    procedure_desc,
    charge_amt,
    allowed_amt,
    provider_name,
    received_date,
    in_network
FROM healthcare_gold.vw_zero_allowed_audit
ORDER BY charge_amt DESC
LIMIT 50;

-- Potential duplicate claims
SELECT 
    claimant_id,
    received_date,
    claim_code,
    charge_amt,
    duplicate_count
FROM healthcare_gold.vw_duplicate_suspicions
WHERE duplicate_count > 1
ORDER BY duplicate_count DESC, charge_amt DESC
LIMIT 100;

-- Calculate rolling average for trend analysis
WITH monthly_data AS (
    SELECT 
        month,
        gross_revenue,
        AVG(gross_revenue) OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_3m_avg
    FROM healthcare_gold.vw_financial_summary
)
SELECT 
    month,
    gross_revenue,
    ROUND(rolling_3m_avg, 2) AS rolling_3m_avg,
    ROUND(gross_revenue - rolling_3m_avg, 2) AS deviation_from_avg
FROM monthly_data
ORDER BY month;

-- Compare same month across years
SELECT 
    EXTRACT(MONTH FROM month) AS month_num,
    CASE EXTRACT(YEAR FROM month)
        WHEN 2020 THEN gross_revenue
    END AS revenue_2020,
    CASE EXTRACT(YEAR FROM month)
        WHEN 2021 THEN gross_revenue
    END AS revenue_2021,
    CASE EXTRACT(YEAR FROM month)
        WHEN 2022 THEN gross_revenue
    END AS revenue_2022
FROM healthcare_gold.vw_financial_summary
GROUP BY EXTRACT(MONTH FROM month), EXTRACT(YEAR FROM month), gross_revenue
ORDER BY month_num;

-- Statistical distribution of patient activity
SELECT 
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_claims) AS p25_claims,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_claims) AS median_claims,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_claims) AS p75_claims,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY total_claims) AS p90_claims,
    ROUND(AVG(total_patient_charges), 2) AS avg_spending,
    ROUND(STDDEV(total_patient_charges), 2) AS stddev_spending
FROM healthcare_gold.vw_patient_activity;

-- Track new patient acquisition over time
SELECT 
    DATE_TRUNC('month', first_service_date) AS cohort_month,
    COUNT(*) AS new_patients,
    ROUND(AVG(total_patient_charges), 2) AS avg_lifetime_value
FROM healthcare_gold.vw_patient_activity
GROUP BY DATE_TRUNC('month', first_service_date)
ORDER BY cohort_month;

