-- =============================================================================
-- CONSOLIDATED BITCOIN RECONCILIATION SQL
-- =============================================================================
-- This file contains all the essential SQL queries for Bitcoin reconciliation
-- Consolidates functionality from multiple redundant SQL files
-- =============================================================================

-- -----------------------------------------------------------------------------
-- SECTION 1: RECONCILIATION STATUS CHECK
-- -----------------------------------------------------------------------------

-- Get current reconciliation status
WITH curtailment_stats AS (
  SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT (settlement_date || '-' || settlement_period || '-' || farm_id)) as unique_combinations
  FROM curtailment_records
),
bitcoin_stats AS (
  SELECT
    miner_model,
    COUNT(*) as calculation_count
  FROM historical_bitcoin_calculations
  GROUP BY miner_model
)
SELECT 
  c.total_records as curtailment_records,
  c.unique_combinations as unique_combinations,
  COALESCE(b.miner_model, 'TOTAL') as miner_model,
  COALESCE(b.calculation_count, SUM(b.calculation_count) OVER ()) as calculation_count,
  c.unique_combinations * 3 as expected_calculations,
  ROUND((COALESCE(b.calculation_count, SUM(b.calculation_count) OVER ()) * 100.0) / 
    (c.unique_combinations * 3), 2) as reconciliation_percentage
FROM curtailment_stats c
CROSS JOIN bitcoin_stats b
GROUP BY c.total_records, c.unique_combinations, b.miner_model, b.calculation_count;

-- -----------------------------------------------------------------------------
-- SECTION 2: FINDING MISSING DATES
-- -----------------------------------------------------------------------------

-- Identify dates with missing Bitcoin calculations
WITH dates_with_curtailment AS (
  SELECT DISTINCT settlement_date
  FROM curtailment_records
  ORDER BY settlement_date DESC
),
unique_date_combos AS (
  SELECT 
    settlement_date,
    COUNT(DISTINCT (settlement_period || '-' || farm_id)) as unique_combinations
  FROM curtailment_records
  GROUP BY settlement_date
),
date_calculations AS (
  SELECT 
    c.settlement_date,
    COUNT(DISTINCT b.id) as calculation_count,
    u.unique_combinations * 3 as expected_count -- 3 is the number of miner models
  FROM dates_with_curtailment c
  JOIN unique_date_combos u ON c.settlement_date = u.settlement_date
  LEFT JOIN historical_bitcoin_calculations b 
    ON c.settlement_date = b.settlement_date
  GROUP BY c.settlement_date, u.unique_combinations
)
SELECT 
  settlement_date::text as date,
  calculation_count,
  expected_count,
  ROUND((calculation_count * 100.0) / expected_count, 2) as completion_percentage
FROM date_calculations
WHERE calculation_count < expected_count
ORDER BY completion_percentage ASC, settlement_date DESC
LIMIT 30;

-- -----------------------------------------------------------------------------
-- SECTION 3: DETAILED DATE AUDIT
-- -----------------------------------------------------------------------------

-- Audit Bitcoin calculations for a specific date (replace with target date)
WITH period_farm_combinations AS (
  SELECT 
    settlement_date,
    settlement_period,
    farm_id,
    COUNT(*) as record_count
  FROM curtailment_records
  WHERE settlement_date = '2023-12-25' -- REPLACE WITH TARGET DATE
  GROUP BY settlement_date, settlement_period, farm_id
),
bitcoin_calculations AS (
  SELECT 
    settlement_date,
    settlement_period,
    farm_id,
    miner_model,
    COUNT(*) as calculation_count
  FROM historical_bitcoin_calculations
  WHERE settlement_date = '2023-12-25' -- REPLACE WITH TARGET DATE
  GROUP BY settlement_date, settlement_period, farm_id, miner_model
)
SELECT
  pf.settlement_date,
  pf.settlement_period,
  pf.farm_id,
  COALESCE(bc_s19.calculation_count, 0) as s19j_pro_calculations,
  COALESCE(bc_s9.calculation_count, 0) as s9_calculations,
  COALESCE(bc_m20s.calculation_count, 0) as m20s_calculations,
  CASE 
    WHEN COALESCE(bc_s19.calculation_count, 0) = 0 OR 
         COALESCE(bc_s9.calculation_count, 0) = 0 OR 
         COALESCE(bc_m20s.calculation_count, 0) = 0 THEN 'Incomplete'
    ELSE 'Complete'
  END as status
FROM period_farm_combinations pf
LEFT JOIN bitcoin_calculations bc_s19 
  ON pf.settlement_date = bc_s19.settlement_date 
  AND pf.settlement_period = bc_s19.settlement_period 
  AND pf.farm_id = bc_s19.farm_id
  AND bc_s19.miner_model = 'S19J_PRO'
LEFT JOIN bitcoin_calculations bc_s9
  ON pf.settlement_date = bc_s9.settlement_date 
  AND pf.settlement_period = bc_s9.settlement_period 
  AND pf.farm_id = bc_s9.farm_id
  AND bc_s9.miner_model = 'S9'
LEFT JOIN bitcoin_calculations bc_m20s
  ON pf.settlement_date = bc_m20s.settlement_date 
  AND pf.settlement_period = bc_m20s.settlement_period 
  AND pf.farm_id = bc_m20s.farm_id
  AND bc_m20s.miner_model = 'M20S'
ORDER BY status, pf.settlement_period, pf.farm_id;

-- -----------------------------------------------------------------------------
-- SECTION 4: FIX MISSING CALCULATIONS
-- -----------------------------------------------------------------------------

-- Identify specific missing Bitcoin calculations for targeted fixing
WITH period_farm_combinations AS (
  SELECT 
    settlement_date,
    settlement_period,
    farm_id,
    volume,
    COUNT(*) as record_count
  FROM curtailment_records
  WHERE settlement_date = '2023-12-25' -- REPLACE WITH TARGET DATE
  GROUP BY settlement_date, settlement_period, farm_id, volume
),
missing_s19j_pro AS (
  SELECT 
    pf.settlement_date, 
    pf.settlement_period, 
    pf.farm_id,
    pf.volume,
    'S19J_PRO' as miner_model
  FROM period_farm_combinations pf
  LEFT JOIN historical_bitcoin_calculations bc
    ON pf.settlement_date = bc.settlement_date 
    AND pf.settlement_period = bc.settlement_period 
    AND pf.farm_id = bc.farm_id
    AND bc.miner_model = 'S19J_PRO'
  WHERE bc.id IS NULL
),
missing_s9 AS (
  SELECT 
    pf.settlement_date, 
    pf.settlement_period, 
    pf.farm_id,
    pf.volume,
    'S9' as miner_model
  FROM period_farm_combinations pf
  LEFT JOIN historical_bitcoin_calculations bc
    ON pf.settlement_date = bc.settlement_date 
    AND pf.settlement_period = bc.settlement_period 
    AND pf.farm_id = bc.farm_id
    AND bc.miner_model = 'S9'
  WHERE bc.id IS NULL
),
missing_m20s AS (
  SELECT 
    pf.settlement_date, 
    pf.settlement_period, 
    pf.farm_id,
    pf.volume,
    'M20S' as miner_model
  FROM period_farm_combinations pf
  LEFT JOIN historical_bitcoin_calculations bc
    ON pf.settlement_date = bc.settlement_date 
    AND pf.settlement_period = bc.settlement_period 
    AND pf.farm_id = bc.farm_id
    AND bc.miner_model = 'M20S'
  WHERE bc.id IS NULL
)
SELECT * FROM missing_s19j_pro
UNION ALL
SELECT * FROM missing_s9
UNION ALL
SELECT * FROM missing_m20s
ORDER BY settlement_date, settlement_period, farm_id, miner_model;

-- -----------------------------------------------------------------------------
-- SECTION 5: MONTHLY RECONCILIATION SUMMARY
-- -----------------------------------------------------------------------------

-- Get monthly reconciliation status
WITH monthly_curtailment AS (
  SELECT 
    TO_CHAR(settlement_date, 'YYYY-MM') as year_month,
    COUNT(*) as total_records,
    COUNT(DISTINCT (settlement_date || '-' || settlement_period || '-' || farm_id)) as unique_combinations
  FROM curtailment_records
  GROUP BY TO_CHAR(settlement_date, 'YYYY-MM')
),
monthly_bitcoin AS (
  SELECT
    TO_CHAR(settlement_date, 'YYYY-MM') as year_month,
    miner_model,
    COUNT(*) as calculation_count
  FROM historical_bitcoin_calculations
  GROUP BY TO_CHAR(settlement_date, 'YYYY-MM'), miner_model
)
SELECT 
  mc.year_month,
  mc.total_records as curtailment_records,
  mc.unique_combinations,
  mb.miner_model,
  mb.calculation_count,
  mc.unique_combinations * 1 as expected_per_model,
  mc.unique_combinations * 3 as expected_total,
  ROUND((mb.calculation_count * 100.0) / (mc.unique_combinations * 1), 2) as model_percentage,
  ROUND((SUM(mb.calculation_count) OVER (PARTITION BY mc.year_month) * 100.0) / 
    (mc.unique_combinations * 3), 2) as month_percentage
FROM monthly_curtailment mc
LEFT JOIN monthly_bitcoin mb ON mc.year_month = mb.year_month
ORDER BY mc.year_month DESC, mb.miner_model;