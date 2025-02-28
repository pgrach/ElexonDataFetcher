-- Reconciliation Queries for Bitcoin Mining Analytics Platform
-- These queries help administrators and data analysts verify data consistency 
-- between curtailment_records and historical_bitcoin_calculations tables.

-- ==========================================
-- 1. Overall Reconciliation Status
-- ==========================================

-- Get overall reconciliation status across all dates
WITH curtailment_summary AS (
  SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT (settlement_date || '-' || settlement_period || '-' || farm_id)) as unique_combinations,
    COUNT(DISTINCT settlement_date) as unique_dates
  FROM curtailment_records
),
bitcoin_summary AS (
  SELECT
    miner_model,
    COUNT(*) as calculation_count,
    COUNT(DISTINCT settlement_date) as date_count
  FROM historical_bitcoin_calculations
  GROUP BY miner_model
),
aggregated_bitcoin AS (
  SELECT
    SUM(calculation_count) as total_calculations,
    COUNT(DISTINCT miner_model) as model_count
  FROM bitcoin_summary
)
SELECT 
  cs.total_records as curtailment_records,
  cs.unique_combinations as unique_combinations,
  cs.unique_dates as unique_dates,
  COALESCE(ab.total_calculations, 0) as total_bitcoin_calculations,
  cs.unique_combinations * 3 as expected_calculations,
  ROUND(COALESCE(ab.total_calculations, 0) * 100.0 / NULLIF(cs.unique_combinations * 3, 0), 2) as reconciliation_percentage
FROM curtailment_summary cs
LEFT JOIN aggregated_bitcoin ab ON true;

-- ==========================================
-- 2. Reconciliation Status By Miner Model
-- ==========================================

-- Get reconciliation status broken down by miner model
WITH curtailment_summary AS (
  SELECT 
    COUNT(DISTINCT (settlement_date || '-' || settlement_period || '-' || farm_id)) as unique_combinations
  FROM curtailment_records
),
bitcoin_by_model AS (
  SELECT
    miner_model,
    COUNT(*) as calculation_count
  FROM historical_bitcoin_calculations
  GROUP BY miner_model
)
SELECT 
  bm.miner_model,
  bm.calculation_count,
  cs.unique_combinations as expected_per_model,
  ROUND(bm.calculation_count * 100.0 / NULLIF(cs.unique_combinations, 0), 2) as model_percentage
FROM bitcoin_by_model bm
CROSS JOIN curtailment_summary cs
ORDER BY bm.miner_model;

-- ==========================================
-- 3. Dates With Missing Calculations
-- ==========================================

-- Find dates with missing or incomplete Bitcoin calculations
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
    u.unique_combinations * 3 as expected_count
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
  ROUND((calculation_count * 100.0) / NULLIF(expected_count, 0), 2) as completion_percentage
FROM date_calculations
WHERE calculation_count < expected_count
ORDER BY completion_percentage ASC, settlement_date DESC
LIMIT 30;

-- ==========================================
-- 4. Missing Calculations For A Specific Date
-- ==========================================

-- Replace '2025-02-28' with the date you want to analyze
WITH curtailment_combos AS (
  SELECT DISTINCT
    settlement_period,
    farm_id
  FROM curtailment_records
  WHERE settlement_date = '2025-02-28'
),
bitcoin_combos AS (
  SELECT DISTINCT
    miner_model,
    settlement_period,
    farm_id
  FROM historical_bitcoin_calculations
  WHERE settlement_date = '2025-02-28'
),
missing_by_model AS (
  SELECT
    m.miner_model,
    c.settlement_period,
    c.farm_id
  FROM curtailment_combos c
  CROSS JOIN (SELECT unnest(ARRAY['S19J_PRO', 'S9', 'M20S']) as miner_model) m
  EXCEPT
  SELECT
    miner_model,
    settlement_period,
    farm_id
  FROM bitcoin_combos
)
SELECT
  miner_model,
  settlement_period,
  farm_id,
  COUNT(*) OVER (PARTITION BY miner_model) as model_missing_count
FROM missing_by_model
ORDER BY miner_model, settlement_period, farm_id;

-- ==========================================
-- 5. Reconciliation Status For Date Range
-- ==========================================

-- Replace '2025-01-01' and '2025-02-28' with your desired date range
WITH date_range_curtailment AS (
  SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT (settlement_date || '-' || settlement_period || '-' || farm_id)) as unique_combinations,
    COUNT(DISTINCT settlement_date) as unique_dates
  FROM curtailment_records
  WHERE settlement_date BETWEEN '2025-01-01' AND '2025-02-28'
),
date_range_bitcoin AS (
  SELECT
    miner_model,
    COUNT(*) as calculation_count,
    COUNT(DISTINCT settlement_date) as date_count
  FROM historical_bitcoin_calculations
  WHERE settlement_date BETWEEN '2025-01-01' AND '2025-02-28'
  GROUP BY miner_model
),
range_bitcoin_agg AS (
  SELECT
    SUM(calculation_count) as total_calculations,
    COUNT(DISTINCT miner_model) as model_count
  FROM date_range_bitcoin
)
SELECT 
  drc.total_records as curtailment_records,
  drc.unique_combinations as unique_combinations,
  drc.unique_dates as unique_dates,
  COALESCE(rba.total_calculations, 0) as total_bitcoin_calculations,
  drc.unique_combinations * 3 as expected_calculations,
  ROUND(COALESCE(rba.total_calculations, 0) * 100.0 / NULLIF(drc.unique_combinations * 3, 0), 2) as reconciliation_percentage
FROM date_range_curtailment drc
LEFT JOIN range_bitcoin_agg rba ON true;

-- ==========================================
-- 6. Find Curtailment Records Without Bitcoin Calculations
-- ==========================================

-- Find specific curtailment records that don't have corresponding calculations
WITH curtailment_data AS (
  SELECT
    settlement_date,
    settlement_period,
    farm_id,
    SUM(volume::numeric) as total_volume
  FROM curtailment_records
  GROUP BY settlement_date, settlement_period, farm_id
),
missing_calculations AS (
  SELECT
    cd.settlement_date,
    cd.settlement_period,
    cd.farm_id,
    cd.total_volume,
    COUNT(bc.id) as calculation_count
  FROM curtailment_data cd
  LEFT JOIN historical_bitcoin_calculations bc ON
    cd.settlement_date = bc.settlement_date AND
    cd.settlement_period = bc.settlement_period AND
    cd.farm_id = bc.farm_id
  GROUP BY cd.settlement_date, cd.settlement_period, cd.farm_id, cd.total_volume
)
SELECT
  settlement_date,
  settlement_period,
  farm_id,
  total_volume,
  calculation_count,
  CASE 
    WHEN calculation_count = 0 THEN 'No calculations'
    WHEN calculation_count < 3 THEN 'Incomplete calculations'
    ELSE 'Complete'
  END as status
FROM missing_calculations
WHERE calculation_count < 3
ORDER BY settlement_date DESC, settlement_period, farm_id
LIMIT 100;

-- ==========================================
-- 7. Data Quality Check
-- ==========================================

-- Check for unusual values or potential data issues
SELECT
  MIN(settlement_date) as earliest_date,
  MAX(settlement_date) as latest_date,
  COUNT(DISTINCT settlement_date) as unique_dates,
  COUNT(DISTINCT farm_id) as unique_farms,
  MIN(volume::numeric) as min_volume,
  MAX(volume::numeric) as max_volume,
  ROUND(AVG(volume::numeric), 2) as avg_volume
FROM curtailment_records;

SELECT
  MIN(settlement_date) as earliest_date,
  MAX(settlement_date) as latest_date,
  COUNT(DISTINCT settlement_date) as unique_dates,
  COUNT(DISTINCT farm_id) as unique_farms,
  MIN(bitcoin_mined::numeric) as min_bitcoin,
  MAX(bitcoin_mined::numeric) as max_bitcoin,
  ROUND(AVG(bitcoin_mined::numeric), 6) as avg_bitcoin,
  MIN(difficulty::numeric) as min_difficulty,
  MAX(difficulty::numeric) as max_difficulty
FROM historical_bitcoin_calculations;