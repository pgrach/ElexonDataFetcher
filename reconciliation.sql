-- Reconciliation SQL Queries
-- These queries help identify and analyze missing calculations between tables

-- 1. Overall reconciliation status
WITH curtailment_summary AS (
  SELECT 
    COUNT(DISTINCT (settlement_date || '-' || settlement_period || '-' || farm_id)) * 3 as expected_count
  FROM curtailment_records
  WHERE volume::numeric != 0
),
bitcoin_summary AS (
  SELECT 
    COUNT(*) as actual_count
  FROM historical_bitcoin_calculations
)
SELECT 
  cs.expected_count,
  bs.actual_count,
  ROUND((bs.actual_count * 100.0) / cs.expected_count, 2) as completion_percentage,
  cs.expected_count - bs.actual_count as missing_count
FROM curtailment_summary cs, bitcoin_summary bs;

-- 2. Find top dates with missing calculations
WITH curtailment_by_date AS (
  SELECT 
    settlement_date,
    COUNT(DISTINCT (settlement_period || '-' || farm_id)) * 3 as expected_count
  FROM curtailment_records
  WHERE volume::numeric != 0
  GROUP BY settlement_date
),
bitcoin_by_date AS (
  SELECT 
    settlement_date,
    COUNT(*) as actual_count
  FROM historical_bitcoin_calculations
  GROUP BY settlement_date
)
SELECT 
  cd.settlement_date as date,
  cd.expected_count,
  COALESCE(bd.actual_count, 0) as actual_count,
  cd.expected_count - COALESCE(bd.actual_count, 0) as missing_count,
  CASE 
    WHEN cd.expected_count = 0 THEN 100
    ELSE ROUND((COALESCE(bd.actual_count, 0) * 100.0) / cd.expected_count, 2)
  END as completion_percentage
FROM curtailment_by_date cd
LEFT JOIN bitcoin_by_date bd ON cd.settlement_date = bd.settlement_date
WHERE cd.expected_count > 0
  AND (cd.expected_count - COALESCE(bd.actual_count, 0)) > 0
ORDER BY missing_count DESC
LIMIT 20;

-- 3. Find missing calculations for a specific date
WITH curtailment_data AS (
  SELECT DISTINCT
    settlement_date,
    settlement_period,
    farm_id
  FROM curtailment_records
  WHERE settlement_date = '2022-10-06'
    AND volume::numeric != 0
),
bitcoin_data AS (
  SELECT DISTINCT
    settlement_period,
    farm_id,
    miner_model
  FROM historical_bitcoin_calculations
  WHERE settlement_date = '2022-10-06'
),
missing AS (
  SELECT
    cd.settlement_period,
    cd.farm_id,
    ARRAY(
      SELECT unnest(ARRAY['S19J_PRO', 'S9', 'M20S'])
      EXCEPT
      SELECT bd.miner_model
      FROM bitcoin_data bd
      WHERE bd.settlement_period = cd.settlement_period
        AND bd.farm_id = cd.farm_id
    ) as missing_models
  FROM curtailment_data cd
)
SELECT
  settlement_period,
  farm_id,
  missing_models
FROM missing
WHERE array_length(missing_models, 1) > 0
ORDER BY settlement_period, farm_id;

-- 4. Find reconciliation status by month
WITH date_parts AS (
  SELECT 
    settlement_date,
    TO_CHAR(settlement_date::date, 'YYYY-MM') as year_month
  FROM curtailment_records
  GROUP BY settlement_date
),
curtailment_by_month AS (
  SELECT 
    dp.year_month,
    COUNT(DISTINCT (cr.settlement_date || '-' || cr.settlement_period || '-' || cr.farm_id)) * 3 as expected_count
  FROM curtailment_records cr
  JOIN date_parts dp ON cr.settlement_date = dp.settlement_date
  WHERE cr.volume::numeric != 0
  GROUP BY dp.year_month
),
bitcoin_by_month AS (
  SELECT 
    TO_CHAR(settlement_date::date, 'YYYY-MM') as year_month,
    COUNT(*) as actual_count
  FROM historical_bitcoin_calculations
  GROUP BY TO_CHAR(settlement_date::date, 'YYYY-MM')
)
SELECT 
  cbm.year_month,
  cbm.expected_count,
  COALESCE(bbm.actual_count, 0) as actual_count,
  cbm.expected_count - COALESCE(bbm.actual_count, 0) as missing_count,
  CASE 
    WHEN cbm.expected_count = 0 THEN 100
    ELSE ROUND((COALESCE(bbm.actual_count, 0) * 100.0) / cbm.expected_count, 2)
  END as completion_percentage
FROM curtailment_by_month cbm
LEFT JOIN bitcoin_by_month bbm ON cbm.year_month = bbm.year_month
ORDER BY cbm.year_month;

-- 5. Check for dates with no curtailment records but having Bitcoin calculations
WITH bitcoin_dates AS (
  SELECT DISTINCT settlement_date
  FROM historical_bitcoin_calculations
),
curtailment_dates AS (
  SELECT DISTINCT settlement_date
  FROM curtailment_records
)
SELECT 
  bd.settlement_date
FROM bitcoin_dates bd
LEFT JOIN curtailment_dates cd ON bd.settlement_date = cd.settlement_date
WHERE cd.settlement_date IS NULL;

-- 6. Identify records with inconsistent difficulty values for the same date
WITH difficulty_values AS (
  SELECT 
    settlement_date,
    difficulty::numeric as difficulty,
    COUNT(*) as record_count
  FROM historical_bitcoin_calculations
  GROUP BY settlement_date, difficulty::numeric
)
SELECT 
  settlement_date,
  json_agg(json_build_object('difficulty', difficulty, 'count', record_count)) as difficulties
FROM difficulty_values
GROUP BY settlement_date
HAVING COUNT(*) > 1
ORDER BY settlement_date;

-- 7. Check for potential data inconsistencies in calculations
WITH miner_stats AS (
  SELECT 
    miner_model,
    COUNT(*) as calculations,
    ROUND(AVG(bitcoin_mined::numeric), 8) as avg_bitcoin,
    MIN(bitcoin_mined::numeric) as min_bitcoin,
    MAX(bitcoin_mined::numeric) as max_bitcoin
  FROM historical_bitcoin_calculations
  GROUP BY miner_model
)
SELECT 
  miner_model,
  calculations,
  avg_bitcoin,
  min_bitcoin,
  max_bitcoin
FROM miner_stats
ORDER BY calculations DESC;

-- 8. Query to analyze farm distribution in curtailment records
SELECT 
  farm_id,
  COUNT(*) as record_count,
  COUNT(DISTINCT settlement_date) as unique_dates
FROM curtailment_records
GROUP BY farm_id
ORDER BY record_count DESC
LIMIT 20;

-- 9. Index check for performance optimization
SELECT
  schemaname, 
  tablename, 
  indexname, 
  indexdef
FROM pg_indexes
WHERE tablename IN ('curtailment_records', 'historical_bitcoin_calculations')
ORDER BY tablename, indexname;

-- 10. Find the most resource-intensive periods to reconcile
WITH period_counts AS (
  SELECT 
    settlement_date,
    settlement_period,
    COUNT(*) as records
  FROM curtailment_records
  GROUP BY settlement_date, settlement_period
  ORDER BY records DESC
)
SELECT * FROM period_counts
LIMIT 20;