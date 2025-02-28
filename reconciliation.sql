-- Reconciliation SQL Queries
-- A collection of useful SQL queries for analyzing and fixing reconciliation issues

-- 1. Overall Reconciliation Status
SELECT
  COUNT(DISTINCT cr.settlement_date) AS total_dates,
  SUM(CASE WHEN hbc_count.count = cr_count.expected THEN 1 ELSE 0 END) AS fully_reconciled_dates,
  ROUND(
    SUM(CASE WHEN hbc_count.count = cr_count.expected THEN 1 ELSE 0 END)::numeric / 
    COUNT(DISTINCT cr.settlement_date) * 100
  , 2) AS reconciliation_percentage
FROM 
  curtailment_records cr
LEFT JOIN (
  SELECT 
    settlement_date, 
    COUNT(DISTINCT (settlement_period || '-' || farm_id)) * 3 AS expected
  FROM 
    curtailment_records
  GROUP BY 
    settlement_date
) cr_count ON cr.settlement_date = cr_count.settlement_date
LEFT JOIN (
  SELECT 
    settlement_date, 
    COUNT(*) AS count
  FROM 
    historical_bitcoin_calculations
  GROUP BY 
    settlement_date
) hbc_count ON cr.settlement_date = hbc_count.settlement_date;

-- 2. Missing Calculations by Date (Top 20)
WITH required_combinations AS (
  SELECT 
    settlement_date,
    COUNT(DISTINCT (settlement_period || '-' || farm_id)) * 3 AS expected_count
  FROM 
    curtailment_records
  GROUP BY 
    settlement_date
),
actual_calculations AS (
  SELECT 
    settlement_date,
    COUNT(*) AS actual_count
  FROM 
    historical_bitcoin_calculations
  GROUP BY 
    settlement_date
)
SELECT 
  rc.settlement_date,
  rc.expected_count,
  COALESCE(ac.actual_count, 0) AS actual_count,
  rc.expected_count - COALESCE(ac.actual_count, 0) AS missing_count,
  CASE 
    WHEN rc.expected_count = 0 THEN 100
    ELSE ROUND((COALESCE(ac.actual_count, 0)::numeric / rc.expected_count) * 100, 2)
  END AS completion_percentage
FROM 
  required_combinations rc
LEFT JOIN 
  actual_calculations ac ON rc.settlement_date = ac.settlement_date
WHERE 
  rc.expected_count > COALESCE(ac.actual_count, 0)
ORDER BY 
  missing_count DESC, settlement_date
LIMIT 20;

-- 3. Missing Calculations by Miner Model
WITH expected_combinations AS (
  SELECT 
    COUNT(DISTINCT cr.settlement_date) * 
    COUNT(DISTINCT cr.settlement_period) * 
    COUNT(DISTINCT cr.farm_id) AS total_expected_combinations,
    'S19J_PRO' AS miner_model
  FROM 
    curtailment_records cr
  UNION ALL
  SELECT 
    COUNT(DISTINCT cr.settlement_date) * 
    COUNT(DISTINCT cr.settlement_period) * 
    COUNT(DISTINCT cr.farm_id) AS total_expected_combinations,
    'S9' AS miner_model
  FROM 
    curtailment_records cr
  UNION ALL
  SELECT 
    COUNT(DISTINCT cr.settlement_date) * 
    COUNT(DISTINCT cr.settlement_period) * 
    COUNT(DISTINCT cr.farm_id) AS total_expected_combinations,
    'M20S' AS miner_model
  FROM 
    curtailment_records cr
)
SELECT 
  ec.miner_model,
  ec.total_expected_combinations AS expected,
  COALESCE(actual.count, 0) AS actual,
  ec.total_expected_combinations - COALESCE(actual.count, 0) AS missing,
  CASE 
    WHEN ec.total_expected_combinations = 0 THEN 100
    ELSE ROUND((COALESCE(actual.count, 0)::numeric / ec.total_expected_combinations) * 100, 2)
  END AS completion_percentage
FROM 
  expected_combinations ec
LEFT JOIN (
  SELECT 
    miner_model,
    COUNT(*) AS count
  FROM 
    historical_bitcoin_calculations
  GROUP BY 
    miner_model
) actual ON ec.miner_model = actual.miner_model
ORDER BY 
  completion_percentage;

-- 4. Find Specific Date-Period-Farm Combinations with Missing Calculations
WITH required_combinations AS (
  SELECT DISTINCT
    cr.settlement_date,
    cr.settlement_period,
    cr.farm_id
  FROM 
    curtailment_records cr
),
existing_calculations AS (
  SELECT DISTINCT
    hbc.settlement_date,
    hbc.settlement_period,
    hbc.farm_id,
    hbc.miner_model
  FROM 
    historical_bitcoin_calculations hbc
)
SELECT 
  rc.settlement_date,
  rc.settlement_period,
  rc.farm_id,
  CASE WHEN s19.settlement_date IS NULL THEN 'Missing' ELSE 'Present' END AS s19j_pro_status,
  CASE WHEN s9.settlement_date IS NULL THEN 'Missing' ELSE 'Present' END AS s9_status,
  CASE WHEN m20s.settlement_date IS NULL THEN 'Missing' ELSE 'Present' END AS m20s_status
FROM 
  required_combinations rc
LEFT JOIN 
  existing_calculations s19 
  ON rc.settlement_date = s19.settlement_date 
  AND rc.settlement_period = s19.settlement_period 
  AND rc.farm_id = s19.farm_id 
  AND s19.miner_model = 'S19J_PRO'
LEFT JOIN 
  existing_calculations s9 
  ON rc.settlement_date = s9.settlement_date 
  AND rc.settlement_period = s9.settlement_period 
  AND rc.farm_id = s9.farm_id 
  AND s9.miner_model = 'S9'
LEFT JOIN 
  existing_calculations m20s 
  ON rc.settlement_date = m20s.settlement_date 
  AND rc.settlement_period = m20s.settlement_period 
  AND rc.farm_id = m20s.farm_id 
  AND m20s.miner_model = 'M20S'
WHERE 
  s19.settlement_date IS NULL OR s9.settlement_date IS NULL OR m20s.settlement_date IS NULL
ORDER BY 
  rc.settlement_date DESC, rc.settlement_period, rc.farm_id
LIMIT 100;

-- 5. Validate Difficulty Data Across Calculations
SELECT 
  settlement_date,
  miner_model,
  COUNT(*) AS record_count,
  MIN(difficulty) AS min_difficulty,
  MAX(difficulty) AS max_difficulty,
  COUNT(DISTINCT difficulty) AS unique_difficulty_count
FROM 
  historical_bitcoin_calculations
GROUP BY 
  settlement_date, miner_model
HAVING 
  COUNT(DISTINCT difficulty) > 1
ORDER BY 
  settlement_date DESC, miner_model;

-- 6. Check for Incorrect Calculation Amounts
WITH expected_values AS (
  SELECT
    settlement_date,
    settlement_period,
    farm_id,
    miner_model,
    AVG(bitcoin_mined) AS avg_bitcoin_mined
  FROM
    historical_bitcoin_calculations
  GROUP BY
    settlement_date,
    settlement_period,
    farm_id,
    miner_model
)
SELECT
  hbc.settlement_date,
  hbc.settlement_period,
  hbc.farm_id,
  hbc.miner_model,
  hbc.bitcoin_mined,
  ev.avg_bitcoin_mined,
  ABS(hbc.bitcoin_mined - ev.avg_bitcoin_mined) AS difference,
  CASE
    WHEN ABS((hbc.bitcoin_mined - ev.avg_bitcoin_mined) / NULLIF(ev.avg_bitcoin_mined, 0)) > 0.5 THEN 'Significant'
    WHEN ABS((hbc.bitcoin_mined - ev.avg_bitcoin_mined) / NULLIF(ev.avg_bitcoin_mined, 0)) > 0.1 THEN 'Moderate'
    ELSE 'Minor'
  END AS difference_category
FROM
  historical_bitcoin_calculations hbc
JOIN
  expected_values ev
  ON hbc.settlement_date = ev.settlement_date
  AND hbc.settlement_period = ev.settlement_period
  AND hbc.farm_id = ev.farm_id
  AND hbc.miner_model = ev.miner_model
WHERE
  ABS((hbc.bitcoin_mined - ev.avg_bitcoin_mined) / NULLIF(ev.avg_bitcoin_mined, 0)) > 0.1
ORDER BY
  difference DESC
LIMIT 100;

-- 7. Fix Missing Monthly Summaries
INSERT INTO bitcoin_monthly_summaries (
  year_month,
  miner_model,
  total_bitcoin_mined,
  average_difficulty,
  total_curtailed_mwh,
  created_at,
  updated_at
)
SELECT
  TO_CHAR(DATE_TRUNC('month', settlement_date::date), 'YYYY-MM') AS year_month,
  miner_model,
  SUM(bitcoin_mined) AS total_bitcoin_mined,
  AVG(difficulty) AS average_difficulty,
  SUM(curtailed_mwh) AS total_curtailed_mwh,
  NOW() AS created_at,
  NOW() AS updated_at
FROM
  historical_bitcoin_calculations
GROUP BY
  TO_CHAR(DATE_TRUNC('month', settlement_date::date), 'YYYY-MM'),
  miner_model
ON CONFLICT (year_month, miner_model) 
DO UPDATE SET
  total_bitcoin_mined = EXCLUDED.total_bitcoin_mined,
  average_difficulty = EXCLUDED.average_difficulty,
  total_curtailed_mwh = EXCLUDED.total_curtailed_mwh,
  updated_at = NOW();

-- 8. Clean up any duplicate records (select only, run with DELETE after verification)
WITH duplicates AS (
  SELECT
    settlement_date,
    settlement_period,
    farm_id,
    miner_model,
    COUNT(*) AS count,
    MAX(id) AS latest_id
  FROM
    historical_bitcoin_calculations
  GROUP BY
    settlement_date,
    settlement_period,
    farm_id,
    miner_model
  HAVING
    COUNT(*) > 1
)
SELECT
  hbc.*
FROM
  historical_bitcoin_calculations hbc
JOIN
  duplicates d
  ON hbc.settlement_date = d.settlement_date
  AND hbc.settlement_period = d.settlement_period
  AND hbc.farm_id = d.farm_id
  AND hbc.miner_model = d.miner_model
  AND hbc.id != d.latest_id;
  
-- 9. Restore from backup if needed (example structure)
-- CREATE TEMPORARY TABLE temp_historical_calculations AS
-- SELECT * FROM historical_bitcoin_calculations_backup
-- WHERE settlement_date = '2025-02-15';
--
-- INSERT INTO historical_bitcoin_calculations
-- SELECT * FROM temp_historical_calculations
-- ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model)
-- DO NOTHING;