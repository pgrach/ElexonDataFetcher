-- Quick Test Reconciliation Script for Bitcoin Mining Calculations
-- This script processes 2023-10-15 with known curtailment records but missing Bitcoin calculations

-- First, check the current reconciliation status
WITH years AS (
  SELECT DISTINCT EXTRACT(YEAR FROM settlement_date)::INTEGER as year
  FROM curtailment_records
  ORDER BY year
),
year_stats AS (
  SELECT 
    y.year,
    (SELECT COUNT(*) FROM curtailment_records WHERE EXTRACT(YEAR FROM settlement_date) = y.year) as curtailment_count,
    (SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE EXTRACT(YEAR FROM settlement_date) = y.year) as bitcoin_count,
    (SELECT COUNT(*) FROM curtailment_records WHERE EXTRACT(YEAR FROM settlement_date) = y.year) * 3 as expected_bitcoin_count
  FROM years y
)
SELECT 
  year,
  curtailment_count,
  bitcoin_count,
  expected_bitcoin_count,
  ROUND(bitcoin_count * 100.0 / NULLIF(expected_bitcoin_count, 0), 2) as completion_percentage,
  expected_bitcoin_count - bitcoin_count as missing_records
FROM year_stats
ORDER BY completion_percentage ASC, year ASC;

-- Get detailed information about the selected test date
SELECT 
  '2023-10-15'::TEXT as date,
  (SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = '2023-10-15'::DATE) as curtailment_count,
  (SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE settlement_date = '2023-10-15'::DATE) as bitcoin_count,
  (SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = '2023-10-15'::DATE) * 3 as expected_bitcoin_count,
  ROUND((SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE settlement_date = '2023-10-15'::DATE) * 100.0 / 
        ((SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = '2023-10-15'::DATE) * 3), 2) as completion_percentage,
  (SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = '2023-10-15'::DATE) * 3 -
  (SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE settlement_date = '2023-10-15'::DATE) as missing_records;

-- Process specific date 2023-10-15
DO $$
DECLARE
  difficulty_value NUMERIC := 37935772752142;  -- 2023 difficulty
  curtailment_count INTEGER;
  bitcoin_before INTEGER;
  bitcoin_after INTEGER;
BEGIN
  -- Get original counts
  SELECT COUNT(*) INTO curtailment_count FROM curtailment_records WHERE settlement_date = '2023-10-15';
  SELECT COUNT(*) INTO bitcoin_before FROM historical_bitcoin_calculations WHERE settlement_date = '2023-10-15';
  
  -- Process the date
  -- Create temporary table for this date
  CREATE TEMPORARY TABLE temp_date_curtailment AS
  SELECT 
    settlement_date,
    settlement_period,
    farm_id,
    SUM(volume) AS total_volume
  FROM curtailment_records
  WHERE settlement_date = '2023-10-15'
  GROUP BY settlement_date, settlement_period, farm_id;
  
  -- Insert S19J_PRO calculations
  INSERT INTO historical_bitcoin_calculations (
    settlement_date, settlement_period, farm_id, miner_model,
    bitcoin_mined, calculated_at, difficulty
  )
  SELECT
    settlement_date,
    settlement_period,
    farm_id,
    'S19J_PRO',
    ABS(total_volume) * 0.00021 * (50000000000000 / difficulty_value),
    NOW(),
    difficulty_value
  FROM temp_date_curtailment
  WHERE ABS(total_volume) > 0
  ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
  DO UPDATE SET 
    bitcoin_mined = EXCLUDED.bitcoin_mined,
    calculated_at = EXCLUDED.calculated_at,
    difficulty = EXCLUDED.difficulty;
    
  -- Insert S9 calculations
  INSERT INTO historical_bitcoin_calculations (
    settlement_date, settlement_period, farm_id, miner_model,
    bitcoin_mined, calculated_at, difficulty
  )
  SELECT
    settlement_date,
    settlement_period,
    farm_id,
    'S9',
    ABS(total_volume) * 0.00011 * (50000000000000 / difficulty_value),
    NOW(),
    difficulty_value
  FROM temp_date_curtailment
  WHERE ABS(total_volume) > 0
  ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
  DO UPDATE SET 
    bitcoin_mined = EXCLUDED.bitcoin_mined,
    calculated_at = EXCLUDED.calculated_at,
    difficulty = EXCLUDED.difficulty;
    
  -- Insert M20S calculations
  INSERT INTO historical_bitcoin_calculations (
    settlement_date, settlement_period, farm_id, miner_model,
    bitcoin_mined, calculated_at, difficulty
  )
  SELECT
    settlement_date,
    settlement_period,
    farm_id,
    'M20S',
    ABS(total_volume) * 0.00016 * (50000000000000 / difficulty_value),
    NOW(),
    difficulty_value
  FROM temp_date_curtailment
  WHERE ABS(total_volume) > 0
  ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
  DO UPDATE SET 
    bitcoin_mined = EXCLUDED.bitcoin_mined,
    calculated_at = EXCLUDED.calculated_at,
    difficulty = EXCLUDED.difficulty;
  
  -- Drop temporary table
  DROP TABLE temp_date_curtailment;
  
  -- Get final Bitcoin count
  SELECT COUNT(*) INTO bitcoin_after FROM historical_bitcoin_calculations WHERE settlement_date = '2023-10-15';
  
  -- Report results
  RAISE NOTICE 'Processed date 2023-10-15: % curtailment records, % Bitcoin calculations before, % after, added %',
    curtailment_count, bitcoin_before, bitcoin_after, bitcoin_after - bitcoin_before;
END $$;

-- Now check the status again for verification
WITH years AS (
  SELECT DISTINCT EXTRACT(YEAR FROM settlement_date)::INTEGER as year
  FROM curtailment_records
  ORDER BY year
),
year_stats AS (
  SELECT 
    y.year,
    (SELECT COUNT(*) FROM curtailment_records WHERE EXTRACT(YEAR FROM settlement_date) = y.year) as curtailment_count,
    (SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE EXTRACT(YEAR FROM settlement_date) = y.year) as bitcoin_count,
    (SELECT COUNT(*) FROM curtailment_records WHERE EXTRACT(YEAR FROM settlement_date) = y.year) * 3 as expected_bitcoin_count
  FROM years y
)
SELECT 
  year,
  curtailment_count,
  bitcoin_count,
  expected_bitcoin_count,
  ROUND(bitcoin_count * 100.0 / NULLIF(expected_bitcoin_count, 0), 2) as completion_percentage,
  expected_bitcoin_count - bitcoin_count as missing_records
FROM year_stats
ORDER BY completion_percentage ASC, year ASC;

-- Get detailed information about the selected test date after processing
SELECT 
  '2023-10-15' as date,
  (SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = '2023-10-15') as curtailment_count,
  (SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE settlement_date = '2023-10-15') as bitcoin_count,
  (SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = '2023-10-15') * 3 as expected_bitcoin_count,
  ROUND((SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE settlement_date = '2023-10-15') * 100.0 / 
        (SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = '2023-10-15' * 3), 2) as completion_percentage,
  (SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = '2023-10-15') * 3 -
  (SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE settlement_date = '2023-10-15') as missing_records;