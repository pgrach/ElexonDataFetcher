-- Simple Reconciliation Script
-- This script uses a more streamlined approach to reconcile a small batch of dates

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
  ROUND(bitcoin_count * 100.0 / expected_bitcoin_count, 2) as completion_percentage,
  expected_bitcoin_count - bitcoin_count as missing_records
FROM year_stats
ORDER BY completion_percentage ASC, year ASC;

-- Function for processing a single date
CREATE OR REPLACE FUNCTION process_single_date(
  date_to_process DATE,
  difficulty_value NUMERIC DEFAULT 108105433845147
) RETURNS JSONB AS $$
DECLARE
  curtailment_count INTEGER;
  bitcoin_before INTEGER;
  bitcoin_after INTEGER;
  start_time TIMESTAMP;
  end_time TIMESTAMP;
  result_json JSONB;
BEGIN
  start_time := NOW();
  
  -- Count records for this date
  SELECT COUNT(*) INTO curtailment_count
  FROM curtailment_records
  WHERE settlement_date = date_to_process;
  
  -- Get initial Bitcoin count
  SELECT COUNT(*) INTO bitcoin_before
  FROM historical_bitcoin_calculations
  WHERE settlement_date = date_to_process;
  
  -- Create temporary table for this date
  CREATE TEMPORARY TABLE IF NOT EXISTS temp_date_curtailment AS
  SELECT 
    settlement_date,
    settlement_period,
    farm_id,
    SUM(volume) AS total_volume
  FROM curtailment_records
  WHERE settlement_date = date_to_process
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
  DROP TABLE IF EXISTS temp_date_curtailment;
  
  -- Get final Bitcoin count
  SELECT COUNT(*) INTO bitcoin_after
  FROM historical_bitcoin_calculations
  WHERE settlement_date = date_to_process;
  
  end_time := NOW();
  
  -- Build result JSON
  result_json := jsonb_build_object(
    'date', date_to_process,
    'curtailment_count', curtailment_count,
    'bitcoin_before', bitcoin_before,
    'bitcoin_after', bitcoin_after,
    'bitcoin_added', bitcoin_after - bitcoin_before,
    'expected_count', curtailment_count * 3,
    'completion_percentage', ROUND((bitcoin_after * 100.0) / (curtailment_count * 3), 2),
    'duration_ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000
  );
  
  RETURN result_json;
END;
$$ LANGUAGE plpgsql;

-- Process a sample date from 2023
SELECT jsonb_pretty(process_single_date('2023-10-15', 37935772752142));

-- Process a sample date from 2022
SELECT jsonb_pretty(process_single_date('2022-05-01', 25000000000000));

-- Process today's date from 2025
SELECT jsonb_pretty(process_single_date('2025-02-28', 110568428300952));

-- Check final reconciliation status
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
  ROUND(bitcoin_count * 100.0 / expected_bitcoin_count, 2) as completion_percentage,
  expected_bitcoin_count - bitcoin_count as missing_records
FROM year_stats
ORDER BY completion_percentage ASC, year ASC;