-- Fix Bitcoin calculations for a single date
-- This script will generate missing Bitcoin calculations for 2023-01-15

-- Create temporary table for processing
CREATE TEMPORARY TABLE temp_curtailment_groups AS
SELECT 
  settlement_date,
  settlement_period,
  farm_id,
  SUM(volume) AS total_volume
FROM curtailment_records
WHERE settlement_date = '2023-01-15'
GROUP BY settlement_date, settlement_period, farm_id;

-- Check what we're about to process
SELECT 
  COUNT(*) as record_count,
  COUNT(*) * 3 as expected_calculation_count
FROM temp_curtailment_groups;

-- Get initial count of Bitcoin calculations
SELECT COUNT(*) as initial_bitcoin_count
FROM historical_bitcoin_calculations
WHERE settlement_date = '2023-01-15';

-- Define difficulty value for 2023
DO $$
DECLARE
  difficulty_value NUMERIC := 37935772752142;
BEGIN
  -- Insert calculations for S19J_PRO
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
  FROM temp_curtailment_groups
  WHERE ABS(total_volume) > 0
  ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
  DO UPDATE SET 
    bitcoin_mined = EXCLUDED.bitcoin_mined,
    calculated_at = EXCLUDED.calculated_at,
    difficulty = EXCLUDED.difficulty;
    
  -- Insert calculations for S9
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
  FROM temp_curtailment_groups
  WHERE ABS(total_volume) > 0
  ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
  DO UPDATE SET 
    bitcoin_mined = EXCLUDED.bitcoin_mined,
    calculated_at = EXCLUDED.calculated_at,
    difficulty = EXCLUDED.difficulty;
    
  -- Insert calculations for M20S
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
  FROM temp_curtailment_groups
  WHERE ABS(total_volume) > 0
  ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
  DO UPDATE SET 
    bitcoin_mined = EXCLUDED.bitcoin_mined,
    calculated_at = EXCLUDED.calculated_at,
    difficulty = EXCLUDED.difficulty;
  
  RAISE NOTICE 'Processed 2023-01-15 Bitcoin calculations';
END $$;

-- Get final count of Bitcoin calculations
SELECT COUNT(*) as final_bitcoin_count
FROM historical_bitcoin_calculations
WHERE settlement_date = '2023-01-15';

-- Show how many were added
SELECT 
  (SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE settlement_date = '2023-01-15') - 
  (SELECT COUNT(*) FROM temp_curtailment_groups) * 3 as expected_vs_actual_diff;

-- Cleanup
DROP TABLE temp_curtailment_groups;