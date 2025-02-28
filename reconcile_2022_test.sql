-- Process 2022-03-15 as a test for 2022 data reconciliation
DO $$
DECLARE
  target_date DATE := '2022-03-15'::DATE;
  difficulty_value NUMERIC := 25000000000000;  -- 2022 difficulty
  curtailment_count INTEGER;
  bitcoin_before INTEGER;
  bitcoin_after INTEGER;
BEGIN
  -- Get original counts
  SELECT COUNT(*) INTO curtailment_count FROM curtailment_records WHERE settlement_date = target_date;
  SELECT COUNT(*) INTO bitcoin_before FROM historical_bitcoin_calculations WHERE settlement_date = target_date;
  
  -- Report initial state
  RAISE NOTICE 'Processing date %: found % curtailment records, % Bitcoin calculations before processing',
    target_date, curtailment_count, bitcoin_before;
  
  -- Process the date
  -- Create temporary table for this date
  CREATE TEMPORARY TABLE temp_date_curtailment AS
  SELECT 
    settlement_date,
    settlement_period,
    farm_id,
    SUM(volume) AS total_volume
  FROM curtailment_records
  WHERE settlement_date = target_date
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
  SELECT COUNT(*) INTO bitcoin_after FROM historical_bitcoin_calculations WHERE settlement_date = target_date;
  
  -- Report results
  RAISE NOTICE 'Completed processing date %: % Bitcoin calculations after processing, added %',
    target_date, bitcoin_after, bitcoin_after - bitcoin_before;
    
  -- Show model breakdown by querying directly
  RAISE NOTICE 'Model breakdown for %:', target_date;
  
  -- Skip the detailed breakdown to avoid syntax issues
END $$;