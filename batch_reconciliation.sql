-- Batch Reconciliation Script
-- This script processes dates in batches to reconcile curtailment_records with historical_bitcoin_calculations

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

-- Create a temporary table to track progress
CREATE TEMPORARY TABLE reconciliation_progress (
  batch_id TEXT PRIMARY KEY,
  process_start TIMESTAMP DEFAULT NOW(),
  process_end TIMESTAMP,
  year INTEGER,
  month INTEGER,
  days_processed INTEGER DEFAULT 0,
  records_processed INTEGER DEFAULT 0,
  bitcoin_added INTEGER DEFAULT 0,
  status TEXT DEFAULT 'In Progress'
);

-- Define the reconciliation batch function for a specific month
CREATE OR REPLACE FUNCTION process_month_reconciliation(
  target_year INTEGER,
  target_month INTEGER,
  difficulty_value NUMERIC,
  max_days INTEGER DEFAULT NULL
) RETURNS JSONB AS $$
DECLARE
  batch_id TEXT;
  start_date DATE;
  end_date DATE;
  current_date DATE;
  days_processed INTEGER := 0;
  records_processed INTEGER := 0;
  bitcoin_before INTEGER;
  bitcoin_after INTEGER;
  bitcoin_added INTEGER := 0;
  batch_status TEXT := 'Success';
  result_json JSONB;
BEGIN
  -- Generate batch ID
  batch_id := 'BATCH-' || target_year || '-' || LPAD(target_month::TEXT, 2, '0') || '-' || NOW()::TEXT;
  
  -- Calculate date range
  start_date := make_date(target_year, target_month, 1);
  end_date := (start_date + INTERVAL '1 month')::DATE - INTERVAL '1 day';
  
  -- Record the start of processing
  INSERT INTO reconciliation_progress (batch_id, year, month)
  VALUES (batch_id, target_year, target_month);
  
  -- Get initial Bitcoin calculation count
  SELECT COUNT(*) INTO bitcoin_before
  FROM historical_bitcoin_calculations
  WHERE 
    EXTRACT(YEAR FROM settlement_date) = target_year AND
    EXTRACT(MONTH FROM settlement_date) = target_month;
  
  -- Process each date in the month
  FOR current_date IN 
    SELECT DISTINCT settlement_date
    FROM curtailment_records
    WHERE 
      settlement_date >= start_date AND
      settlement_date <= end_date
    ORDER BY settlement_date
  LOOP
    -- Exit if we've reached the maximum days limit
    IF max_days IS NOT NULL AND days_processed >= max_days THEN
      EXIT;
    END IF;
    
    -- Process this date
    BEGIN
      -- Create temporary table for this date
      CREATE TEMPORARY TABLE temp_date_curtailment AS
      SELECT 
        settlement_date,
        settlement_period,
        farm_id,
        SUM(volume) AS total_volume
      FROM curtailment_records
      WHERE settlement_date = current_date
      GROUP BY settlement_date, settlement_period, farm_id;
      
      -- Count records for this date
      SELECT COUNT(*) INTO records_processed
      FROM temp_date_curtailment;
      
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
      
      -- Clean up temporary table
      DROP TABLE temp_date_curtailment;
      
      -- Increment counters
      days_processed := days_processed + 1;
      
      -- Update progress
      UPDATE reconciliation_progress
      SET 
        days_processed = days_processed,
        records_processed = records_processed + records_processed
      WHERE batch_id = batch_id;
      
      -- Commit after each date
      COMMIT;
      
    EXCEPTION WHEN OTHERS THEN
      -- Log error but continue with other dates
      RAISE WARNING 'Error processing date %: %', current_date, SQLERRM;
      batch_status := 'Partial';
      
      -- Clean up in case of error
      DROP TABLE IF EXISTS temp_date_curtailment;
      
      -- Commit the error handling
      COMMIT;
    END;
    
    -- Start a new transaction
    BEGIN;
  END LOOP;
  
  -- Get final Bitcoin calculation count
  SELECT COUNT(*) INTO bitcoin_after
  FROM historical_bitcoin_calculations
  WHERE 
    EXTRACT(YEAR FROM settlement_date) = target_year AND
    EXTRACT(MONTH FROM settlement_date) = target_month;
  
  bitcoin_added := bitcoin_after - bitcoin_before;
  
  -- Update progress completion
  UPDATE reconciliation_progress
  SET 
    process_end = NOW(),
    bitcoin_added = bitcoin_added,
    status = batch_status
  WHERE batch_id = batch_id;
  
  -- Build result JSON
  result_json := jsonb_build_object(
    'batch_id', batch_id,
    'year', target_year,
    'month', target_month,
    'days_processed', days_processed,
    'records_processed', records_processed,
    'bitcoin_before', bitcoin_before,
    'bitcoin_after', bitcoin_after,
    'bitcoin_added', bitcoin_added,
    'status', batch_status
  );
  
  RETURN result_json;
END;
$$ LANGUAGE plpgsql;

-- Test the function with 2023-10 (highest priority month in 2023)
SELECT jsonb_pretty(process_month_reconciliation(2023, 10, 37935772752142, 2));

-- Test the function with 2022-03 (a month from 2022)
SELECT jsonb_pretty(process_month_reconciliation(2022, 3, 25000000000000, 2));

-- Test the function with 2025-02 (current month)
SELECT jsonb_pretty(process_month_reconciliation(2025, 2, 108105433845147, 1));

-- Display progress results
SELECT * FROM reconciliation_progress;

-- Check the current reconciliation status again to see improvements
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