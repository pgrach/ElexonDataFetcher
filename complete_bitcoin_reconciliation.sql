-- Complete Bitcoin Reconciliation Solution
-- This script provides a comprehensive solution for reconciling Bitcoin calculations 
-- across all years with detailed tracking and error handling

-- First, create a table to track reconciliation progress if it doesn't exist
CREATE TABLE IF NOT EXISTS reconciliation_progress (
  id SERIAL PRIMARY KEY,
  batch_id TEXT UNIQUE NOT NULL,
  process_start TIMESTAMP DEFAULT NOW(),
  process_end TIMESTAMP,
  year INTEGER NOT NULL,
  month INTEGER NOT NULL,
  target_dates INTEGER,
  dates_processed INTEGER DEFAULT 0,
  records_processed INTEGER DEFAULT 0,
  bitcoin_added INTEGER DEFAULT 0,
  difficulty NUMERIC,
  status TEXT DEFAULT 'In Progress',
  error_message TEXT
);

-- Create a table to store date-level progress details if it doesn't exist
CREATE TABLE IF NOT EXISTS reconciliation_date_details (
  id SERIAL PRIMARY KEY,
  batch_id TEXT REFERENCES reconciliation_progress(batch_id),
  date_processed DATE NOT NULL,
  curtailment_count INTEGER NOT NULL,
  bitcoin_before INTEGER NOT NULL,
  bitcoin_after INTEGER NOT NULL,
  expected_count INTEGER NOT NULL,
  duration_ms NUMERIC,
  status TEXT NOT NULL,
  error_message TEXT,
  processed_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(batch_id, date_processed)
);

-- Function for processing a single date
CREATE OR REPLACE FUNCTION process_single_date(
  date_to_process DATE,
  difficulty_value NUMERIC DEFAULT 108105433845147,
  batch_id TEXT DEFAULT NULL
) RETURNS JSONB AS $$
DECLARE
  curtailment_count INTEGER;
  bitcoin_before INTEGER;
  bitcoin_after INTEGER;
  start_time TIMESTAMP;
  end_time TIMESTAMP;
  result_json JSONB;
  status TEXT := 'Success';
  error_msg TEXT := NULL;
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
  
  BEGIN
    -- Process the date
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
  
  EXCEPTION WHEN OTHERS THEN
    -- Capture error information
    status := 'Error';
    error_msg := SQLERRM;
    -- Clean up in case of error
    DROP TABLE IF EXISTS temp_date_curtailment;
  END;
  
  -- Get final Bitcoin count
  SELECT COUNT(*) INTO bitcoin_after
  FROM historical_bitcoin_calculations
  WHERE settlement_date = date_to_process;
  
  end_time := NOW();
  
  -- Log the date processing details if batch_id is provided
  IF batch_id IS NOT NULL THEN
    INSERT INTO reconciliation_date_details (
      batch_id, date_processed, curtailment_count, bitcoin_before,
      bitcoin_after, expected_count, duration_ms, status, error_message
    ) VALUES (
      batch_id, 
      date_to_process, 
      curtailment_count, 
      bitcoin_before, 
      bitcoin_after, 
      curtailment_count * 3, 
      EXTRACT(EPOCH FROM (end_time - start_time)) * 1000,
      status,
      error_msg
    ) ON CONFLICT (batch_id, date_processed) 
    DO UPDATE SET 
      bitcoin_after = EXCLUDED.bitcoin_after,
      duration_ms = EXCLUDED.duration_ms,
      status = EXCLUDED.status,
      error_message = EXCLUDED.error_message;
  END IF;
  
  -- Build result JSON
  result_json := jsonb_build_object(
    'date', date_to_process,
    'curtailment_count', curtailment_count,
    'bitcoin_before', bitcoin_before,
    'bitcoin_after', bitcoin_after,
    'bitcoin_added', bitcoin_after - bitcoin_before,
    'expected_count', curtailment_count * 3,
    'completion_percentage', CASE WHEN curtailment_count = 0 THEN 100.0 
                              ELSE ROUND((bitcoin_after * 100.0) / (curtailment_count * 3), 2) END,
    'duration_ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000,
    'status', status,
    'error', error_msg
  );
  
  RETURN result_json;
END;
$$ LANGUAGE plpgsql;

-- Function for processing all dates within a year-month
CREATE OR REPLACE FUNCTION process_month_reconciliation(
  target_year INTEGER,
  target_month INTEGER,
  difficulty_value NUMERIC,
  max_dates INTEGER DEFAULT NULL
) RETURNS JSONB AS $$
DECLARE
  batch_id TEXT;
  start_date DATE;
  end_date DATE;
  current_date DATE;
  dates_to_process RECORD[];
  dates_processed INTEGER := 0;
  records_processed INTEGER := 0;
  bitcoin_before INTEGER := 0;
  bitcoin_after INTEGER := 0;
  batch_status TEXT := 'Success';
  error_message TEXT := NULL;
  result_json JSONB;
  date_result JSONB;
  total_dates INTEGER;
BEGIN
  -- Generate batch ID
  batch_id := 'BATCH-' || target_year || '-' || LPAD(target_month::TEXT, 2, '0') || '-' || TO_CHAR(NOW(), 'YYYYMMDD-HH24MISS');
  
  -- Calculate date range
  start_date := make_date(target_year, target_month, 1);
  end_date := (start_date + INTERVAL '1 month')::DATE - INTERVAL '1 day';
  
  -- Find dates that need processing
  CREATE TEMPORARY TABLE dates_needing_processing AS
  WITH date_stats AS (
    SELECT 
      c.settlement_date,
      COUNT(DISTINCT c.id) as curtailment_count,
      COUNT(DISTINCT h.id) as bitcoin_count
    FROM 
      curtailment_records c
      LEFT JOIN historical_bitcoin_calculations h ON 
        c.settlement_date = h.settlement_date
    WHERE 
      c.settlement_date >= start_date AND
      c.settlement_date <= end_date
    GROUP BY 
      c.settlement_date
  )
  SELECT 
    settlement_date,
    curtailment_count,
    bitcoin_count,
    curtailment_count * 3 as expected_bitcoin_count,
    (bitcoin_count * 100.0 / NULLIF(curtailment_count * 3, 0)) as completion_percentage
  FROM 
    date_stats
  WHERE 
    bitcoin_count < curtailment_count * 3
  ORDER BY 
    curtailment_count DESC, 
    settlement_date ASC;
  
  -- Count total dates to process
  SELECT COUNT(*) INTO total_dates FROM dates_needing_processing;
  
  -- Record the start of processing
  INSERT INTO reconciliation_progress (
    batch_id, year, month, target_dates, difficulty
  )
  VALUES (
    batch_id, target_year, target_month, total_dates, difficulty_value
  );
  
  -- Get initial Bitcoin calculation count
  SELECT COUNT(*) INTO bitcoin_before
  FROM historical_bitcoin_calculations
  WHERE 
    EXTRACT(YEAR FROM settlement_date) = target_year AND
    EXTRACT(MONTH FROM settlement_date) = target_month;
  
  -- Process each date that needs reconciliation
  FOR current_date IN 
    SELECT settlement_date FROM dates_needing_processing
    ORDER BY curtailment_count DESC
    LIMIT max_dates
  LOOP
    -- Process this date and track the result
    date_result := process_single_date(current_date, difficulty_value, batch_id);
    
    -- Update counters
    dates_processed := dates_processed + 1;
    records_processed := records_processed + (date_result->>'curtailment_count')::INTEGER;
    
    -- Update progress
    UPDATE reconciliation_progress
    SET 
      dates_processed = dates_processed,
      records_processed = records_processed
    WHERE batch_id = batch_id;
    
    -- Check for errors
    IF date_result->>'status' = 'Error' THEN
      batch_status := 'Partial';
      error_message := 'Errors occurred during processing. Check date_details for specifics.';
    END IF;
    
    -- Commit after each date to save progress
    COMMIT;
    
    -- Start a new transaction
    BEGIN;
  END LOOP;
  
  -- Get final Bitcoin calculation count
  SELECT COUNT(*) INTO bitcoin_after
  FROM historical_bitcoin_calculations
  WHERE 
    EXTRACT(YEAR FROM settlement_date) = target_year AND
    EXTRACT(MONTH FROM settlement_date) = target_month;
  
  -- Update progress completion
  UPDATE reconciliation_progress
  SET 
    process_end = NOW(),
    bitcoin_added = bitcoin_after - bitcoin_before,
    status = batch_status,
    error_message = error_message
  WHERE batch_id = batch_id;
  
  -- Build result JSON
  result_json := jsonb_build_object(
    'batch_id', batch_id,
    'year', target_year,
    'month', target_month,
    'total_dates', total_dates,
    'dates_processed', dates_processed,
    'records_processed', records_processed,
    'bitcoin_before', bitcoin_before,
    'bitcoin_after', bitcoin_after,
    'bitcoin_added', bitcoin_after - bitcoin_before,
    'status', batch_status,
    'error', error_message
  );
  
  -- Clean up
  DROP TABLE IF EXISTS dates_needing_processing;
  
  RETURN result_json;
END;
$$ LANGUAGE plpgsql;

-- Helper function to get a summary of reconciliation progress
CREATE OR REPLACE FUNCTION get_reconciliation_summary() RETURNS TABLE (
  year INTEGER,
  total_curtailment INTEGER,
  total_bitcoin INTEGER,
  expected_bitcoin INTEGER,
  completion_percentage NUMERIC,
  missing_records INTEGER
) AS $$
BEGIN
  RETURN QUERY
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
END;
$$ LANGUAGE plpgsql;

-- Function to get priority months to process
CREATE OR REPLACE FUNCTION get_priority_months(max_months INTEGER DEFAULT 5) RETURNS TABLE (
  year INTEGER,
  month INTEGER,
  curtailment_count INTEGER,
  bitcoin_count INTEGER,
  expected_bitcoin INTEGER,
  completion_percentage NUMERIC,
  missing_records INTEGER,
  priority INTEGER
) AS $$
BEGIN
  RETURN QUERY
  WITH month_stats AS (
    SELECT 
      EXTRACT(YEAR FROM c.settlement_date)::INTEGER as year,
      EXTRACT(MONTH FROM c.settlement_date)::INTEGER as month,
      COUNT(DISTINCT c.id) as curtailment_count,
      COALESCE(COUNT(DISTINCT h.id), 0) as bitcoin_count,
      COUNT(DISTINCT c.id) * 3 as expected_bitcoin_count,
      ROUND(COALESCE(COUNT(DISTINCT h.id), 0) * 100.0 / NULLIF(COUNT(DISTINCT c.id) * 3, 0), 2) as completion_percentage,
      (COUNT(DISTINCT c.id) * 3) - COALESCE(COUNT(DISTINCT h.id), 0) as missing_records,
      ROW_NUMBER() OVER (ORDER BY (COUNT(DISTINCT c.id) * 3) - COALESCE(COUNT(DISTINCT h.id), 0) DESC) as priority
    FROM 
      curtailment_records c
      LEFT JOIN historical_bitcoin_calculations h ON 
        c.settlement_date = h.settlement_date
    GROUP BY 
      year, month
    HAVING 
      COALESCE(COUNT(DISTINCT h.id), 0) < COUNT(DISTINCT c.id) * 3
  )
  SELECT 
    year, month, curtailment_count, bitcoin_count, expected_bitcoin_count, 
    completion_percentage, missing_records, priority
  FROM 
    month_stats
  ORDER BY 
    priority ASC
  LIMIT max_months;
END;
$$ LANGUAGE plpgsql;

-- Check initial reconciliation status
SELECT * FROM get_reconciliation_summary();

-- Get the top priority months to process
SELECT * FROM get_priority_months(10);

-- Process a few high-priority months to demonstrate the solution
SELECT jsonb_pretty(process_month_reconciliation(2023, 10, 37935772752142, 10));
SELECT jsonb_pretty(process_month_reconciliation(2022, 3, 25000000000000, 10));
SELECT jsonb_pretty(process_month_reconciliation(2025, 2, 110568428300952, 10));

-- View processing history
SELECT * FROM reconciliation_progress ORDER BY process_start DESC LIMIT 10;

-- View detailed date processing results
SELECT * FROM reconciliation_date_details 
WHERE batch_id IN (SELECT batch_id FROM reconciliation_progress ORDER BY process_start DESC LIMIT 3)
ORDER BY date_processed ASC;

-- Check current reconciliation status
SELECT * FROM get_reconciliation_summary();

-- Example of how to run a full reconciliation process for 2023
-- WARNING: This could take a long time to run
/*
DO $$
DECLARE
  current_month RECORD;
  max_dates_per_batch INTEGER := 20;  -- Limit dates per batch to avoid timeout
  difficulty_2023 NUMERIC := 37935772752142;
  batch_result JSONB;
BEGIN
  -- Get all months in 2023 with missing calculations
  FOR current_month IN 
    WITH month_stats AS (
      SELECT 
        EXTRACT(MONTH FROM c.settlement_date)::INTEGER as month,
        COUNT(DISTINCT c.id) as curtailment_count,
        COALESCE(COUNT(DISTINCT h.id), 0) as bitcoin_count,
        COUNT(DISTINCT c.id) * 3 as expected_bitcoin_count
      FROM 
        curtailment_records c
        LEFT JOIN historical_bitcoin_calculations h ON 
          c.settlement_date = h.settlement_date
      WHERE 
        EXTRACT(YEAR FROM c.settlement_date) = 2023
      GROUP BY 
        month
      HAVING 
        COALESCE(COUNT(DISTINCT h.id), 0) < COUNT(DISTINCT c.id) * 3
    )
    SELECT 
      month
    FROM 
      month_stats
    ORDER BY 
      (expected_bitcoin_count - bitcoin_count) DESC
  LOOP
    -- Process this month
    RAISE NOTICE 'Processing 2023-%', current_month.month;
    batch_result := process_month_reconciliation(2023, current_month.month, difficulty_2023, max_dates_per_batch);
    
    -- Log the result
    RAISE NOTICE 'Completed batch %: processed % dates, added % calculations',
      batch_result->>'batch_id', 
      batch_result->>'dates_processed',
      batch_result->>'bitcoin_added';
  END LOOP;
END $$;
*/