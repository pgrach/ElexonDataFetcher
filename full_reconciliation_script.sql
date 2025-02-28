-- Full Reconciliation Script
-- This script implements a complete solution to ensure 100% reconciliation between
-- curtailment_records and historical_bitcoin_calculations

-- Create a progress tracking table if it doesn't exist
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_tables WHERE tablename = 'reconciliation_tracking') THEN
    CREATE TABLE reconciliation_tracking (
      id SERIAL PRIMARY KEY,
      batch_id TEXT,
      year_value INTEGER,
      year_month TEXT,
      process_date DATE,
      process_start TIMESTAMP DEFAULT NOW(),
      process_end TIMESTAMP,
      curtailment_count INTEGER,
      initial_bitcoin_count INTEGER,
      final_bitcoin_count INTEGER,
      records_added INTEGER,
      status TEXT,
      error_message TEXT
    );
  END IF;
END;
$$;

-- Function to process a single date
CREATE OR REPLACE FUNCTION process_single_date(
  date_to_process DATE,
  difficulty_value NUMERIC,
  batch_id TEXT DEFAULT NULL
) RETURNS JSONB AS $$
DECLARE
  start_time TIMESTAMP;
  end_time TIMESTAMP;
  curtailment_count INTEGER;
  bitcoin_before INTEGER;
  bitcoin_after INTEGER;
  records_added INTEGER;
  year_month TEXT;
  result_status TEXT := 'Success';
  result_message TEXT := NULL;
  result_json JSONB;
BEGIN
  start_time := clock_timestamp();
  year_month := to_char(date_to_process, 'YYYY-MM');
  
  -- Get initial counts
  SELECT COUNT(*) INTO curtailment_count
  FROM curtailment_records
  WHERE settlement_date = date_to_process;
  
  SELECT COUNT(*) INTO bitcoin_before
  FROM historical_bitcoin_calculations
  WHERE settlement_date = date_to_process;
  
  -- Log start of processing
  INSERT INTO reconciliation_tracking (
    batch_id,
    year_value,
    year_month,
    process_date,
    curtailment_count,
    initial_bitcoin_count,
    status
  ) VALUES (
    batch_id,
    EXTRACT(YEAR FROM date_to_process)::INTEGER,
    year_month,
    date_to_process,
    curtailment_count,
    bitcoin_before,
    'Processing'
  );
  
  -- Process each curtailment record
  BEGIN
    FOR record IN (
      SELECT 
        settlement_date,
        settlement_period,
        farm_id,
        SUM(volume) AS total_volume
      FROM curtailment_records
      WHERE settlement_date = date_to_process
      GROUP BY settlement_date, settlement_period, farm_id
    )
    LOOP
      -- Skip zero volume records
      IF ABS(record.total_volume) > 0 THEN
        -- Insert for S19J_PRO
        INSERT INTO historical_bitcoin_calculations (
          settlement_date, settlement_period, farm_id, miner_model,
          bitcoin_mined, calculated_at, difficulty
        )
        VALUES (
          record.settlement_date,
          record.settlement_period,
          record.farm_id,
          'S19J_PRO',
          ABS(record.total_volume) * 0.00021 * (50000000000000 / difficulty_value),
          NOW(),
          difficulty_value
        )
        ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
        DO UPDATE SET 
          bitcoin_mined = EXCLUDED.bitcoin_mined,
          calculated_at = EXCLUDED.calculated_at,
          difficulty = EXCLUDED.difficulty;
          
        -- Insert for S9
        INSERT INTO historical_bitcoin_calculations (
          settlement_date, settlement_period, farm_id, miner_model,
          bitcoin_mined, calculated_at, difficulty
        )
        VALUES (
          record.settlement_date,
          record.settlement_period,
          record.farm_id,
          'S9',
          ABS(record.total_volume) * 0.00011 * (50000000000000 / difficulty_value),
          NOW(),
          difficulty_value
        )
        ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
        DO UPDATE SET 
          bitcoin_mined = EXCLUDED.bitcoin_mined,
          calculated_at = EXCLUDED.calculated_at,
          difficulty = EXCLUDED.difficulty;
          
        -- Insert for M20S
        INSERT INTO historical_bitcoin_calculations (
          settlement_date, settlement_period, farm_id, miner_model,
          bitcoin_mined, calculated_at, difficulty
        )
        VALUES (
          record.settlement_date,
          record.settlement_period,
          record.farm_id,
          'M20S',
          ABS(record.total_volume) * 0.00016 * (50000000000000 / difficulty_value),
          NOW(),
          difficulty_value
        )
        ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
        DO UPDATE SET 
          bitcoin_mined = EXCLUDED.bitcoin_mined,
          calculated_at = EXCLUDED.calculated_at,
          difficulty = EXCLUDED.difficulty;
      END IF;
    END LOOP;
    
    -- Get final count
    SELECT COUNT(*) INTO bitcoin_after
    FROM historical_bitcoin_calculations
    WHERE settlement_date = date_to_process;
    
    records_added := bitcoin_after - bitcoin_before;
    
  EXCEPTION WHEN OTHERS THEN
    result_status := 'Error';
    result_message := SQLERRM;
  END;
  
  end_time := clock_timestamp();
  
  -- Update tracking record
  UPDATE reconciliation_tracking
  SET 
    process_end = end_time,
    final_bitcoin_count = bitcoin_after,
    records_added = records_added,
    status = result_status,
    error_message = result_message
  WHERE batch_id = batch_id 
    AND process_date = date_to_process 
    AND process_end IS NULL;
  
  -- Build result JSON
  result_json := jsonb_build_object(
    'date', date_to_process,
    'curtailment_count', curtailment_count,
    'bitcoin_before', bitcoin_before,
    'bitcoin_after', bitcoin_after,
    'records_added', records_added,
    'status', result_status,
    'message', result_message,
    'duration_ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000
  );
  
  RETURN result_json;
END;
$$ LANGUAGE plpgsql;

-- Function to process a month
CREATE OR REPLACE FUNCTION process_month(
  year_month TEXT,
  difficulty_value NUMERIC,
  limit_days INTEGER DEFAULT NULL,
  batch_id TEXT DEFAULT 'BATCH-' || gen_random_uuid()
) RETURNS JSONB AS $$
DECLARE
  start_time TIMESTAMP;
  end_time TIMESTAMP;
  year_part INTEGER;
  month_part INTEGER;
  start_date DATE;
  end_date DATE;
  batch_status TEXT := 'Success';
  batch_message TEXT;
  current_date DATE;
  days_processed INTEGER := 0;
  total_curtailment INTEGER := 0;
  total_bitcoin_before INTEGER := 0;
  total_bitcoin_after INTEGER := 0;
  total_added INTEGER := 0;
  date_result JSONB;
  result_json JSONB;
  failed_dates JSONB := '[]'::JSONB;
BEGIN
  start_time := clock_timestamp();
  
  -- Parse year-month
  year_part := CAST(SPLIT_PART(year_month, '-', 1) AS INTEGER);
  month_part := CAST(SPLIT_PART(year_month, '-', 2) AS INTEGER);
  
  -- Set date range
  start_date := make_date(year_part, month_part, 1);
  end_date := (start_date + interval '1 month')::DATE - interval '1 day';
  
  -- Process dates in the month
  FOR current_date IN (
    SELECT DISTINCT settlement_date
    FROM curtailment_records
    WHERE 
      settlement_date >= start_date AND
      settlement_date <= end_date
    ORDER BY settlement_date
  )
  LOOP
    -- Exit if we've reached the limit
    IF limit_days IS NOT NULL AND days_processed >= limit_days THEN
      EXIT;
    END IF;
    
    -- Process this date
    date_result := process_single_date(current_date, difficulty_value, batch_id);
    
    -- Accumulate totals
    total_curtailment := total_curtailment + (date_result->>'curtailment_count')::INTEGER;
    total_bitcoin_before := total_bitcoin_before + (date_result->>'bitcoin_before')::INTEGER;
    total_bitcoin_after := total_bitcoin_after + (date_result->>'bitcoin_after')::INTEGER;
    total_added := total_added + (date_result->>'records_added')::INTEGER;
    days_processed := days_processed + 1;
    
    -- Track failed dates
    IF (date_result->>'status') = 'Error' THEN
      batch_status := 'Partial';
      failed_dates := failed_dates || date_result;
    END IF;
    
    -- Commit after each date
    COMMIT;
    -- Start a new transaction 
    BEGIN;
  END LOOP;
  
  end_time := clock_timestamp();
  
  -- Build result JSON
  result_json := jsonb_build_object(
    'month', year_month,
    'batch_id', batch_id,
    'days_processed', days_processed,
    'total_curtailment', total_curtailment,
    'total_bitcoin_before', total_bitcoin_before,
    'total_bitcoin_after', total_bitcoin_after,
    'total_added', total_added,
    'status', batch_status,
    'failed_dates', failed_dates,
    'duration_ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000
  );
  
  RETURN result_json;
END;
$$ LANGUAGE plpgsql;

-- Function to process a year
CREATE OR REPLACE FUNCTION process_year(
  year_value TEXT,
  difficulty_value NUMERIC,
  max_months INTEGER DEFAULT NULL,
  days_per_month INTEGER DEFAULT NULL,
  batch_id TEXT DEFAULT 'YEAR-' || gen_random_uuid()
) RETURNS JSONB AS $$
DECLARE
  start_time TIMESTAMP;
  end_time TIMESTAMP;
  year_status TEXT := 'Success';
  months_to_process TEXT[];
  current_month TEXT;
  months_processed INTEGER := 0;
  total_curtailment INTEGER := 0;
  total_bitcoin_before INTEGER := 0;
  total_bitcoin_after INTEGER := 0;
  total_added INTEGER := 0;
  month_result JSONB;
  result_json JSONB;
  failed_months JSONB := '[]'::JSONB;
BEGIN
  start_time := clock_timestamp();
  
  -- Get months that need processing
  WITH year_months AS (
    SELECT DISTINCT to_char(settlement_date, 'YYYY-MM') as month
    FROM curtailment_records 
    WHERE EXTRACT(YEAR FROM settlement_date) = year_value::INTEGER
  ),
  month_status AS (
    SELECT 
      ym.month,
      (SELECT COUNT(*) FROM curtailment_records 
       WHERE to_char(settlement_date, 'YYYY-MM') = ym.month) as curtailment_count,
      (SELECT COUNT(*) FROM historical_bitcoin_calculations 
       WHERE to_char(settlement_date, 'YYYY-MM') = ym.month) as bitcoin_count
    FROM year_months ym
  )
  SELECT array_agg(month ORDER BY 
    CASE 
      WHEN bitcoin_count = 0 THEN 1                      -- Missing months first
      WHEN bitcoin_count < curtailment_count * 3 THEN 2  -- Then incomplete months
      ELSE 3                                             -- Then complete months
    END,
    curtailment_count DESC                               -- Highest volume first
  ) INTO months_to_process
  FROM month_status
  WHERE bitcoin_count < curtailment_count * 3;
  
  -- If there are no months to process
  IF months_to_process IS NULL OR array_length(months_to_process, 1) = 0 THEN
    RETURN jsonb_build_object(
      'year', year_value,
      'status', 'Complete',
      'months_processed', 0,
      'message', 'No months need processing'
    );
  END IF;
  
  -- Process each month
  FOREACH current_month IN ARRAY months_to_process
  LOOP
    -- Exit if we've reached the limit
    IF max_months IS NOT NULL AND months_processed >= max_months THEN
      EXIT;
    END IF;
    
    -- Process this month
    month_result := process_month(
      current_month, 
      difficulty_value, 
      days_per_month,
      batch_id || '-' || current_month
    );
    
    -- Accumulate totals
    total_curtailment := total_curtailment + (month_result->>'total_curtailment')::INTEGER;
    total_bitcoin_before := total_bitcoin_before + (month_result->>'total_bitcoin_before')::INTEGER;
    total_bitcoin_after := total_bitcoin_after + (month_result->>'total_bitcoin_after')::INTEGER;
    total_added := total_added + (month_result->>'total_added')::INTEGER;
    months_processed := months_processed + 1;
    
    -- Track failed months
    IF (month_result->>'status') != 'Success' THEN
      year_status := 'Partial';
      failed_months := failed_months || month_result;
    END IF;
  END LOOP;
  
  end_time := clock_timestamp();
  
  -- Build result JSON
  result_json := jsonb_build_object(
    'year', year_value,
    'batch_id', batch_id,
    'months_processed', months_processed,
    'total_curtailment', total_curtailment,
    'total_bitcoin_before', total_bitcoin_before,
    'total_bitcoin_after', total_bitcoin_after,
    'total_added', total_added,
    'status', year_status,
    'failed_months', failed_months,
    'duration_ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000
  );
  
  RETURN result_json;
END;
$$ LANGUAGE plpgsql;

-- Function to check reconciliation status
CREATE OR REPLACE FUNCTION check_reconciliation_status() RETURNS JSONB AS $$
DECLARE
  result_json JSONB;
  years_json JSONB := '[]'::JSONB;
  year_record RECORD;
BEGIN
  -- First, get overall status
  WITH overall_status AS (
    SELECT 
      COUNT(*) as curtailment_count,
      COUNT(*) * 3 as expected_bitcoin_count
    FROM curtailment_records
  ),
  bitcoin_status AS (
    SELECT COUNT(*) as bitcoin_count
    FROM historical_bitcoin_calculations
  ),
  combined AS (
    SELECT 
      os.curtailment_count,
      os.expected_bitcoin_count,
      bs.bitcoin_count,
      os.expected_bitcoin_count - bs.bitcoin_count as missing_count,
      ROUND(bs.bitcoin_count * 100.0 / os.expected_bitcoin_count, 2) as completion_percentage
    FROM overall_status os, bitcoin_status bs
  )
  SELECT jsonb_build_object(
    'curtailment_count', curtailment_count,
    'expected_bitcoin_count', expected_bitcoin_count,
    'bitcoin_count', bitcoin_count,
    'missing_count', missing_count,
    'completion_percentage', completion_percentage,
    'status', CASE 
      WHEN completion_percentage = 100 THEN 'COMPLETE'
      WHEN completion_percentage >= 99.9 THEN 'NEAR COMPLETE'
      ELSE 'INCOMPLETE'
    END
  ) INTO result_json
  FROM combined;
  
  -- Now get year-by-year breakdown
  FOR year_record IN (
    WITH years AS (
      SELECT DISTINCT EXTRACT(YEAR FROM settlement_date)::INTEGER as year
      FROM curtailment_records
      ORDER BY year
    ),
    year_curtailment AS (
      SELECT 
        y.year,
        (SELECT COUNT(*) FROM curtailment_records WHERE EXTRACT(YEAR FROM settlement_date) = y.year) as curtailment_count,
        (SELECT COUNT(*) FROM curtailment_records WHERE EXTRACT(YEAR FROM settlement_date) = y.year) * 3 as expected_count
      FROM years y
    ),
    year_bitcoin AS (
      SELECT 
        EXTRACT(YEAR FROM settlement_date)::INTEGER as year,
        COUNT(*) as bitcoin_count
      FROM historical_bitcoin_calculations
      GROUP BY year
    ),
    combined AS (
      SELECT 
        yc.year,
        yc.curtailment_count,
        yc.expected_count,
        COALESCE(yb.bitcoin_count, 0) as bitcoin_count,
        yc.expected_count - COALESCE(yb.bitcoin_count, 0) as missing_count,
        ROUND(COALESCE(yb.bitcoin_count, 0) * 100.0 / yc.expected_count, 2) as completion_percentage
      FROM year_curtailment yc
      LEFT JOIN year_bitcoin yb ON yc.year = yb.year
    )
    SELECT 
      year,
      curtailment_count,
      expected_count,
      bitcoin_count,
      missing_count,
      completion_percentage,
      CASE 
        WHEN completion_percentage = 100 THEN 'COMPLETE'
        WHEN completion_percentage >= 99.9 THEN 'NEAR COMPLETE'
        ELSE 'INCOMPLETE'
      END as status
    FROM combined
    ORDER BY year
  ) LOOP
    years_json := years_json || jsonb_build_object(
      'year', year_record.year,
      'curtailment_count', year_record.curtailment_count,
      'expected_count', year_record.expected_count,
      'bitcoin_count', year_record.bitcoin_count,
      'missing_count', year_record.missing_count,
      'completion_percentage', year_record.completion_percentage,
      'status', year_record.status
    );
  END LOOP;
  
  -- Add years to result
  result_json := result_json || jsonb_build_object('years', years_json);
  
  RETURN result_json;
END;
$$ LANGUAGE plpgsql;

-- Create necessary indexes
CREATE INDEX IF NOT EXISTS idx_curtailment_settlement_date 
ON curtailment_records (settlement_date);

CREATE INDEX IF NOT EXISTS idx_bitcoin_settlement_date 
ON historical_bitcoin_calculations (settlement_date);

CREATE INDEX IF NOT EXISTS idx_bitcoin_settlement_date_model 
ON historical_bitcoin_calculations (settlement_date, miner_model);

-- Main function to run full reconciliation process
CREATE OR REPLACE FUNCTION run_full_reconciliation(
  max_months_per_year INTEGER DEFAULT NULL,
  days_per_month INTEGER DEFAULT NULL
) RETURNS JSONB AS $$
DECLARE
  start_time TIMESTAMP;
  end_time TIMESTAMP;
  batch_id TEXT;
  overall_status TEXT := 'Success';
  result_json JSONB;
  
  -- Results for each year
  result_2023 JSONB;
  result_2022 JSONB;
  result_2025 JSONB;
  result_2024 JSONB;
  
  -- Difficulty values by year
  difficulty_2022 NUMERIC := 25000000000000;
  difficulty_2023 NUMERIC := 37935772752142;
  difficulty_2024 NUMERIC := 68980189436404;
  difficulty_2025 NUMERIC := 108105433845147;
BEGIN
  start_time := clock_timestamp();
  batch_id := 'FULL-RECONCILIATION-' || to_char(NOW(), 'YYYY-MM-DD-HH24-MI-SS');
  
  -- Get initial status
  result_json := check_reconciliation_status();
  
  -- Store initial status
  result_json := result_json || jsonb_build_object(
    'batch_id', batch_id,
    'start_time', start_time,
    'initial_status', jsonb_build_object(
      'completion_percentage', result_json->>'completion_percentage',
      'missing_count', result_json->>'missing_count'
    )
  );
  
  -- PHASE 1: Process 2023 (highest priority)
  result_2023 := process_year('2023', difficulty_2023, max_months_per_year, days_per_month, batch_id || '-2023');
  
  -- PHASE 2: Process 2022 (second priority)
  result_2022 := process_year('2022', difficulty_2022, max_months_per_year, days_per_month, batch_id || '-2022');
  
  -- PHASE 3: Process 2025 (third priority)
  result_2025 := process_year('2025', difficulty_2025, max_months_per_year, days_per_month, batch_id || '-2025');
  
  -- PHASE 4: Process 2024 (fourth priority)
  result_2024 := process_year('2024', difficulty_2024, max_months_per_year, days_per_month, batch_id || '-2024');
  
  -- Get final status
  result_json := result_json || check_reconciliation_status()->'years';
  
  -- Track overall status
  IF (result_2023->>'status') != 'Success' OR
     (result_2022->>'status') != 'Success' OR
     (result_2025->>'status') != 'Success' OR
     (result_2024->>'status') != 'Success' THEN
    overall_status := 'Partial';
  END IF;
  
  -- Calculate end time
  end_time := clock_timestamp();
  
  -- Add results to JSON
  result_json := result_json || jsonb_build_object(
    'status', overall_status,
    'end_time', end_time,
    'duration_ms', EXTRACT(EPOCH FROM (end_time - start_time)) * 1000,
    'years_processed', jsonb_build_object(
      '2023', result_2023,
      '2022', result_2022,
      '2025', result_2025,
      '2024', result_2024
    )
  );
  
  -- Get current reconciliation status
  result_json := result_json || jsonb_build_object(
    'final_status', check_reconciliation_status()
  );
  
  RETURN result_json;
END;
$$ LANGUAGE plpgsql;

-- Sample usage:

-- 1. Check current reconciliation status:
-- SELECT check_reconciliation_status();

-- 2. Process a specific date:
-- SELECT process_single_date('2023-01-15', 37935772752142);

-- 3. Process a specific month:
-- SELECT process_month('2023-10', 37935772752142, 5);

-- 4. Process a specific year (limited to 2 months, 3 days per month):
-- SELECT process_year('2023', 37935772752142, 2, 3);

-- 5. Run full reconciliation with limits (3 months per year, 5 days per month):
-- SELECT run_full_reconciliation(3, 5);

-- 6. Run full reconciliation with no limits (may take longer):
-- SELECT run_full_reconciliation();