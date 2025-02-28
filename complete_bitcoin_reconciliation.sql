-- Complete Bitcoin Reconciliation Implementation
-- This script implements a single-file approach for complete reconciliation

-- Create temporary table for progress tracking
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_tables WHERE tablename = 'reconciliation_progress') THEN
    CREATE TABLE reconciliation_progress (
      id SERIAL PRIMARY KEY,
      run_date TIMESTAMP DEFAULT NOW(),
      year_month TEXT,
      target_date DATE,
      curtailment_count INTEGER,
      bitcoin_before INTEGER,
      bitcoin_after INTEGER,
      records_added INTEGER,
      status TEXT,
      duration_ms INTEGER
    );
  END IF;
END $$;

-- Main function to reconcile a specific date
CREATE OR REPLACE FUNCTION reconcile_single_date(
  target_date DATE,
  difficulty_value NUMERIC
) RETURNS TABLE (
  date DATE, 
  curtailment_count INTEGER,
  bitcoin_before INTEGER,
  bitcoin_after INTEGER,
  records_added INTEGER,
  status TEXT
) AS $$
DECLARE
  start_time TIMESTAMP;
  end_time TIMESTAMP;
  curtailment_count INTEGER;
  bitcoin_before INTEGER;
  bitcoin_after INTEGER;
  records_added INTEGER;
  execution_status TEXT := 'Success';
BEGIN
  -- Record start time
  start_time := clock_timestamp();
  
  -- Get initial counts
  SELECT COUNT(*) INTO curtailment_count
  FROM curtailment_records
  WHERE settlement_date = target_date;
  
  SELECT COUNT(*) INTO bitcoin_before
  FROM historical_bitcoin_calculations
  WHERE settlement_date = target_date;
  
  -- Begin transaction
  BEGIN
    -- Process each curtailment record
    FOR curt_rec IN 
      SELECT 
        settlement_date,
        settlement_period,
        farm_id,
        SUM(volume) AS total_volume
      FROM curtailment_records
      WHERE settlement_date = target_date
      GROUP BY settlement_date, settlement_period, farm_id
    LOOP
      -- Skip zero volume records
      IF ABS(curt_rec.total_volume) > 0 THEN
        -- Insert S19J_PRO calculation
        INSERT INTO historical_bitcoin_calculations (
          settlement_date, settlement_period, farm_id, miner_model,
          bitcoin_mined, calculated_at, difficulty
        )
        VALUES (
          curt_rec.settlement_date,
          curt_rec.settlement_period,
          curt_rec.farm_id,
          'S19J_PRO',
          ABS(curt_rec.total_volume) * 0.00021 * (50000000000000 / difficulty_value),
          NOW(),
          difficulty_value
        )
        ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
        DO UPDATE SET 
          bitcoin_mined = EXCLUDED.bitcoin_mined,
          calculated_at = EXCLUDED.calculated_at,
          difficulty = EXCLUDED.difficulty;
          
        -- Insert S9 calculation
        INSERT INTO historical_bitcoin_calculations (
          settlement_date, settlement_period, farm_id, miner_model,
          bitcoin_mined, calculated_at, difficulty
        )
        VALUES (
          curt_rec.settlement_date,
          curt_rec.settlement_period,
          curt_rec.farm_id,
          'S9',
          ABS(curt_rec.total_volume) * 0.00011 * (50000000000000 / difficulty_value),
          NOW(),
          difficulty_value
        )
        ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
        DO UPDATE SET 
          bitcoin_mined = EXCLUDED.bitcoin_mined,
          calculated_at = EXCLUDED.calculated_at,
          difficulty = EXCLUDED.difficulty;
          
        -- Insert M20S calculation
        INSERT INTO historical_bitcoin_calculations (
          settlement_date, settlement_period, farm_id, miner_model,
          bitcoin_mined, calculated_at, difficulty
        )
        VALUES (
          curt_rec.settlement_date,
          curt_rec.settlement_period,
          curt_rec.farm_id,
          'M20S',
          ABS(curt_rec.total_volume) * 0.00016 * (50000000000000 / difficulty_value),
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
    WHERE settlement_date = target_date;
    
    records_added := bitcoin_after - bitcoin_before;
    
    -- Record end time
    end_time := clock_timestamp();
    
    -- Record progress in tracking table
    INSERT INTO reconciliation_progress (
      year_month, 
      target_date, 
      curtailment_count, 
      bitcoin_before, 
      bitcoin_after, 
      records_added, 
      status, 
      duration_ms
    ) VALUES (
      to_char(target_date, 'YYYY-MM'),
      target_date,
      curtailment_count,
      bitcoin_before,
      bitcoin_after,
      records_added,
      execution_status,
      EXTRACT(EPOCH FROM (end_time - start_time)) * 1000
    );
    
  EXCEPTION WHEN OTHERS THEN
    execution_status := 'Error: ' || SQLERRM;
    
    -- Record failed attempt
    INSERT INTO reconciliation_progress (
      year_month, 
      target_date, 
      curtailment_count, 
      bitcoin_before, 
      bitcoin_after, 
      records_added, 
      status, 
      duration_ms
    ) VALUES (
      to_char(target_date, 'YYYY-MM'),
      target_date,
      curtailment_count,
      bitcoin_before,
      0,
      0,
      execution_status,
      EXTRACT(EPOCH FROM (clock_timestamp() - start_time)) * 1000
    );
  END;
  
  RETURN QUERY SELECT 
    target_date, 
    curtailment_count, 
    bitcoin_before, 
    bitcoin_after, 
    records_added, 
    execution_status;
END;
$$ LANGUAGE plpgsql;

-- Function to reconcile a month
CREATE OR REPLACE FUNCTION reconcile_month(
  year_month TEXT,
  difficulty_value NUMERIC,
  limit_days INTEGER DEFAULT NULL
) RETURNS TABLE (
  month TEXT,
  total_curtailment INTEGER,
  total_bitcoin_before INTEGER,
  total_bitcoin_after INTEGER,
  total_added INTEGER,
  days_processed INTEGER,
  status TEXT
) AS $$
DECLARE
  year_part INTEGER;
  month_part INTEGER;
  start_date DATE;
  end_date DATE;
  current_date DATE;
  days_processed INTEGER := 0;
  total_curtailment INTEGER := 0;
  total_bitcoin_before INTEGER := 0;
  total_bitcoin_after INTEGER := 0;
  total_added INTEGER := 0;
  execution_status TEXT := 'Success';
  result_record RECORD;
BEGIN
  -- Parse year-month
  year_part := CAST(SPLIT_PART(year_month, '-', 1) AS INTEGER);
  month_part := CAST(SPLIT_PART(year_month, '-', 2) AS INTEGER);
  
  -- Calculate date range
  start_date := make_date(year_part, month_part, 1);
  end_date := (start_date + INTERVAL '1 month')::DATE - INTERVAL '1 day';
  
  -- Get dates with curtailment records
  FOR current_date IN
    SELECT DISTINCT settlement_date
    FROM curtailment_records
    WHERE 
      settlement_date >= start_date AND 
      settlement_date <= end_date
    ORDER BY
      settlement_date
  LOOP
    -- Exit if we've reached the limit
    IF limit_days IS NOT NULL AND days_processed >= limit_days THEN
      EXIT;
    END IF;
    
    -- Process this date
    SELECT * FROM reconcile_single_date(current_date, difficulty_value) INTO result_record;
    
    -- Accumulate totals
    total_curtailment := total_curtailment + result_record.curtailment_count;
    total_bitcoin_before := total_bitcoin_before + result_record.bitcoin_before;
    total_bitcoin_after := total_bitcoin_after + result_record.bitcoin_after;
    total_added := total_added + result_record.records_added;
    days_processed := days_processed + 1;
    
    -- If any date fails, mark the month as failed but continue processing
    IF result_record.status != 'Success' THEN
      execution_status := 'Partial: Error on ' || current_date;
    END IF;
    
    -- Commit after each date
    COMMIT;
  END LOOP;
  
  RETURN QUERY SELECT 
    year_month, 
    total_curtailment, 
    total_bitcoin_before, 
    total_bitcoin_after, 
    total_added, 
    days_processed, 
    execution_status;
END;
$$ LANGUAGE plpgsql;

-- Function to reconcile a year
CREATE OR REPLACE FUNCTION reconcile_year(
  year_value TEXT,
  difficulty_value NUMERIC,
  max_months INTEGER DEFAULT NULL,
  days_per_month INTEGER DEFAULT NULL
) RETURNS TABLE (
  year TEXT,
  months_processed INTEGER,
  total_curtailment INTEGER,
  total_bitcoin_before INTEGER,
  total_bitcoin_after INTEGER,
  total_added INTEGER,
  status TEXT
) AS $$
DECLARE
  months_to_process TEXT[];
  current_month TEXT;
  months_processed INTEGER := 0;
  total_curtailment INTEGER := 0;
  total_bitcoin_before INTEGER := 0;
  total_bitcoin_after INTEGER := 0;
  total_added INTEGER := 0;
  execution_status TEXT := 'Success';
  result_record RECORD;
BEGIN
  -- Get months that need processing
  WITH months_data AS (
    SELECT DISTINCT to_char(settlement_date, 'YYYY-MM') as year_month
    FROM curtailment_records
    WHERE EXTRACT(YEAR FROM settlement_date) = year_value::INTEGER
  ),
  month_status AS (
    SELECT 
      md.year_month,
      (SELECT COUNT(*) FROM curtailment_records 
       WHERE to_char(settlement_date, 'YYYY-MM') = md.year_month) as curtailment_count,
      (SELECT COUNT(*) FROM historical_bitcoin_calculations 
       WHERE to_char(settlement_date, 'YYYY-MM') = md.year_month) as bitcoin_count
    FROM months_data md
  )
  SELECT array_agg(year_month ORDER BY 
      CASE 
        WHEN bitcoin_count = 0 THEN 1                     -- Missing months first
        WHEN bitcoin_count < curtailment_count * 3 THEN 2 -- Then incomplete months
        ELSE 3                                           -- Then complete months (unlikely)
      END,
      curtailment_count DESC                            -- Highest volume first
  ) INTO months_to_process
  FROM month_status
  WHERE bitcoin_count < curtailment_count * 3;
  
  -- Process each month in order
  FOREACH current_month IN ARRAY months_to_process
  LOOP
    -- Exit if we've reached the limit
    IF max_months IS NOT NULL AND months_processed >= max_months THEN
      EXIT;
    END IF;
    
    -- Process this month
    SELECT * FROM reconcile_month(current_month, difficulty_value, days_per_month) INTO result_record;
    
    -- Accumulate totals
    total_curtailment := total_curtailment + result_record.total_curtailment;
    total_bitcoin_before := total_bitcoin_before + result_record.total_bitcoin_before;
    total_bitcoin_after := total_bitcoin_after + result_record.total_bitcoin_after;
    total_added := total_added + result_record.total_added;
    months_processed := months_processed + 1;
    
    -- If any month fails, mark the year as degraded but continue
    IF result_record.status != 'Success' THEN
      execution_status := 'Degraded: Issues with ' || current_month;
    END IF;
  END LOOP;
  
  RETURN QUERY SELECT 
    year_value, 
    months_processed, 
    total_curtailment, 
    total_bitcoin_before, 
    total_bitcoin_after, 
    total_added, 
    execution_status;
END;
$$ LANGUAGE plpgsql;

-- Function to check reconciliation status for a specific date
CREATE OR REPLACE FUNCTION check_date_status(date_value DATE) RETURNS TABLE (
  date DATE,
  curtailment_count INTEGER,
  bitcoin_count INTEGER,
  expected_count INTEGER,
  missing_count INTEGER,
  completion_percentage NUMERIC,
  status TEXT
) AS $$
BEGIN
  RETURN QUERY
  WITH date_status AS (
    SELECT
      COUNT(*) as curtailment_count
    FROM curtailment_records
    WHERE settlement_date = date_value
  ),
  bitcoin_status AS (
    SELECT
      COUNT(*) as bitcoin_count
    FROM historical_bitcoin_calculations
    WHERE settlement_date = date_value
  )
  SELECT
    date_value,
    c.curtailment_count,
    b.bitcoin_count,
    c.curtailment_count * 3 as expected_count,
    (c.curtailment_count * 3) - b.bitcoin_count as missing_count,
    ROUND(b.bitcoin_count * 100.0 / NULLIF(c.curtailment_count * 3, 0), 2) as completion_percentage,
    CASE
      WHEN c.curtailment_count = 0 THEN 'No Data'
      WHEN b.bitcoin_count = 0 THEN 'Missing'
      WHEN b.bitcoin_count < c.curtailment_count * 3 THEN 'Incomplete'
      ELSE 'Complete'
    END as status
  FROM date_status c, bitcoin_status b;
END;
$$ LANGUAGE plpgsql;

-- Function to check reconciliation status for a specific month
CREATE OR REPLACE FUNCTION check_month_status(month_value TEXT) RETURNS TABLE (
  month TEXT,
  curtailment_count INTEGER,
  bitcoin_count INTEGER,
  expected_count INTEGER,
  missing_count INTEGER,
  completion_percentage NUMERIC,
  status TEXT,
  s19j_pro_count INTEGER,
  s9_count INTEGER,
  m20s_count INTEGER
) AS $$
BEGIN
  RETURN QUERY
  WITH month_status AS (
    SELECT
      COUNT(*) as curtailment_count
    FROM curtailment_records
    WHERE to_char(settlement_date, 'YYYY-MM') = month_value
  ),
  bitcoin_status AS (
    SELECT
      COUNT(*) as bitcoin_count
    FROM historical_bitcoin_calculations
    WHERE to_char(settlement_date, 'YYYY-MM') = month_value
  ),
  model_counts AS (
    SELECT
      miner_model,
      COUNT(*) as model_count
    FROM historical_bitcoin_calculations
    WHERE to_char(settlement_date, 'YYYY-MM') = month_value
    GROUP BY miner_model
  )
  SELECT
    month_value,
    ms.curtailment_count,
    bs.bitcoin_count,
    ms.curtailment_count * 3 as expected_count,
    (ms.curtailment_count * 3) - bs.bitcoin_count as missing_count,
    ROUND(bs.bitcoin_count * 100.0 / NULLIF(ms.curtailment_count * 3, 0), 2) as completion_percentage,
    CASE
      WHEN ms.curtailment_count = 0 THEN 'No Data'
      WHEN bs.bitcoin_count = 0 THEN 'Missing'
      WHEN bs.bitcoin_count < ms.curtailment_count * 3 THEN 'Incomplete'
      ELSE 'Complete'
    END as status,
    COALESCE((SELECT model_count FROM model_counts WHERE miner_model = 'S19J_PRO'), 0) as s19j_pro_count,
    COALESCE((SELECT model_count FROM model_counts WHERE miner_model = 'S9'), 0) as s9_count,
    COALESCE((SELECT model_count FROM model_counts WHERE miner_model = 'M20S'), 0) as m20s_count;
END;
$$ LANGUAGE plpgsql;

-- Function to check reconciliation status for a specific year
CREATE OR REPLACE FUNCTION check_year_status(year_value TEXT) RETURNS TABLE (
  year TEXT,
  curtailment_count INTEGER,
  bitcoin_count INTEGER,
  expected_count INTEGER,
  missing_count INTEGER,
  completion_percentage NUMERIC,
  status TEXT
) AS $$
BEGIN
  RETURN QUERY
  WITH year_status AS (
    SELECT
      COUNT(*) as curtailment_count
    FROM curtailment_records
    WHERE EXTRACT(YEAR FROM settlement_date) = year_value::INTEGER
  ),
  bitcoin_status AS (
    SELECT
      COUNT(*) as bitcoin_count
    FROM historical_bitcoin_calculations
    WHERE EXTRACT(YEAR FROM settlement_date) = year_value::INTEGER
  )
  SELECT
    year_value,
    ys.curtailment_count,
    bs.bitcoin_count,
    ys.curtailment_count * 3 as expected_count,
    (ys.curtailment_count * 3) - bs.bitcoin_count as missing_count,
    ROUND(bs.bitcoin_count * 100.0 / NULLIF(ys.curtailment_count * 3, 0), 2) as completion_percentage,
    CASE
      WHEN ys.curtailment_count = 0 THEN 'No Data'
      WHEN bs.bitcoin_count = 0 THEN 'Missing'
      WHEN bs.bitcoin_count < ys.curtailment_count * 3 THEN 'Incomplete'
      ELSE 'Complete'
    END as status
  FROM year_status ys, bitcoin_status bs;
END;
$$ LANGUAGE plpgsql;

-- Function to check overall reconciliation status
CREATE OR REPLACE FUNCTION check_overall_status() RETURNS TABLE (
  total_curtailment INTEGER,
  total_bitcoin INTEGER,
  total_expected INTEGER,
  missing_count INTEGER,
  completion_percentage NUMERIC,
  status TEXT
) AS $$
BEGIN
  RETURN QUERY
  WITH overall_status AS (
    SELECT
      COUNT(*) as curtailment_count
    FROM curtailment_records
  ),
  bitcoin_status AS (
    SELECT
      COUNT(*) as bitcoin_count
    FROM historical_bitcoin_calculations
  )
  SELECT
    os.curtailment_count,
    bs.bitcoin_count,
    os.curtailment_count * 3 as expected_count,
    (os.curtailment_count * 3) - bs.bitcoin_count as missing_count,
    ROUND(bs.bitcoin_count * 100.0 / NULLIF(os.curtailment_count * 3, 0), 2) as completion_percentage,
    CASE
      WHEN os.curtailment_count = 0 THEN 'No Data'
      WHEN bs.bitcoin_count = os.curtailment_count * 3 THEN '100% COMPLETE'
      WHEN bs.bitcoin_count >= os.curtailment_count * 3 * 0.99 THEN '>99% COMPLETE'
      ELSE 'INCOMPLETE'
    END as status
  FROM overall_status os, bitcoin_status bs;
END;
$$ LANGUAGE plpgsql;

-- Create any necessary indexes
CREATE INDEX IF NOT EXISTS idx_curtailment_date ON curtailment_records(settlement_date);
CREATE INDEX IF NOT EXISTS idx_bitcoin_calc_date ON historical_bitcoin_calculations(settlement_date);
CREATE INDEX IF NOT EXISTS idx_bitcoin_calc_date_model ON historical_bitcoin_calculations(settlement_date, miner_model);

-- Sample usage:
-- Check date status: SELECT * FROM check_date_status('2023-01-15');
-- Reconcile a single date: SELECT * FROM reconcile_single_date('2023-01-15', 37935772752142);
-- Check month status: SELECT * FROM check_month_status('2023-01');
-- Reconcile a month: SELECT * FROM reconcile_month('2023-01', 37935772752142, 5);
-- Check year status: SELECT * FROM check_year_status('2023');
-- Reconcile a year: SELECT * FROM reconcile_year('2023', 37935772752142, 2, 3);
-- Check overall status: SELECT * FROM check_overall_status();