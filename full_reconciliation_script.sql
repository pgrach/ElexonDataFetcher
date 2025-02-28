-- Full Reconciliation Script for Bitcoin Mining Calculations
-- This script provides a comprehensive solution to reconcile curtailment_records 
-- with corresponding bitcoin mining calculations

-- Create tables to track reconciliation progress if they don't exist
CREATE TABLE IF NOT EXISTS reconciliation_progress (
  id SERIAL PRIMARY KEY,
  batch_id TEXT UNIQUE NOT NULL,
  start_time TIMESTAMP DEFAULT NOW(),
  end_time TIMESTAMP,
  year INTEGER NOT NULL,
  month INTEGER NOT NULL,
  dates_processed INTEGER DEFAULT 0,
  records_processed INTEGER DEFAULT 0,
  bitcoin_added INTEGER DEFAULT 0,
  status TEXT DEFAULT 'In Progress'
);

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

-- Next, get high-priority months to process (most missing records)
WITH month_stats AS (
  SELECT 
    EXTRACT(YEAR FROM c.settlement_date)::INTEGER as year,
    EXTRACT(MONTH FROM c.settlement_date)::INTEGER as month,
    TO_CHAR(c.settlement_date, 'YYYY-MM') as yearmonth,
    COUNT(DISTINCT c.id) as curtailment_count,
    COALESCE(COUNT(DISTINCT h.id), 0) as bitcoin_count,
    COUNT(DISTINCT c.id) * 3 as expected_bitcoin_count,
    ROUND(COALESCE(COUNT(DISTINCT h.id), 0) * 100.0 / NULLIF(COUNT(DISTINCT c.id) * 3, 0), 2) as completion_percentage,
    (COUNT(DISTINCT c.id) * 3) - COALESCE(COUNT(DISTINCT h.id), 0) as missing_records
  FROM 
    curtailment_records c
    LEFT JOIN historical_bitcoin_calculations h ON 
      c.settlement_date = h.settlement_date
  GROUP BY 
    year, month, yearmonth
  HAVING 
    COALESCE(COUNT(DISTINCT h.id), 0) < COUNT(DISTINCT c.id) * 3
)
SELECT 
  year, 
  month, 
  yearmonth,
  curtailment_count, 
  bitcoin_count, 
  expected_bitcoin_count, 
  completion_percentage, 
  missing_records,
  ROW_NUMBER() OVER (ORDER BY missing_records DESC) as priority
FROM 
  month_stats
ORDER BY 
  missing_records DESC
LIMIT 10;

-- Process 2023 October (high-priority month in 2023)
DO $$
DECLARE
  target_year INTEGER := 2023;
  target_month INTEGER := 10;
  difficulty_value NUMERIC := 37935772752142;
  batch_id TEXT;
  start_date DATE;
  end_date DATE;
  current_date DATE;
  dates_processed INTEGER := 0;
  records_processed INTEGER := 0;
  bitcoin_before INTEGER;
  bitcoin_after INTEGER;
  bitcoin_added INTEGER := 0;
  result RECORD;
  batch_status TEXT := 'Success';
BEGIN
  -- Generate a batch ID for tracking
  batch_id := 'BATCH-' || target_year || '-' || LPAD(target_month::TEXT, 2, '0') || '-' || 
              TO_CHAR(NOW(), 'YYYYMMDD-HH24MISS');
  
  -- Calculate date range
  start_date := make_date(target_year, target_month, 1);
  end_date := (start_date + INTERVAL '1 month')::DATE - INTERVAL '1 day';
  
  -- Insert batch tracking record
  INSERT INTO reconciliation_progress (
    batch_id, year, month
  ) VALUES (
    batch_id, target_year, target_month
  );
  
  -- Get initial Bitcoin count
  SELECT COUNT(*) INTO bitcoin_before
  FROM historical_bitcoin_calculations
  WHERE EXTRACT(YEAR FROM settlement_date) = target_year
    AND EXTRACT(MONTH FROM settlement_date) = target_month;
    
  -- Find dates that need processing
  FOR current_date IN
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
      settlement_date
    FROM 
      date_stats
    WHERE 
      bitcoin_count < curtailment_count * 3
    ORDER BY 
      curtailment_count DESC
    LIMIT 10  -- Process top 10 dates with most records
  LOOP
    RAISE NOTICE 'Processing date: %', current_date;
    
    -- Create temporary table for this date
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_date_curtailment AS
    SELECT 
      settlement_date,
      settlement_period,
      farm_id,
      SUM(volume) AS total_volume
    FROM curtailment_records
    WHERE settlement_date = current_date
    GROUP BY settlement_date, settlement_period, farm_id;
    
    -- Count records for tracking
    SELECT COUNT(*) INTO result.count FROM temp_date_curtailment;
    records_processed := records_processed + result.count;
    
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
    
    -- Update counters
    dates_processed := dates_processed + 1;
    
    -- Update progress
    UPDATE reconciliation_progress
    SET 
      dates_processed = dates_processed,
      records_processed = records_processed
    WHERE batch_id = batch_id;
  END LOOP;
  
  -- Get final Bitcoin count
  SELECT COUNT(*) INTO bitcoin_after
  FROM historical_bitcoin_calculations
  WHERE EXTRACT(YEAR FROM settlement_date) = target_year
    AND EXTRACT(MONTH FROM settlement_date) = target_month;
  
  bitcoin_added := bitcoin_after - bitcoin_before;
  
  -- Update final progress
  UPDATE reconciliation_progress
  SET 
    end_time = NOW(),
    bitcoin_added = bitcoin_added,
    status = batch_status
  WHERE batch_id = batch_id;
  
  RAISE NOTICE 'Batch % completed: processed % dates, added % Bitcoin calculations',
    batch_id, dates_processed, bitcoin_added;
END $$;

-- Process March 2022 (high-priority month in 2022)
DO $$
DECLARE
  target_year INTEGER := 2022;
  target_month INTEGER := 3;
  difficulty_value NUMERIC := 25000000000000;
  batch_id TEXT;
  start_date DATE;
  end_date DATE;
  current_date DATE;
  dates_processed INTEGER := 0;
  records_processed INTEGER := 0;
  bitcoin_before INTEGER;
  bitcoin_after INTEGER;
  bitcoin_added INTEGER := 0;
  result RECORD;
  batch_status TEXT := 'Success';
BEGIN
  -- Generate a batch ID for tracking
  batch_id := 'BATCH-' || target_year || '-' || LPAD(target_month::TEXT, 2, '0') || '-' || 
              TO_CHAR(NOW(), 'YYYYMMDD-HH24MISS');
  
  -- Calculate date range
  start_date := make_date(target_year, target_month, 1);
  end_date := (start_date + INTERVAL '1 month')::DATE - INTERVAL '1 day';
  
  -- Insert batch tracking record
  INSERT INTO reconciliation_progress (
    batch_id, year, month
  ) VALUES (
    batch_id, target_year, target_month
  );
  
  -- Get initial Bitcoin count
  SELECT COUNT(*) INTO bitcoin_before
  FROM historical_bitcoin_calculations
  WHERE EXTRACT(YEAR FROM settlement_date) = target_year
    AND EXTRACT(MONTH FROM settlement_date) = target_month;
    
  -- Find dates that need processing
  FOR current_date IN
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
      settlement_date
    FROM 
      date_stats
    WHERE 
      bitcoin_count < curtailment_count * 3
    ORDER BY 
      curtailment_count DESC
    LIMIT 10  -- Process top 10 dates with most records
  LOOP
    RAISE NOTICE 'Processing date: %', current_date;
    
    -- Create temporary table for this date
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_date_curtailment AS
    SELECT 
      settlement_date,
      settlement_period,
      farm_id,
      SUM(volume) AS total_volume
    FROM curtailment_records
    WHERE settlement_date = current_date
    GROUP BY settlement_date, settlement_period, farm_id;
    
    -- Count records for tracking
    SELECT COUNT(*) INTO result.count FROM temp_date_curtailment;
    records_processed := records_processed + result.count;
    
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
    
    -- Update counters
    dates_processed := dates_processed + 1;
    
    -- Update progress
    UPDATE reconciliation_progress
    SET 
      dates_processed = dates_processed,
      records_processed = records_processed
    WHERE batch_id = batch_id;
  END LOOP;
  
  -- Get final Bitcoin count
  SELECT COUNT(*) INTO bitcoin_after
  FROM historical_bitcoin_calculations
  WHERE EXTRACT(YEAR FROM settlement_date) = target_year
    AND EXTRACT(MONTH FROM settlement_date) = target_month;
  
  bitcoin_added := bitcoin_after - bitcoin_before;
  
  -- Update final progress
  UPDATE reconciliation_progress
  SET 
    end_time = NOW(),
    bitcoin_added = bitcoin_added,
    status = batch_status
  WHERE batch_id = batch_id;
  
  RAISE NOTICE 'Batch % completed: processed % dates, added % Bitcoin calculations',
    batch_id, dates_processed, bitcoin_added;
END $$;

-- Process February 2025 (to get current month up to 100%)
DO $$
DECLARE
  target_year INTEGER := 2025;
  target_month INTEGER := 2;
  difficulty_value NUMERIC := 110568428300952;
  batch_id TEXT;
  start_date DATE;
  end_date DATE;
  current_date DATE;
  dates_processed INTEGER := 0;
  records_processed INTEGER := 0;
  bitcoin_before INTEGER;
  bitcoin_after INTEGER;
  bitcoin_added INTEGER := 0;
  result RECORD;
  batch_status TEXT := 'Success';
BEGIN
  -- Generate a batch ID for tracking
  batch_id := 'BATCH-' || target_year || '-' || LPAD(target_month::TEXT, 2, '0') || '-' || 
              TO_CHAR(NOW(), 'YYYYMMDD-HH24MISS');
  
  -- Calculate date range
  start_date := make_date(target_year, target_month, 1);
  end_date := (start_date + INTERVAL '1 month')::DATE - INTERVAL '1 day';
  
  -- Insert batch tracking record
  INSERT INTO reconciliation_progress (
    batch_id, year, month
  ) VALUES (
    batch_id, target_year, target_month
  );
  
  -- Get initial Bitcoin count
  SELECT COUNT(*) INTO bitcoin_before
  FROM historical_bitcoin_calculations
  WHERE EXTRACT(YEAR FROM settlement_date) = target_year
    AND EXTRACT(MONTH FROM settlement_date) = target_month;
    
  -- Find dates that need processing
  FOR current_date IN
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
      settlement_date
    FROM 
      date_stats
    WHERE 
      bitcoin_count < curtailment_count * 3
    ORDER BY 
      curtailment_count DESC
    LIMIT 10  -- Process top 10 dates with most records
  LOOP
    RAISE NOTICE 'Processing date: %', current_date;
    
    -- Create temporary table for this date
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_date_curtailment AS
    SELECT 
      settlement_date,
      settlement_period,
      farm_id,
      SUM(volume) AS total_volume
    FROM curtailment_records
    WHERE settlement_date = current_date
    GROUP BY settlement_date, settlement_period, farm_id;
    
    -- Count records for tracking
    SELECT COUNT(*) INTO result.count FROM temp_date_curtailment;
    records_processed := records_processed + result.count;
    
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
    
    -- Update counters
    dates_processed := dates_processed + 1;
    
    -- Update progress
    UPDATE reconciliation_progress
    SET 
      dates_processed = dates_processed,
      records_processed = records_processed
    WHERE batch_id = batch_id;
  END LOOP;
  
  -- Get final Bitcoin count
  SELECT COUNT(*) INTO bitcoin_after
  FROM historical_bitcoin_calculations
  WHERE EXTRACT(YEAR FROM settlement_date) = target_year
    AND EXTRACT(MONTH FROM settlement_date) = target_month;
  
  bitcoin_added := bitcoin_after - bitcoin_before;
  
  -- Update final progress
  UPDATE reconciliation_progress
  SET 
    end_time = NOW(),
    bitcoin_added = bitcoin_added,
    status = batch_status
  WHERE batch_id = batch_id;
  
  RAISE NOTICE 'Batch % completed: processed % dates, added % Bitcoin calculations',
    batch_id, dates_processed, bitcoin_added;
END $$;

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
  ROUND(bitcoin_count * 100.0 / NULLIF(expected_bitcoin_count, 0), 2) as completion_percentage,
  expected_bitcoin_count - bitcoin_count as missing_records
FROM year_stats
ORDER BY completion_percentage ASC, year ASC;

-- View processing history
SELECT * FROM reconciliation_progress 
ORDER BY start_time DESC;