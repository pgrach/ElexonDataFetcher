-- Full Reconciliation Implementation Script
-- This script processes an entire month of data for 2023
-- Optimized for bulk processing with safeguards against timeouts

-- Identify which month needs most urgent reconciliation (excluding already processed dates)
WITH already_processed AS (
  SELECT DISTINCT settlement_date
  FROM historical_bitcoin_calculations
  WHERE EXTRACT(YEAR FROM settlement_date) = 2023
  GROUP BY settlement_date
  HAVING COUNT(DISTINCT miner_model) = 3
),
month_stats AS (
  SELECT 
    EXTRACT(YEAR FROM c.settlement_date)::INTEGER as year,
    EXTRACT(MONTH FROM c.settlement_date)::INTEGER as month,
    TO_CHAR(c.settlement_date, 'YYYY-MM') as year_month,
    COUNT(DISTINCT c.settlement_date) as dates_in_month,
    COUNT(DISTINCT c.id) as curtailment_count,
    COUNT(DISTINCT CASE WHEN ap.settlement_date IS NOT NULL THEN c.settlement_date ELSE NULL END) as processed_dates,
    COUNT(DISTINCT CASE WHEN ap.settlement_date IS NULL THEN c.settlement_date ELSE NULL END) as unprocessed_dates,
    SUM(CASE WHEN ap.settlement_date IS NULL THEN 1 ELSE 0 END) as missing_records,
    ROUND(
      COUNT(DISTINCT CASE WHEN ap.settlement_date IS NOT NULL THEN c.settlement_date ELSE NULL END) * 100.0 / 
      NULLIF(COUNT(DISTINCT c.settlement_date), 0),
      2
    ) as completion_percentage
  FROM 
    curtailment_records c
    LEFT JOIN already_processed ap ON c.settlement_date = ap.settlement_date
  WHERE 
    EXTRACT(YEAR FROM c.settlement_date) = 2023
  GROUP BY 
    year, month, year_month
)
SELECT 
  year_month,
  dates_in_month,
  curtailment_count,
  processed_dates,
  unprocessed_dates,
  missing_records,
  completion_percentage
FROM 
  month_stats
ORDER BY 
  completion_percentage ASC,
  missing_records DESC
LIMIT 5;

-- Create a function for month-based batch processing
CREATE OR REPLACE FUNCTION process_monthly_batch(target_year_month TEXT, difficulty_value NUMERIC) RETURNS TEXT AS $$
DECLARE
  start_date DATE;
  end_date DATE;
  current_date DATE;
  processed_count INTEGER := 0;
  skipped_count INTEGER := 0;
  total_records_before INTEGER := 0;
  total_records_after INTEGER := 0;
  result_text TEXT;
  already_processed BOOLEAN;
  max_dates_to_process INTEGER := 10; -- Limit to avoid timeouts
  dates_processed INTEGER := 0;
BEGIN
  -- Parse year-month to get date range
  start_date := (target_year_month || '-01')::DATE;
  end_date := (start_date + INTERVAL '1 month' - INTERVAL '1 day')::DATE;
  
  -- Get count before processing
  SELECT COUNT(*) INTO total_records_before 
  FROM historical_bitcoin_calculations 
  WHERE settlement_date BETWEEN start_date AND end_date;
  
  -- Process each date in the month (limited to max_dates_to_process)
  FOR current_date IN 
    SELECT DISTINCT c.settlement_date
    FROM curtailment_records c
    LEFT JOIN (
      SELECT settlement_date, COUNT(DISTINCT miner_model) as model_count
      FROM historical_bitcoin_calculations
      GROUP BY settlement_date
    ) h ON c.settlement_date = h.settlement_date
    WHERE c.settlement_date BETWEEN start_date AND end_date
    AND (h.model_count IS NULL OR h.model_count < 3)
    ORDER BY c.settlement_date
    LIMIT max_dates_to_process
  LOOP
    -- Check if this date is already completely processed
    SELECT 
      COUNT(DISTINCT miner_model) = 3 INTO already_processed
    FROM 
      historical_bitcoin_calculations
    WHERE 
      settlement_date = current_date;
      
    IF already_processed THEN
      skipped_count := skipped_count + 1;
      CONTINUE;
    END IF;
    
    -- Process this date
    -- Create temporary table for this date
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_month_curtailment AS
    SELECT 
      settlement_date,
      settlement_period,
      farm_id,
      SUM(volume) AS total_volume
    FROM curtailment_records
    WHERE settlement_date = current_date
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
    FROM temp_month_curtailment
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
    FROM temp_month_curtailment
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
    FROM temp_month_curtailment
    WHERE ABS(total_volume) > 0
    ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
    DO UPDATE SET 
      bitcoin_mined = EXCLUDED.bitcoin_mined,
      calculated_at = EXCLUDED.calculated_at,
      difficulty = EXCLUDED.difficulty;
    
    -- Drop temporary table
    DROP TABLE IF EXISTS temp_month_curtailment;
    
    processed_count := processed_count + 1;
    dates_processed := dates_processed + 1;
    
    -- Safeguard against timeouts by limiting batch size
    IF dates_processed >= max_dates_to_process THEN
      EXIT;
    END IF;
  END LOOP;
  
  -- Get count after processing
  SELECT COUNT(*) INTO total_records_after 
  FROM historical_bitcoin_calculations 
  WHERE settlement_date BETWEEN start_date AND end_date;
  
  -- Prepare result text
  result_text := 'Month ' || target_year_month || ': ' || 
               'Processed ' || processed_count || ' dates, ' ||
               'Skipped ' || skipped_count || ' already processed dates, ' ||
               'Added ' || (total_records_after - total_records_before) || ' new calculation records';
               
  RETURN result_text;
END;
$$ LANGUAGE plpgsql;

-- Process the month with lowest completion percentage
DO $$
DECLARE
  target_year_month TEXT;
  difficulty_value NUMERIC := 37935772752142; -- 2023 difficulty value
  result_text TEXT;
BEGIN
  -- Find the month with lowest completion percentage
  SELECT 
    TO_CHAR(c.settlement_date, 'YYYY-MM')
  INTO target_year_month
  FROM 
    curtailment_records c
    LEFT JOIN (
      SELECT 
        settlement_date, 
        COUNT(DISTINCT miner_model) as model_count
      FROM 
        historical_bitcoin_calculations
      GROUP BY 
        settlement_date
    ) h ON c.settlement_date = h.settlement_date
  WHERE 
    EXTRACT(YEAR FROM c.settlement_date) = 2023
  GROUP BY 
    TO_CHAR(c.settlement_date, 'YYYY-MM')
  ORDER BY 
    COUNT(CASE WHEN h.model_count = 3 THEN 1 ELSE NULL END) * 100.0 / COUNT(*) ASC,
    COUNT(*) DESC
  LIMIT 1;
  
  -- Process the identified month
  result_text := process_monthly_batch(target_year_month, difficulty_value);
  RAISE NOTICE '%', result_text;
  
  -- Check updated status
  WITH month_stats AS (
    SELECT 
      TO_CHAR(c.settlement_date, 'YYYY-MM') as year_month,
      COUNT(DISTINCT c.settlement_date) as dates_in_month,
      COUNT(DISTINCT c.id) as curtailment_count,
      COUNT(DISTINCT CASE WHEN h.model_count = 3 THEN c.settlement_date ELSE NULL END) as fully_processed_dates,
      COUNT(DISTINCT CASE WHEN h.model_count < 3 OR h.model_count IS NULL THEN c.settlement_date ELSE NULL END) as incomplete_dates,
      COUNT(DISTINCT c.id) * 3 as expected_calculations,
      SUM(COALESCE(h.calculation_count, 0)) as actual_calculations,
      ROUND(
        SUM(COALESCE(h.calculation_count, 0)) * 100.0 / 
        NULLIF(COUNT(DISTINCT c.id) * 3, 0),
        2
      ) as completion_percentage
    FROM 
      curtailment_records c
      LEFT JOIN (
        SELECT 
          settlement_date, 
          COUNT(DISTINCT miner_model) as model_count,
          COUNT(*) as calculation_count
        FROM 
          historical_bitcoin_calculations
        GROUP BY 
          settlement_date
      ) h ON c.settlement_date = h.settlement_date
    WHERE 
      TO_CHAR(c.settlement_date, 'YYYY-MM') = target_year_month
    GROUP BY 
      year_month
  )
  SELECT 
    'Month ' || year_month || ' - After Processing: ' ||
    fully_processed_dates || '/' || dates_in_month || ' dates complete (' ||
    completion_percentage || '% of calculations)'
  INTO result_text
  FROM month_stats;
  
  RAISE NOTICE '%', result_text;
END $$;

-- Check overall status across all years after batch processing
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