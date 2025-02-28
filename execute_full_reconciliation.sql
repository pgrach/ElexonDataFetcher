-- Complete Reconciliation Script
-- This script processes the next set of critical dates from 2023

-- First, identify the next 5 dates from 2023 with the most missing calculations (skipping ones we've already processed)
WITH already_processed AS (
  SELECT DISTINCT settlement_date
  FROM historical_bitcoin_calculations
  WHERE EXTRACT(YEAR FROM settlement_date) = 2023
    AND settlement_date IN ('2023-08-12', '2023-04-12', '2023-09-25', '2023-08-19', '2023-07-01')
),
date_stats AS (
  SELECT 
    c.settlement_date,
    COUNT(DISTINCT c.id) as curtailment_count,
    COALESCE(COUNT(DISTINCT h.id), 0) as bitcoin_count,
    (COUNT(DISTINCT c.id) * 3) - COALESCE(COUNT(DISTINCT h.id), 0) as missing_count
  FROM 
    curtailment_records c
    LEFT JOIN historical_bitcoin_calculations h ON 
      c.settlement_date = h.settlement_date
  WHERE 
    EXTRACT(YEAR FROM c.settlement_date) = 2023
    AND c.settlement_date NOT IN (SELECT settlement_date FROM already_processed)
  GROUP BY 
    c.settlement_date
  HAVING 
    COALESCE(COUNT(DISTINCT h.id), 0) < COUNT(DISTINCT c.id) * 3
)
SELECT 
  settlement_date,
  curtailment_count,
  bitcoin_count,
  missing_count,
  ROUND((bitcoin_count * 100.0) / (curtailment_count * 3), 2) as completion_percentage
FROM 
  date_stats
ORDER BY 
  missing_count DESC, 
  settlement_date ASC
LIMIT 5;

-- Create a function to process a single date
CREATE OR REPLACE FUNCTION process_next_date(target_date DATE, difficulty_value NUMERIC) RETURNS TEXT AS $$
DECLARE
  curtailment_count INTEGER;
  bitcoin_before INTEGER;
  bitcoin_after INTEGER;
  added_count INTEGER;
  result_text TEXT;
BEGIN
  -- Get original counts
  SELECT COUNT(*) INTO curtailment_count FROM curtailment_records WHERE settlement_date = target_date;
  SELECT COUNT(*) INTO bitcoin_before FROM historical_bitcoin_calculations WHERE settlement_date = target_date;

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
  
  added_count := bitcoin_after - bitcoin_before;
  
  -- Prepare result text
  result_text := 'Date ' || target_date || ': ' || 
                 curtailment_count || ' curtailment records, ' ||
                 bitcoin_before || ' calculations before, ' ||
                 bitcoin_after || ' after, added ' || added_count;
                 
  RETURN result_text;
END;
$$ LANGUAGE plpgsql;

-- Process each of the next 5 critical dates
DO $$
DECLARE
  next_critical_date DATE;
  difficulty_value NUMERIC := 37935772752142; -- 2023 difficulty value
  result_text TEXT;
  total_added INTEGER := 0;
  total_processed INTEGER := 0;
  already_processed_dates TEXT[] := ARRAY['2023-08-12', '2023-04-12', '2023-09-25', '2023-08-19', '2023-07-01'];
BEGIN
  -- Process each of the next 5 dates
  FOR next_critical_date IN
    SELECT settlement_date 
    FROM (
      SELECT 
        c.settlement_date,
        COUNT(DISTINCT c.id) as curtailment_count,
        COALESCE(COUNT(DISTINCT h.id), 0) as bitcoin_count,
        (COUNT(DISTINCT c.id) * 3) - COALESCE(COUNT(DISTINCT h.id), 0) as missing_count
      FROM 
        curtailment_records c
        LEFT JOIN historical_bitcoin_calculations h ON 
          c.settlement_date = h.settlement_date
      WHERE 
        EXTRACT(YEAR FROM c.settlement_date) = 2023
        AND c.settlement_date::TEXT NOT IN (SELECT unnest(already_processed_dates))
      GROUP BY 
        c.settlement_date
      HAVING 
        COALESCE(COUNT(DISTINCT h.id), 0) < COUNT(DISTINCT c.id) * 3
      ORDER BY 
        missing_count DESC,
        settlement_date ASC
      LIMIT 5
    ) next_dates
  LOOP
    result_text := process_next_date(next_critical_date, difficulty_value);
    RAISE NOTICE '%', result_text;
    
    -- Extract and accumulate statistics (rough estimate)
    total_processed := total_processed + 1;
    total_added := total_added + 
      (SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE settlement_date = next_critical_date) -
      (SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE settlement_date = next_critical_date AND calculated_at < (NOW() - INTERVAL '1 minute'));
  END LOOP;
  
  RAISE NOTICE 'Reconciliation completed for % dates, added approximately % calculations',
    total_processed, total_added;
END $$;

-- Check the updated reconciliation status for 2023
SELECT
  EXTRACT(YEAR FROM settlement_date)::INTEGER as year,
  COUNT(DISTINCT settlement_date) as processed_dates,
  COUNT(*) as total_records,
  COUNT(*) / 3.0 as curtailment_equivalent,
  (SELECT COUNT(*) FROM curtailment_records WHERE EXTRACT(YEAR FROM settlement_date) = 2023) as actual_curtailment_count,
  ROUND(
    (COUNT(*) * 100.0) / 
    (SELECT COUNT(*) FROM curtailment_records WHERE EXTRACT(YEAR FROM settlement_date) = 2023) / 3.0
  , 2) as completion_percentage
FROM historical_bitcoin_calculations
WHERE EXTRACT(YEAR FROM settlement_date) = 2023
GROUP BY year;

-- Check overall status across all years
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