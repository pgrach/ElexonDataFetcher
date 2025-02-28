-- Reconcile Critical Dates Script
-- This script processes 5 high-priority dates from 2023 with missing Bitcoin calculations

-- First, identify the top 5 dates from 2023 with the most missing calculations
WITH date_stats AS (
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

-- Set the 2023 difficulty value as a constant
-- Create a function to process a single date
CREATE OR REPLACE FUNCTION process_date(target_date DATE, difficulty_value NUMERIC) RETURNS TEXT AS $$
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

-- Process each critical date
DO $$
DECLARE
  critical_date DATE;
  difficulty_value NUMERIC := 37935772752142; -- 2023 difficulty value
  result_text TEXT;
  total_added INTEGER := 0;
  total_processed INTEGER := 0;
BEGIN
  -- Process each of the top 5 dates
  FOR critical_date IN
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
      GROUP BY 
        c.settlement_date
      HAVING 
        COALESCE(COUNT(DISTINCT h.id), 0) < COUNT(DISTINCT c.id) * 3
      ORDER BY 
        missing_count DESC,
        settlement_date ASC
      LIMIT 5
    ) top_dates
  LOOP
    result_text := process_date(critical_date, difficulty_value);
    RAISE NOTICE '%', result_text;
    
    -- Extract and accumulate statistics (rough estimate)
    total_processed := total_processed + 1;
    total_added := total_added + 
      (SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE settlement_date = critical_date) -
      (SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE settlement_date = critical_date AND calculated_at < (NOW() - INTERVAL '1 minute'));
  END LOOP;
  
  RAISE NOTICE 'Reconciliation completed for % dates, added approximately % calculations',
    total_processed, total_added;
END $$;

-- Check the updated reconciliation status
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