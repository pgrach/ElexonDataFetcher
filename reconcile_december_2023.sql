-- Reconcile December 2023 Script
-- This script specifically processes December 2023 data

-- First, check the current status of December 2023
SELECT 
  TO_CHAR(c.settlement_date, 'YYYY-MM') as year_month,
  COUNT(DISTINCT c.settlement_date) as dates_in_month,
  COUNT(DISTINCT c.id) as curtailment_count,
  SUM(CASE WHEN h.calculation_count IS NOT NULL THEN h.calculation_count ELSE 0 END) as bitcoin_calculations,
  COUNT(DISTINCT c.id) * 3 as expected_bitcoin_count,
  ROUND(
    SUM(CASE WHEN h.calculation_count IS NOT NULL THEN h.calculation_count ELSE 0 END) * 100.0 / 
    NULLIF(COUNT(DISTINCT c.id) * 3, 0), 
    2
  ) as completion_percentage
FROM 
  curtailment_records c
  LEFT JOIN (
    SELECT 
      settlement_date, 
      COUNT(*) as calculation_count
    FROM 
      historical_bitcoin_calculations
    GROUP BY 
      settlement_date
  ) h ON c.settlement_date = h.settlement_date
WHERE 
  TO_CHAR(c.settlement_date, 'YYYY-MM') = '2023-12'
GROUP BY 
  year_month;

-- Create a function to process December 2023 dates in batches
CREATE OR REPLACE FUNCTION process_december_2023(target_date DATE, difficulty_value NUMERIC) RETURNS TEXT AS $$
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
  CREATE TEMPORARY TABLE temp_december_curtailment AS
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
  FROM temp_december_curtailment
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
  FROM temp_december_curtailment
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
  FROM temp_december_curtailment
  WHERE ABS(total_volume) > 0
  ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
  DO UPDATE SET 
    bitcoin_mined = EXCLUDED.bitcoin_mined,
    calculated_at = EXCLUDED.calculated_at,
    difficulty = EXCLUDED.difficulty;
  
  -- Drop temporary table
  DROP TABLE temp_december_curtailment;
  
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

-- Get the list of December 2023 dates that need processing
WITH december_dates AS (
  SELECT DISTINCT settlement_date
  FROM curtailment_records
  WHERE TO_CHAR(settlement_date, 'YYYY-MM') = '2023-12'
  ORDER BY settlement_date
)
SELECT *
FROM december_dates;

-- Process next 5 dates from December 2023
DO $$
DECLARE
  target_date DATE;
  difficulty_value NUMERIC := 37935772752142; -- 2023 difficulty value
  result_text TEXT;
  total_added INTEGER := 0;
  total_processed INTEGER := 0;
  date_count INTEGER := 0;
  max_dates INTEGER := 5; -- Process 5 dates at a time to avoid timeouts
  processed_dates DATE[] := ARRAY['2023-12-04', '2023-12-06', '2023-12-07', '2023-12-08', '2023-12-09']::DATE[];
BEGIN
  -- Process each date in December 2023
  FOR target_date IN
    SELECT DISTINCT settlement_date
    FROM curtailment_records
    WHERE TO_CHAR(settlement_date, 'YYYY-MM') = '2023-12'
    AND settlement_date NOT IN (
      SELECT unnest(processed_dates)
    )
    AND settlement_date NOT IN (
      SELECT settlement_date
      FROM historical_bitcoin_calculations
      WHERE TO_CHAR(settlement_date, 'YYYY-MM') = '2023-12'
      GROUP BY settlement_date
      HAVING COUNT(DISTINCT miner_model) = 3
    )
    ORDER BY settlement_date
    LIMIT max_dates
  LOOP
    date_count := date_count + 1;
    
    IF date_count <= max_dates THEN
      result_text := process_december_2023(target_date, difficulty_value);
      RAISE NOTICE '%', result_text;
      
      total_processed := total_processed + 1;
      total_added := total_added + 
        (SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE settlement_date = target_date) -
        (SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE settlement_date = target_date AND calculated_at < (NOW() - INTERVAL '1 minute'));
    END IF;
  END LOOP;
  
  RAISE NOTICE 'Reconciliation completed for % dates, added approximately % calculations',
    total_processed, total_added;
END $$;

-- Check status after processing
SELECT 
  TO_CHAR(c.settlement_date, 'YYYY-MM') as year_month,
  COUNT(DISTINCT c.settlement_date) as dates_in_month,
  COUNT(DISTINCT c.id) as curtailment_count,
  SUM(CASE WHEN h.calculation_count IS NOT NULL THEN h.calculation_count ELSE 0 END) as bitcoin_calculations,
  COUNT(DISTINCT c.id) * 3 as expected_bitcoin_count,
  ROUND(
    SUM(CASE WHEN h.calculation_count IS NOT NULL THEN h.calculation_count ELSE 0 END) * 100.0 / 
    NULLIF(COUNT(DISTINCT c.id) * 3, 0), 
    2
  ) as completion_percentage
FROM 
  curtailment_records c
  LEFT JOIN (
    SELECT 
      settlement_date, 
      COUNT(*) as calculation_count
    FROM 
      historical_bitcoin_calculations
    GROUP BY 
      settlement_date
  ) h ON c.settlement_date = h.settlement_date
WHERE 
  TO_CHAR(c.settlement_date, 'YYYY-MM') = '2023-12'
GROUP BY 
  year_month;

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