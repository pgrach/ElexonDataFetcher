-- Direct Reconciliation Script
-- Process a few dates from each year to verify the concept works

-- Create indexes if they don't exist to speed up queries
CREATE INDEX IF NOT EXISTS idx_curtailment_settlement_date 
ON curtailment_records (settlement_date);

CREATE INDEX IF NOT EXISTS idx_bitcoin_settlement_date 
ON historical_bitcoin_calculations (settlement_date);

CREATE INDEX IF NOT EXISTS idx_bitcoin_settlement_date_model 
ON historical_bitcoin_calculations (settlement_date, miner_model);

-- Check initial reconciliation status
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

-- Get sample dates from each year that need processing
WITH sample_dates AS (
  SELECT DISTINCT
    settlement_date,
    EXTRACT(YEAR FROM settlement_date)::INTEGER as year,
    COUNT(*) OVER (PARTITION BY settlement_date) as record_count
  FROM curtailment_records
  WHERE
    -- Check if this date has missing Bitcoin calculations
    (SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE settlement_date = curtailment_records.settlement_date) <
    (SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = curtailment_records.settlement_date) * 3
),
ranked_dates AS (
  SELECT
    settlement_date,
    year,
    record_count,
    ROW_NUMBER() OVER (PARTITION BY year ORDER BY record_count DESC) as date_rank
  FROM sample_dates
)
SELECT 
  settlement_date,
  year,
  record_count
FROM ranked_dates
WHERE date_rank <= 5
ORDER BY year, record_count DESC;

-- Process 5 dates from 2023 (highest priority)
DO $$
DECLARE
  difficulty_2023 NUMERIC := 37935772752142;
  days_processed INTEGER := 0;
  current_record RECORD;
  bitcoin_before INTEGER;
  bitcoin_after INTEGER;
  total_added INTEGER := 0;
BEGIN
  -- Get top 5 dates from 2023 by record count
  FOR current_record IN 
    WITH sample_dates AS (
      SELECT DISTINCT
        settlement_date,
        COUNT(*) OVER (PARTITION BY settlement_date) as record_count
      FROM curtailment_records
      WHERE EXTRACT(YEAR FROM settlement_date) = 2023
      AND
        -- Check if this date has missing Bitcoin calculations
        (SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE settlement_date = curtailment_records.settlement_date) <
        (SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = curtailment_records.settlement_date) * 3
    ),
    ranked_dates AS (
      SELECT
        settlement_date,
        record_count,
        ROW_NUMBER() OVER (ORDER BY record_count DESC) as date_rank
      FROM sample_dates
    )
    SELECT 
      settlement_date
    FROM ranked_dates
    WHERE date_rank <= 5
    ORDER BY date_rank
  LOOP
    -- Exit after processing 5 days
    IF days_processed >= 5 THEN 
      EXIT;
    END IF;
    
    RAISE NOTICE 'Processing 2023 date: %', current_record.settlement_date;
    
    -- Get initial Bitcoin count
    SELECT COUNT(*) INTO bitcoin_before
    FROM historical_bitcoin_calculations
    WHERE settlement_date = current_record.settlement_date;
    
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
      WHERE settlement_date = current_record.settlement_date
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
        ABS(total_volume) * 0.00021 * (50000000000000 / difficulty_2023),
        NOW(),
        difficulty_2023
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
        ABS(total_volume) * 0.00011 * (50000000000000 / difficulty_2023),
        NOW(),
        difficulty_2023
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
        ABS(total_volume) * 0.00016 * (50000000000000 / difficulty_2023),
        NOW(),
        difficulty_2023
      FROM temp_date_curtailment
      WHERE ABS(total_volume) > 0
      ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
      DO UPDATE SET 
        bitcoin_mined = EXCLUDED.bitcoin_mined,
        calculated_at = EXCLUDED.calculated_at,
        difficulty = EXCLUDED.difficulty;
      
      -- Clean up temporary table
      DROP TABLE temp_date_curtailment;
      
      -- Get final Bitcoin count
      SELECT COUNT(*) INTO bitcoin_after
      FROM historical_bitcoin_calculations
      WHERE settlement_date = current_record.settlement_date;
      
      -- Report results
      RAISE NOTICE 'Completed date %: Added % Bitcoin calculations (before: %, after: %)',
        current_record.settlement_date, 
        bitcoin_after - bitcoin_before,
        bitcoin_before,
        bitcoin_after;
      
      -- Update totals
      total_added := total_added + (bitcoin_after - bitcoin_before);
      days_processed := days_processed + 1;
    
    EXCEPTION WHEN OTHERS THEN
      -- Log error but continue with other dates
      RAISE WARNING 'Error processing date %: %', current_record.settlement_date, SQLERRM;
      -- Clean up in case of error
      DROP TABLE IF EXISTS temp_date_curtailment;
    END;
  END LOOP;
  
  RAISE NOTICE 'Completed processing % days from 2023. Added % Bitcoin calculations.',
    days_processed, total_added;
END $$;

-- Process 5 dates from 2022 (second priority)
DO $$
DECLARE
  difficulty_2022 NUMERIC := 25000000000000;
  days_processed INTEGER := 0;
  current_record RECORD;
  bitcoin_before INTEGER;
  bitcoin_after INTEGER;
  total_added INTEGER := 0;
BEGIN
  -- Get top 5 dates from 2022 by record count
  FOR current_record IN 
    WITH sample_dates AS (
      SELECT DISTINCT
        settlement_date,
        COUNT(*) OVER (PARTITION BY settlement_date) as record_count
      FROM curtailment_records
      WHERE EXTRACT(YEAR FROM settlement_date) = 2022
      AND
        -- Check if this date has missing Bitcoin calculations
        (SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE settlement_date = curtailment_records.settlement_date) <
        (SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = curtailment_records.settlement_date) * 3
    ),
    ranked_dates AS (
      SELECT
        settlement_date,
        record_count,
        ROW_NUMBER() OVER (ORDER BY record_count DESC) as date_rank
      FROM sample_dates
    )
    SELECT 
      settlement_date
    FROM ranked_dates
    WHERE date_rank <= 5
    ORDER BY date_rank
  LOOP
    -- Exit after processing 5 days
    IF days_processed >= 5 THEN 
      EXIT;
    END IF;
    
    RAISE NOTICE 'Processing 2022 date: %', current_record.settlement_date;
    
    -- Get initial Bitcoin count
    SELECT COUNT(*) INTO bitcoin_before
    FROM historical_bitcoin_calculations
    WHERE settlement_date = current_record.settlement_date;
    
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
      WHERE settlement_date = current_record.settlement_date
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
        ABS(total_volume) * 0.00021 * (50000000000000 / difficulty_2022),
        NOW(),
        difficulty_2022
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
        ABS(total_volume) * 0.00011 * (50000000000000 / difficulty_2022),
        NOW(),
        difficulty_2022
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
        ABS(total_volume) * 0.00016 * (50000000000000 / difficulty_2022),
        NOW(),
        difficulty_2022
      FROM temp_date_curtailment
      WHERE ABS(total_volume) > 0
      ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
      DO UPDATE SET 
        bitcoin_mined = EXCLUDED.bitcoin_mined,
        calculated_at = EXCLUDED.calculated_at,
        difficulty = EXCLUDED.difficulty;
      
      -- Clean up temporary table
      DROP TABLE temp_date_curtailment;
      
      -- Get final Bitcoin count
      SELECT COUNT(*) INTO bitcoin_after
      FROM historical_bitcoin_calculations
      WHERE settlement_date = current_record.settlement_date;
      
      -- Report results
      RAISE NOTICE 'Completed date %: Added % Bitcoin calculations (before: %, after: %)',
        current_record.settlement_date, 
        bitcoin_after - bitcoin_before,
        bitcoin_before,
        bitcoin_after;
      
      -- Update totals
      total_added := total_added + (bitcoin_after - bitcoin_before);
      days_processed := days_processed + 1;
    
    EXCEPTION WHEN OTHERS THEN
      -- Log error but continue with other dates
      RAISE WARNING 'Error processing date %: %', current_record.settlement_date, SQLERRM;
      -- Clean up in case of error
      DROP TABLE IF EXISTS temp_date_curtailment;
    END;
  END LOOP;
  
  RAISE NOTICE 'Completed processing % days from 2022. Added % Bitcoin calculations.',
    days_processed, total_added;
END $$;

-- Process 5 dates from 2025 (third priority)
DO $$
DECLARE
  difficulty_2025 NUMERIC := 108105433845147;
  days_processed INTEGER := 0;
  current_record RECORD;
  bitcoin_before INTEGER;
  bitcoin_after INTEGER;
  total_added INTEGER := 0;
BEGIN
  -- Get top 5 dates from 2025 by record count
  FOR current_record IN 
    WITH sample_dates AS (
      SELECT DISTINCT
        settlement_date,
        COUNT(*) OVER (PARTITION BY settlement_date) as record_count
      FROM curtailment_records
      WHERE EXTRACT(YEAR FROM settlement_date) = 2025
      AND
        -- Check if this date has missing Bitcoin calculations
        (SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE settlement_date = curtailment_records.settlement_date) <
        (SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = curtailment_records.settlement_date) * 3
    ),
    ranked_dates AS (
      SELECT
        settlement_date,
        record_count,
        ROW_NUMBER() OVER (ORDER BY record_count DESC) as date_rank
      FROM sample_dates
    )
    SELECT 
      settlement_date
    FROM ranked_dates
    WHERE date_rank <= 5
    ORDER BY date_rank
  LOOP
    -- Exit after processing 5 days
    IF days_processed >= 5 THEN 
      EXIT;
    END IF;
    
    RAISE NOTICE 'Processing 2025 date: %', current_record.settlement_date;
    
    -- Get initial Bitcoin count
    SELECT COUNT(*) INTO bitcoin_before
    FROM historical_bitcoin_calculations
    WHERE settlement_date = current_record.settlement_date;
    
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
      WHERE settlement_date = current_record.settlement_date
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
        ABS(total_volume) * 0.00021 * (50000000000000 / difficulty_2025),
        NOW(),
        difficulty_2025
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
        ABS(total_volume) * 0.00011 * (50000000000000 / difficulty_2025),
        NOW(),
        difficulty_2025
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
        ABS(total_volume) * 0.00016 * (50000000000000 / difficulty_2025),
        NOW(),
        difficulty_2025
      FROM temp_date_curtailment
      WHERE ABS(total_volume) > 0
      ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
      DO UPDATE SET 
        bitcoin_mined = EXCLUDED.bitcoin_mined,
        calculated_at = EXCLUDED.calculated_at,
        difficulty = EXCLUDED.difficulty;
      
      -- Clean up temporary table
      DROP TABLE temp_date_curtailment;
      
      -- Get final Bitcoin count
      SELECT COUNT(*) INTO bitcoin_after
      FROM historical_bitcoin_calculations
      WHERE settlement_date = current_record.settlement_date;
      
      -- Report results
      RAISE NOTICE 'Completed date %: Added % Bitcoin calculations (before: %, after: %)',
        current_record.settlement_date, 
        bitcoin_after - bitcoin_before,
        bitcoin_before,
        bitcoin_after;
      
      -- Update totals
      total_added := total_added + (bitcoin_after - bitcoin_before);
      days_processed := days_processed + 1;
    
    EXCEPTION WHEN OTHERS THEN
      -- Log error but continue with other dates
      RAISE WARNING 'Error processing date %: %', current_record.settlement_date, SQLERRM;
      -- Clean up in case of error
      DROP TABLE IF EXISTS temp_date_curtailment;
    END;
  END LOOP;
  
  RAISE NOTICE 'Completed processing % days from 2025. Added % Bitcoin calculations.',
    days_processed, total_added;
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
  ROUND(bitcoin_count * 100.0 / expected_bitcoin_count, 2) as completion_percentage,
  expected_bitcoin_count - bitcoin_count as missing_records
FROM year_stats
ORDER BY completion_percentage ASC, year ASC;