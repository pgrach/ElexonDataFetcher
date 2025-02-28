-- Simple Reconciliation Script
-- This script directly processes specific dates to reconcile the data

-- 1. Check current reconciliation status
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
ORDER BY year;

-- 2. Process 2023-01-15 (highest priority year)
DO $$
DECLARE
  target_date DATE := '2023-01-15';
  difficulty_value NUMERIC := 37935772752142;
  curtailment_count INTEGER;
  bitcoin_before INTEGER;
  bitcoin_after INTEGER;
BEGIN
  -- Get initial counts
  SELECT COUNT(*) INTO curtailment_count
  FROM curtailment_records
  WHERE settlement_date = target_date;
  
  SELECT COUNT(*) INTO bitcoin_before
  FROM historical_bitcoin_calculations
  WHERE settlement_date = target_date;
  
  RAISE NOTICE 'Processing % with % curtailment records and % initial bitcoin calculations',
    target_date, curtailment_count, bitcoin_before;
  
  -- Process each curtailment record for this date
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
      -- Calculate Bitcoin for S19J_PRO
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
        
      -- Calculate Bitcoin for S9
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
        
      -- Calculate Bitcoin for M20S
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
  
  RAISE NOTICE 'Completed processing % - Bitcoin calculations increased from % to % (added %)',
    target_date, bitcoin_before, bitcoin_after, bitcoin_after - bitcoin_before;
END;
$$;

-- 3. Process 2022-03-15 (second priority year)
DO $$
DECLARE
  target_date DATE := '2022-03-15';
  difficulty_value NUMERIC := 25000000000000;
  curtailment_count INTEGER;
  bitcoin_before INTEGER;
  bitcoin_after INTEGER;
BEGIN
  -- Get initial counts
  SELECT COUNT(*) INTO curtailment_count
  FROM curtailment_records
  WHERE settlement_date = target_date;
  
  SELECT COUNT(*) INTO bitcoin_before
  FROM historical_bitcoin_calculations
  WHERE settlement_date = target_date;
  
  RAISE NOTICE 'Processing % with % curtailment records and % initial bitcoin calculations',
    target_date, curtailment_count, bitcoin_before;
  
  -- Process each curtailment record for this date
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
      -- Calculate Bitcoin for S19J_PRO
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
        
      -- Calculate Bitcoin for S9
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
        
      -- Calculate Bitcoin for M20S
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
  
  RAISE NOTICE 'Completed processing % - Bitcoin calculations increased from % to % (added %)',
    target_date, bitcoin_before, bitcoin_after, bitcoin_after - bitcoin_before;
END;
$$;

-- 4. Process 2025-02-28 (current year)
DO $$
DECLARE
  target_date DATE := '2025-02-28';
  difficulty_value NUMERIC := 108105433845147;
  curtailment_count INTEGER;
  bitcoin_before INTEGER;
  bitcoin_after INTEGER;
BEGIN
  -- Get initial counts
  SELECT COUNT(*) INTO curtailment_count
  FROM curtailment_records
  WHERE settlement_date = target_date;
  
  SELECT COUNT(*) INTO bitcoin_before
  FROM historical_bitcoin_calculations
  WHERE settlement_date = target_date;
  
  RAISE NOTICE 'Processing % with % curtailment records and % initial bitcoin calculations',
    target_date, curtailment_count, bitcoin_before;
  
  -- Process each curtailment record for this date
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
      -- Calculate Bitcoin for S19J_PRO
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
        
      -- Calculate Bitcoin for S9
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
        
      -- Calculate Bitcoin for M20S
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
  
  RAISE NOTICE 'Completed processing % - Bitcoin calculations increased from % to % (added %)',
    target_date, bitcoin_before, bitcoin_after, bitcoin_after - bitcoin_before;
END;
$$;

-- 5. Check reconciliation status after processing the test dates
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
ORDER BY year;