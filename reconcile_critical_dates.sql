-- Reconciliation Script for Critical Dates
-- This script focuses on only the most important dates with missing calculations

-- Function to reconcile Bitcoin calculations for a specific date
CREATE OR REPLACE FUNCTION reconcile_critical_day(target_date DATE, difficulty_override NUMERIC DEFAULT NULL) RETURNS void AS $$
DECLARE
    total_curtailment_records INTEGER;
    total_bitcoin_records_before INTEGER;
    total_bitcoin_records_after INTEGER;
    current_farm_id TEXT;
    current_period INTEGER;
    current_record RECORD;
    farm_cursor CURSOR(process_date DATE) FOR 
        SELECT DISTINCT farm_id, settlement_period
        FROM curtailment_records
        WHERE settlement_date = process_date
        ORDER BY farm_id, settlement_period;
    difficulty_value NUMERIC;
    year_value INTEGER;
    start_timestamp TIMESTAMP;
    processed_count INTEGER := 0;
BEGIN
    -- Record start time
    start_timestamp := NOW();
    
    -- Get the year for automatic difficulty selection
    year_value := EXTRACT(YEAR FROM target_date);
    
    -- Determine difficulty based on year or use override
    IF difficulty_override IS NOT NULL THEN
        difficulty_value := difficulty_override;
    ELSIF year_value = 2022 THEN
        difficulty_value := 28650501065301; -- Avg 2022
    ELSIF year_value = 2023 THEN
        difficulty_value := 37935772752142; -- Avg 2023
    ELSIF year_value = 2024 THEN
        difficulty_value := 81537565317401; -- Avg 2024
    ELSIF year_value = 2025 THEN
        difficulty_value := 110568428300952; -- Recent
    ELSE
        difficulty_value := 50000000000000; -- Default
    END IF;
    
    -- Count initial records
    SELECT COUNT(*) INTO total_curtailment_records
    FROM curtailment_records
    WHERE settlement_date = target_date;
    
    SELECT COUNT(*) INTO total_bitcoin_records_before
    FROM historical_bitcoin_calculations
    WHERE settlement_date = target_date;
    
    RAISE NOTICE 'Starting reconciliation for % with difficulty %: % curtailment records, % bitcoin records', 
        target_date, difficulty_value, total_curtailment_records, total_bitcoin_records_before;
    
    -- Process each farm and period for this date
    OPEN farm_cursor(target_date);
    LOOP
        FETCH farm_cursor INTO current_farm_id, current_period;
        EXIT WHEN NOT FOUND;
        
        -- Process this farm and period
        FOR current_record IN 
            SELECT 
                settlement_date,
                settlement_period,
                farm_id,
                SUM(volume) AS total_volume
            FROM curtailment_records
            WHERE 
                settlement_date = target_date AND
                settlement_period = current_period AND
                farm_id = current_farm_id
            GROUP BY settlement_date, settlement_period, farm_id
        LOOP
            -- Only process non-zero volumes
            IF ABS(current_record.total_volume) > 0 THEN
                -- Calculate Bitcoin for S19J_PRO
                INSERT INTO historical_bitcoin_calculations (
                    settlement_date, settlement_period, farm_id, miner_model,
                    bitcoin_mined, calculated_at, difficulty
                )
                VALUES (
                    current_record.settlement_date,
                    current_record.settlement_period,
                    current_record.farm_id,
                    'S19J_PRO',
                    ABS(current_record.total_volume) * 0.00021 * (50000000000000 / difficulty_value),
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
                    current_record.settlement_date,
                    current_record.settlement_period,
                    current_record.farm_id,
                    'S9',
                    ABS(current_record.total_volume) * 0.00011 * (50000000000000 / difficulty_value),
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
                    current_record.settlement_date,
                    current_record.settlement_period,
                    current_record.farm_id,
                    'M20S',
                    ABS(current_record.total_volume) * 0.00016 * (50000000000000 / difficulty_value),
                    NOW(),
                    difficulty_value
                )
                ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
                DO UPDATE SET 
                    bitcoin_mined = EXCLUDED.bitcoin_mined,
                    calculated_at = EXCLUDED.calculated_at,
                    difficulty = EXCLUDED.difficulty;
                
                processed_count := processed_count + 3;
            END IF;
        END LOOP;
    END LOOP;
    CLOSE farm_cursor;
    
    -- Get final count
    SELECT COUNT(*) INTO total_bitcoin_records_after
    FROM historical_bitcoin_calculations
    WHERE settlement_date = target_date;
    
    RAISE NOTICE 'Reconciliation complete for %: Before: % records, After: % records, Added: % records', 
        target_date, total_bitcoin_records_before, total_bitcoin_records_after, processed_count;
END;
$$ LANGUAGE plpgsql;

-- Process a few critical dates with high volumes, one from each year
-- 2022
SELECT reconcile_critical_day('2022-12-15'); -- December 2022
-- 2023 
SELECT reconcile_critical_day('2023-01-15'); -- January 2023
SELECT reconcile_critical_day('2023-06-20'); -- June 2023
-- 2024
SELECT reconcile_critical_day('2024-09-15'); -- September 2024
-- 2025
SELECT reconcile_critical_day('2025-01-15'); -- January 2025

-- Check the results of our reconciliation for 2023-01-15
WITH curtailment_data AS (
  SELECT 
    settlement_date,
    COUNT(*) as curtailment_count
  FROM curtailment_records
  WHERE settlement_date = '2023-01-15'
  GROUP BY settlement_date
),
bitcoin_data AS (
  SELECT 
    settlement_date,
    miner_model,
    COUNT(*) as bitcoin_count
  FROM historical_bitcoin_calculations
  WHERE settlement_date = '2023-01-15'
  GROUP BY settlement_date, miner_model
),
date_summary AS (
  SELECT 
    cd.settlement_date,
    cd.curtailment_count,
    COALESCE(s19.bitcoin_count, 0) as s19j_pro_count,
    COALESCE(s9.bitcoin_count, 0) as s9_count,
    COALESCE(m20s.bitcoin_count, 0) as m20s_count,
    CASE
      WHEN COALESCE(s19.bitcoin_count, 0) >= cd.curtailment_count AND
           COALESCE(s9.bitcoin_count, 0) >= cd.curtailment_count AND
           COALESCE(m20s.bitcoin_count, 0) >= cd.curtailment_count THEN 'Complete'
      WHEN COALESCE(s19.bitcoin_count, 0) = 0 AND
           COALESCE(s9.bitcoin_count, 0) = 0 AND
           COALESCE(m20s.bitcoin_count, 0) = 0 THEN 'Missing'
      ELSE 'Incomplete'
    END as status,
    ROUND(
      (COALESCE(s19.bitcoin_count, 0) + COALESCE(s9.bitcoin_count, 0) + COALESCE(m20s.bitcoin_count, 0)) * 100.0 / 
      (CASE WHEN cd.curtailment_count = 0 THEN 1 ELSE cd.curtailment_count * 3 END),
      2
    ) as completion_percentage
  FROM curtailment_data cd
  LEFT JOIN bitcoin_data s19 ON cd.settlement_date = s19.settlement_date AND s19.miner_model = 'S19J_PRO'
  LEFT JOIN bitcoin_data s9 ON cd.settlement_date = s9.settlement_date AND s9.miner_model = 'S9'
  LEFT JOIN bitcoin_data m20s ON cd.settlement_date = m20s.settlement_date AND m20s.miner_model = 'M20S'
)
SELECT 
  settlement_date,
  status,
  completion_percentage,
  curtailment_count,
  s19j_pro_count,
  s9_count,
  m20s_count
FROM date_summary;

-- Drop the temporary function
DROP FUNCTION reconcile_critical_day(DATE, NUMERIC);