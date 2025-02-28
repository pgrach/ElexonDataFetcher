-- Reconciliation Script for Missing Months
-- This script focuses only on months with 0% completion

-- Function to reconcile Bitcoin calculations for a specific month
CREATE OR REPLACE FUNCTION reconcile_missing_month(target_year_month TEXT) RETURNS void AS $$
DECLARE
    year_part TEXT;
    month_part TEXT;
    start_date DATE;
    end_date DATE;
    process_date DATE;
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
    -- Parse year-month into year and month parts
    year_part := split_part(target_year_month, '-', 1);
    month_part := split_part(target_year_month, '-', 2);
    
    -- Calculate start and end dates for the month
    start_date := make_date(year_part::INTEGER, month_part::INTEGER, 1);
    end_date := (start_date + INTERVAL '1 month')::DATE - INTERVAL '1 day';
    
    -- Determine year for difficulty selection
    year_value := EXTRACT(YEAR FROM start_date)::INTEGER;
    
    -- Set difficulty based on year
    IF extract(year from start_date) = 2022 THEN 
        difficulty_value := 28650501065301; -- Avg 2022
    ELSIF extract(year from start_date) = 2023 THEN 
        difficulty_value := 37935772752142; -- Avg 2023
    ELSIF extract(year from start_date) = 2024 THEN 
        difficulty_value := 81537565317401; -- Avg 2024
    ELSIF extract(year from start_date) = 2025 THEN 
        difficulty_value := 110568428300952; -- Recent
    ELSE 
        difficulty_value := 50000000000000; -- Default
    END IF;
    
    -- Record start time
    start_timestamp := NOW();
    
    -- Count initial records
    SELECT COUNT(*) INTO total_curtailment_records
    FROM curtailment_records
    WHERE settlement_date BETWEEN start_date AND end_date;
    
    SELECT COUNT(*) INTO total_bitcoin_records_before
    FROM historical_bitcoin_calculations
    WHERE settlement_date BETWEEN start_date AND end_date;
    
    RAISE NOTICE 'Starting reconciliation for % with difficulty %: % curtailment records, % bitcoin records', 
        target_year_month, difficulty_value, total_curtailment_records, total_bitcoin_records_before;
    
    -- Process each day in the month
    process_date := start_date;
    WHILE process_date <= end_date LOOP
        RAISE NOTICE 'Processing date % for month %', process_date, target_year_month;
        
        -- Process each farm and period for this date
        OPEN farm_cursor(process_date);
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
                    settlement_date = process_date AND
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
        
        -- Move to next day
        process_date := process_date + INTERVAL '1 day';
    END LOOP;
    
    -- Get final count
    SELECT COUNT(*) INTO total_bitcoin_records_after
    FROM historical_bitcoin_calculations
    WHERE settlement_date BETWEEN start_date AND end_date;
    
    RAISE NOTICE 'Reconciliation complete for %: Before: % records, After: % records, Added: % records', 
        target_year_month, total_bitcoin_records_before, total_bitcoin_records_after, processed_count;
END;
$$ LANGUAGE plpgsql;

-- Process high-priority missing months
-- 2023 months (completely missing)
SELECT reconcile_missing_month('2023-12');
-- SELECT reconcile_missing_month('2023-11');
-- SELECT reconcile_missing_month('2023-10');
-- SELECT reconcile_missing_month('2023-09');
-- SELECT reconcile_missing_month('2023-08');
-- SELECT reconcile_missing_month('2023-07');
-- SELECT reconcile_missing_month('2023-05');
-- SELECT reconcile_missing_month('2023-04');
-- SELECT reconcile_missing_month('2023-03');
-- SELECT reconcile_missing_month('2023-02');

-- 2022 months (completely missing)
-- SELECT reconcile_missing_month('2022-11');
-- SELECT reconcile_missing_month('2022-10');
-- SELECT reconcile_missing_month('2022-09');
-- SELECT reconcile_missing_month('2022-08');
-- SELECT reconcile_missing_month('2022-07');
-- SELECT reconcile_missing_month('2022-06');
-- SELECT reconcile_missing_month('2022-04');

-- Check the results of our reconciliation
WITH monthly_curtailment AS (
  SELECT 
    to_char(settlement_date, 'YYYY-MM') as year_month,
    COUNT(*) as curtailment_count
  FROM curtailment_records
  WHERE to_char(settlement_date, 'YYYY-MM') = '2023-12'
  GROUP BY to_char(settlement_date, 'YYYY-MM')
),
monthly_bitcoin AS (
  SELECT 
    to_char(settlement_date, 'YYYY-MM') as year_month,
    miner_model,
    COUNT(*) as bitcoin_count
  FROM historical_bitcoin_calculations
  WHERE to_char(settlement_date, 'YYYY-MM') = '2023-12'
  GROUP BY to_char(settlement_date, 'YYYY-MM'), miner_model
),
monthly_summary AS (
  SELECT 
    mc.year_month,
    mc.curtailment_count,
    COALESCE(s19.bitcoin_count, 0) as s19j_pro_count,
    COALESCE(s9.bitcoin_count, 0) as s9_count,
    COALESCE(m20s.bitcoin_count, 0) as m20s_count,
    CASE
      WHEN COALESCE(s19.bitcoin_count, 0) >= mc.curtailment_count AND
           COALESCE(s9.bitcoin_count, 0) >= mc.curtailment_count AND
           COALESCE(m20s.bitcoin_count, 0) >= mc.curtailment_count THEN 'Complete'
      WHEN COALESCE(s19.bitcoin_count, 0) = 0 AND
           COALESCE(s9.bitcoin_count, 0) = 0 AND
           COALESCE(m20s.bitcoin_count, 0) = 0 THEN 'Missing'
      ELSE 'Incomplete'
    END as status,
    ROUND(
      (COALESCE(s19.bitcoin_count, 0) + COALESCE(s9.bitcoin_count, 0) + COALESCE(m20s.bitcoin_count, 0)) * 100.0 / 
      (CASE WHEN mc.curtailment_count = 0 THEN 1 ELSE mc.curtailment_count * 3 END),
      2
    ) as completion_percentage
  FROM monthly_curtailment mc
  LEFT JOIN monthly_bitcoin s19 ON mc.year_month = s19.year_month AND s19.miner_model = 'S19J_PRO'
  LEFT JOIN monthly_bitcoin s9 ON mc.year_month = s9.year_month AND s9.miner_model = 'S9'
  LEFT JOIN monthly_bitcoin m20s ON mc.year_month = m20s.year_month AND m20s.miner_model = 'M20S'
)
SELECT 
  year_month,
  status,
  completion_percentage,
  curtailment_count,
  s19j_pro_count,
  s9_count,
  m20s_count
FROM monthly_summary;

-- Drop the temporary function
DROP FUNCTION reconcile_missing_month(TEXT);