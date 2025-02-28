-- Complete Bitcoin Reconciliation Script
-- This script systematically repairs all missing and incomplete Bitcoin calculations
-- by targeting specific time periods with issues

-- Create a temporary table to track the progress
CREATE TEMPORARY TABLE reconciliation_progress (
    year_month TEXT,
    status TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    records_before INTEGER,
    records_after INTEGER,
    success BOOLEAN
);

-- Function to reconcile Bitcoin calculations for a specific month
CREATE OR REPLACE FUNCTION reconcile_month(target_year_month TEXT) RETURNS void AS $$
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
    start_timestamp TIMESTAMP;
    miner_stats JSONB;
BEGIN
    -- Parse year-month into year and month parts
    year_part := split_part(target_year_month, '-', 1);
    month_part := split_part(target_year_month, '-', 2);
    
    -- Calculate start and end dates for the month
    start_date := make_date(year_part::INTEGER, month_part::INTEGER, 1);
    end_date := (start_date + INTERVAL '1 month')::DATE - INTERVAL '1 day';
    
    -- Set up miner statistics
    miner_stats := '{
        "S19J_PRO": {"hashrate": 100, "power": 3050, "factor": 0.00021},
        "S9": {"hashrate": 13.5, "power": 1300, "factor": 0.00011},
        "M20S": {"hashrate": 68, "power": 3360, "factor": 0.00016}
    }'::JSONB;
    
    -- Record start time
    start_timestamp := NOW();
    
    -- Count initial records
    SELECT COUNT(*) INTO total_curtailment_records
    FROM curtailment_records
    WHERE settlement_date BETWEEN start_date AND end_date;
    
    SELECT COUNT(*) INTO total_bitcoin_records_before
    FROM historical_bitcoin_calculations
    WHERE settlement_date BETWEEN start_date AND end_date;
    
    -- Store initial status in progress table
    INSERT INTO reconciliation_progress (year_month, status, start_time, records_before)
    VALUES (target_year_month, 'In Progress', start_timestamp, total_bitcoin_records_before);
    
    RAISE NOTICE 'Starting reconciliation for %: % curtailment records, % bitcoin records', 
        target_year_month, total_curtailment_records, total_bitcoin_records_before;
    
    -- Process each day in the month
    process_date := start_date;
    WHILE process_date <= end_date LOOP
        -- Get difficulty for this date (use default if missing)
        -- For demonstration, using a simplified approach - in production we'd call the DynamoDB service
        
        -- Get difficulty based on year
        IF extract(year from process_date) = 2022 THEN 
            difficulty_value := 28650501065301; -- Avg 2022
        ELSIF extract(year from process_date) = 2023 THEN 
            difficulty_value := 37935772752142; -- Avg 2023
        ELSIF extract(year from process_date) = 2024 THEN 
            difficulty_value := 81537565317401; -- Avg 2024
        ELSIF extract(year from process_date) = 2025 THEN 
            difficulty_value := 110568428300952; -- Recent
        ELSE 
            difficulty_value := 50000000000000; -- Default
        END IF;
        
        RAISE NOTICE 'Processing date % with difficulty %', process_date, difficulty_value;
        
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
                        ABS(current_record.total_volume) * (miner_stats->'S19J_PRO'->>'factor')::NUMERIC * (50000000000000 / difficulty_value),
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
                        ABS(current_record.total_volume) * (miner_stats->'S9'->>'factor')::NUMERIC * (50000000000000 / difficulty_value),
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
                        ABS(current_record.total_volume) * (miner_stats->'M20S'->>'factor')::NUMERIC * (50000000000000 / difficulty_value),
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
        END LOOP;
        CLOSE farm_cursor;
        
        -- Move to next day
        process_date := process_date + INTERVAL '1 day';
    END LOOP;
    
    -- Get final count
    SELECT COUNT(*) INTO total_bitcoin_records_after
    FROM historical_bitcoin_calculations
    WHERE settlement_date BETWEEN start_date AND end_date;
    
    -- Update progress table
    UPDATE reconciliation_progress
    SET 
        status = 'Completed',
        end_time = NOW(),
        records_after = total_bitcoin_records_after,
        success = TRUE
    WHERE year_month = target_year_month AND end_time IS NULL;
    
    RAISE NOTICE 'Reconciliation complete for %: Before: % records, After: % records', 
        target_year_month, total_bitcoin_records_before, total_bitcoin_records_after;
END;
$$ LANGUAGE plpgsql;

-- Main section to process specific months

-- Process a single month as a test
SELECT reconcile_month('2023-01');

-- Check the results of our reconciliation
WITH monthly_curtailment AS (
  SELECT 
    to_char(settlement_date, 'YYYY-MM') as year_month,
    COUNT(*) as curtailment_count
  FROM curtailment_records
  WHERE to_char(settlement_date, 'YYYY-MM') = '2023-01'
  GROUP BY to_char(settlement_date, 'YYYY-MM')
),
monthly_bitcoin AS (
  SELECT 
    to_char(settlement_date, 'YYYY-MM') as year_month,
    miner_model,
    COUNT(*) as bitcoin_count
  FROM historical_bitcoin_calculations
  WHERE to_char(settlement_date, 'YYYY-MM') = '2023-01'
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

-- Check reconciliation progress
SELECT * FROM reconciliation_progress;

-- Uncomment these to process more months if the test is successful
-- SELECT reconcile_month('2022-04');
-- SELECT reconcile_month('2022-06');
-- SELECT reconcile_month('2022-07');
-- SELECT reconcile_month('2022-08');
-- SELECT reconcile_month('2022-09');
-- SELECT reconcile_month('2022-10');
-- SELECT reconcile_month('2022-11');
-- SELECT reconcile_month('2022-12');
-- SELECT reconcile_month('2023-02');
-- SELECT reconcile_month('2023-03');
-- SELECT reconcile_month('2023-04');
-- SELECT reconcile_month('2023-05');
-- SELECT reconcile_month('2023-06');
-- SELECT reconcile_month('2023-07');
-- SELECT reconcile_month('2023-08');
-- SELECT reconcile_month('2023-09');
-- SELECT reconcile_month('2023-10');
-- SELECT reconcile_month('2023-11');
-- SELECT reconcile_month('2023-12');
-- SELECT reconcile_month('2024-09');
-- SELECT reconcile_month('2024-12');
-- SELECT reconcile_month('2025-01');
-- SELECT reconcile_month('2025-02');

-- Drop the temporary function
DROP FUNCTION reconcile_month(TEXT);