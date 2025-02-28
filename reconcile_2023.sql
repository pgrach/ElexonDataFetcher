-- Comprehensive Reconciliation Script for 2023
-- This script fixes all missing Bitcoin calculations for 2023 data

-- Create temporary table to track progress
CREATE TEMPORARY TABLE IF NOT EXISTS reconciliation_progress_2023 (
    month TEXT PRIMARY KEY,
    curtailment_count INTEGER,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    bitcoin_count_before INTEGER,
    bitcoin_count_after INTEGER,
    status TEXT,
    error_message TEXT
);

-- Create checkpoint function
CREATE OR REPLACE FUNCTION save_checkpoint_2023(
    p_month TEXT,
    p_status TEXT,
    p_error_message TEXT DEFAULT NULL
) RETURNS void AS $$
BEGIN
    UPDATE reconciliation_progress_2023
    SET 
        end_time = NOW(),
        bitcoin_count_after = (
            SELECT COUNT(*) 
            FROM historical_bitcoin_calculations 
            WHERE TO_CHAR(settlement_date, 'YYYY-MM') = p_month
        ),
        status = p_status,
        error_message = p_error_message
    WHERE month = p_month;
END;
$$ LANGUAGE plpgsql;

-- Function to reconcile Bitcoin calculations for a specific month in 2023
CREATE OR REPLACE FUNCTION reconcile_month_2023(target_month TEXT) RETURNS void AS $$
DECLARE
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
    
    -- 2023 specific difficulty value
    difficulty_value NUMERIC := 37935772752142;
    
    month_part TEXT;
    processed_count INTEGER := 0;
    error_message TEXT;
BEGIN
    -- Extract month from target_month (format 'YYYY-MM')
    month_part := SPLIT_PART(target_month, '-', 2);
    
    -- Calculate start and end dates
    start_date := make_date(2023, month_part::INTEGER, 1);
    end_date := (start_date + INTERVAL '1 month')::DATE - INTERVAL '1 day';
    
    -- Count initial records
    SELECT COUNT(*) INTO total_curtailment_records
    FROM curtailment_records
    WHERE settlement_date BETWEEN start_date AND end_date;
    
    SELECT COUNT(*) INTO total_bitcoin_records_before
    FROM historical_bitcoin_calculations
    WHERE settlement_date BETWEEN start_date AND end_date;
    
    -- Store initial information in progress tracking table
    INSERT INTO reconciliation_progress_2023 (
        month, 
        curtailment_count, 
        start_time, 
        bitcoin_count_before, 
        status
    )
    VALUES (
        target_month, 
        total_curtailment_records, 
        NOW(), 
        total_bitcoin_records_before, 
        'In Progress'
    )
    ON CONFLICT (month) 
    DO UPDATE SET 
        curtailment_count = EXCLUDED.curtailment_count,
        start_time = EXCLUDED.start_time,
        bitcoin_count_before = EXCLUDED.bitcoin_count_before,
        status = EXCLUDED.status,
        end_time = NULL,
        bitcoin_count_after = NULL,
        error_message = NULL;
    
    RAISE NOTICE 'Starting reconciliation for % with difficulty %: % curtailment records, % bitcoin records', 
        target_month, difficulty_value, total_curtailment_records, total_bitcoin_records_before;
    
    -- Process each day in the month within a transaction block
    BEGIN
        process_date := start_date;
        WHILE process_date <= end_date LOOP
            RAISE NOTICE 'Processing date %', process_date;
            
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
            
            -- Checkpoint at the end of each day for extra safety
            PERFORM save_checkpoint_2023(target_month, 'Processing Day: ' || process_date);
        END LOOP;
        
        -- Get final count
        SELECT COUNT(*) INTO total_bitcoin_records_after
        FROM historical_bitcoin_calculations
        WHERE settlement_date BETWEEN start_date AND end_date;
        
        -- Update progress with success
        PERFORM save_checkpoint_2023(target_month, 'Completed');
        
        RAISE NOTICE 'Reconciliation complete for %: Before: % records, After: % records, Added: % records', 
            target_month, total_bitcoin_records_before, total_bitcoin_records_after, processed_count;
            
    EXCEPTION WHEN OTHERS THEN
        -- Log error and save checkpoint
        error_message := SQLERRM;
        RAISE NOTICE 'Error during reconciliation of %: %', target_month, error_message;
        PERFORM save_checkpoint_2023(target_month, 'Failed', error_message);
        -- Continue and allow processing of next month
    END;
END;
$$ LANGUAGE plpgsql;

-- Create verification function
CREATE OR REPLACE FUNCTION verify_month_2023(target_month TEXT) RETURNS TABLE (
    month TEXT,
    status TEXT,
    completion_percentage NUMERIC,
    curtailment_count INTEGER,
    s19j_pro_count INTEGER,
    s9_count INTEGER,
    m20s_count INTEGER
) AS $$
BEGIN
    RETURN QUERY
    WITH monthly_curtailment AS (
        SELECT 
            COUNT(*) as curtailment_count
        FROM curtailment_records
        WHERE TO_CHAR(settlement_date, 'YYYY-MM') = target_month
    ),
    monthly_bitcoin AS (
        SELECT 
            miner_model,
            COUNT(*) as bitcoin_count
        FROM historical_bitcoin_calculations
        WHERE TO_CHAR(settlement_date, 'YYYY-MM') = target_month
        GROUP BY miner_model
    )
    SELECT 
        target_month,
        CASE
            WHEN COALESCE((SELECT bitcoin_count FROM monthly_bitcoin WHERE miner_model = 'S19J_PRO'), 0) >= mc.curtailment_count AND
                COALESCE((SELECT bitcoin_count FROM monthly_bitcoin WHERE miner_model = 'S9'), 0) >= mc.curtailment_count AND
                COALESCE((SELECT bitcoin_count FROM monthly_bitcoin WHERE miner_model = 'M20S'), 0) >= mc.curtailment_count THEN 'Complete'
            WHEN COALESCE((SELECT bitcoin_count FROM monthly_bitcoin WHERE miner_model = 'S19J_PRO'), 0) = 0 AND
                COALESCE((SELECT bitcoin_count FROM monthly_bitcoin WHERE miner_model = 'S9'), 0) = 0 AND
                COALESCE((SELECT bitcoin_count FROM monthly_bitcoin WHERE miner_model = 'M20S'), 0) = 0 THEN 'Missing'
            ELSE 'Incomplete'
        END,
        ROUND(
            (
                COALESCE((SELECT bitcoin_count FROM monthly_bitcoin WHERE miner_model = 'S19J_PRO'), 0) + 
                COALESCE((SELECT bitcoin_count FROM monthly_bitcoin WHERE miner_model = 'S9'), 0) + 
                COALESCE((SELECT bitcoin_count FROM monthly_bitcoin WHERE miner_model = 'M20S'), 0)
            ) * 100.0 / (mc.curtailment_count * 3),
            2
        ),
        mc.curtailment_count,
        COALESCE((SELECT bitcoin_count FROM monthly_bitcoin WHERE miner_model = 'S19J_PRO'), 0),
        COALESCE((SELECT bitcoin_count FROM monthly_bitcoin WHERE miner_model = 'S9'), 0),
        COALESCE((SELECT bitcoin_count FROM monthly_bitcoin WHERE miner_model = 'M20S'), 0)
    FROM monthly_curtailment mc;
END;
$$ LANGUAGE plpgsql;

-- Main execution section
-- Start with a transaction for consistency
BEGIN;

-- Initialize progress table with all months
INSERT INTO reconciliation_progress_2023 (month, start_time, status)
VALUES 
    ('2023-01', NULL, 'Pending'),
    ('2023-02', NULL, 'Pending'),
    ('2023-03', NULL, 'Pending'),
    ('2023-04', NULL, 'Pending'),
    ('2023-05', NULL, 'Pending'),
    ('2023-06', NULL, 'Pending'),
    ('2023-07', NULL, 'Pending'),
    ('2023-08', NULL, 'Pending'),
    ('2023-09', NULL, 'Pending'),
    ('2023-10', NULL, 'Pending'),
    ('2023-11', NULL, 'Pending'),
    ('2023-12', NULL, 'Pending')
ON CONFLICT (month) DO NOTHING;

-- Process one month as a test first
SELECT reconcile_month_2023('2023-12');

-- Uncommment additional months once the first one is successful
-- SELECT reconcile_month_2023('2023-11');
-- SELECT reconcile_month_2023('2023-10');
-- SELECT reconcile_month_2023('2023-09');
-- SELECT reconcile_month_2023('2023-08');
-- SELECT reconcile_month_2023('2023-07');
-- SELECT reconcile_month_2023('2023-06');
-- SELECT reconcile_month_2023('2023-05');
-- SELECT reconcile_month_2023('2023-04');
-- SELECT reconcile_month_2023('2023-03');
-- SELECT reconcile_month_2023('2023-02');
-- SELECT reconcile_month_2023('2023-01');

COMMIT;

-- Verify the results after processing
SELECT * FROM verify_month_2023('2023-12');

-- Check the progress of all months
SELECT 
    month,
    curtailment_count,
    bitcoin_count_before,
    bitcoin_count_after,
    status,
    CASE
        WHEN bitcoin_count_after IS NOT NULL AND curtailment_count > 0 THEN
            ROUND((bitcoin_count_after / (curtailment_count * 3.0)) * 100, 2)
        ELSE 0
    END AS completion_percentage,
    start_time,
    end_time,
    CASE
        WHEN start_time IS NOT NULL AND end_time IS NOT NULL THEN
            EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER
        ELSE NULL
    END AS duration_seconds,
    error_message
FROM reconciliation_progress_2023
ORDER BY month;

-- Drop the temporary functions when done
DROP FUNCTION reconcile_month_2023(TEXT);
DROP FUNCTION save_checkpoint_2023(TEXT, TEXT, TEXT);
DROP FUNCTION verify_month_2023(TEXT);