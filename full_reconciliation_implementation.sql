-- Full Reconciliation Implementation
-- This script implements the complete reconciliation process with optimized performance for large datasets

-- Create progress tracking table
CREATE TABLE IF NOT EXISTS reconciliation_progress (
    id SERIAL PRIMARY KEY,
    year_month TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    curtailment_count INTEGER,
    initial_bitcoin_count INTEGER,
    final_bitcoin_count INTEGER,
    processed_dates INTEGER,
    status TEXT,
    error_message TEXT
);

-- Function to process dates in a specific month
CREATE OR REPLACE FUNCTION reconcile_month(
    target_month TEXT,
    difficulty_value NUMERIC,
    batch_size INTEGER DEFAULT 5
) RETURNS void AS $$
DECLARE
    year_part TEXT;
    month_part TEXT;
    start_date DATE;
    end_date DATE;
    process_date DATE;
    current_record RECORD;
    total_curtailment_records INTEGER;
    initial_bitcoin_records INTEGER;
    days_processed INTEGER := 0;
    date_cursor REFCURSOR;
    error_message TEXT;
    last_processed_date DATE := NULL;
BEGIN
    -- Extract year and month from target_month (format 'YYYY-MM')
    year_part := SPLIT_PART(target_month, '-', 1);
    month_part := SPLIT_PART(target_month, '-', 2);
    
    -- Calculate start and end dates
    start_date := make_date(year_part::INTEGER, month_part::INTEGER, 1);
    end_date := (start_date + INTERVAL '1 month')::DATE - INTERVAL '1 day';
    
    -- Count initial records
    SELECT COUNT(*) INTO total_curtailment_records
    FROM curtailment_records
    WHERE settlement_date BETWEEN start_date AND end_date;
    
    SELECT COUNT(*) INTO initial_bitcoin_records
    FROM historical_bitcoin_calculations
    WHERE settlement_date BETWEEN start_date AND end_date;
    
    -- Store initial information in progress tracking table
    INSERT INTO reconciliation_progress (
        year_month, 
        start_time, 
        curtailment_count, 
        initial_bitcoin_count,
        status
    )
    VALUES (
        target_month, 
        NOW(), 
        total_curtailment_records, 
        initial_bitcoin_records,
        'In Progress'
    );
    
    RAISE NOTICE 'Starting reconciliation for % with difficulty %: % curtailment records, % bitcoin records', 
        target_month, difficulty_value, total_curtailment_records, initial_bitcoin_records;
    
    -- Get all dates in this month that have curtailment records but incomplete bitcoin calculations
    OPEN date_cursor FOR
    WITH date_curtailment AS (
        SELECT 
            settlement_date,
            COUNT(*) as curtailment_count
        FROM curtailment_records
        WHERE to_char(settlement_date, 'YYYY-MM') = target_month
        GROUP BY settlement_date
    ),
    date_bitcoin AS (
        SELECT 
            settlement_date,
            miner_model,
            COUNT(*) as bitcoin_count
        FROM historical_bitcoin_calculations
        WHERE to_char(settlement_date, 'YYYY-MM') = target_month
        GROUP BY settlement_date, miner_model
    ),
    model_counts AS (
        SELECT 
            dc.settlement_date,
            dc.curtailment_count,
            COALESCE((SELECT bitcoin_count FROM date_bitcoin WHERE settlement_date = dc.settlement_date AND miner_model = 'S19J_PRO'), 0) as s19j_pro_count,
            COALESCE((SELECT bitcoin_count FROM date_bitcoin WHERE settlement_date = dc.settlement_date AND miner_model = 'S9'), 0) as s9_count,
            COALESCE((SELECT bitcoin_count FROM date_bitcoin WHERE settlement_date = dc.settlement_date AND miner_model = 'M20S'), 0) as m20s_count
        FROM date_curtailment dc
    ),
    incomplete_dates AS (
        SELECT 
            settlement_date,
            curtailment_count,
            CASE
                WHEN s19j_pro_count = 0 AND s9_count = 0 AND m20s_count = 0 THEN 'Missing'
                WHEN s19j_pro_count < curtailment_count OR s9_count < curtailment_count OR m20s_count < curtailment_count THEN 'Incomplete'
                ELSE 'Complete'
            END as status
        FROM model_counts
        WHERE s19j_pro_count < curtailment_count OR s9_count < curtailment_count OR m20s_count < curtailment_count
        ORDER BY 
            CASE 
                WHEN s19j_pro_count = 0 AND s9_count = 0 AND m20s_count = 0 THEN 1  -- Missing dates first
                ELSE 2                                                              -- Then incomplete dates
            END,
            curtailment_count DESC                                                   -- Highest curtailment count first
    )
    SELECT settlement_date
    FROM incomplete_dates;
    
    -- Process dates in batches
    BEGIN
        LOOP
            -- Process a batch of dates
            FOR i IN 1..batch_size LOOP
                FETCH date_cursor INTO process_date;
                EXIT WHEN NOT FOUND;
                
                -- Process this date
                BEGIN
                    RAISE NOTICE 'Processing date %', process_date;
                    
                    -- Loop through all relevant curtailment records
                    FOR current_record IN 
                        SELECT 
                            settlement_date,
                            settlement_period,
                            farm_id,
                            SUM(volume) AS total_volume
                        FROM curtailment_records
                        WHERE 
                            settlement_date = process_date
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
                        END IF;
                    END LOOP;
                    
                    days_processed := days_processed + 1;
                    last_processed_date := process_date;
                    
                    -- Save progress after each date
                    UPDATE reconciliation_progress
                    SET 
                        processed_dates = days_processed,
                        status = 'Processing: ' || process_date::TEXT
                    WHERE year_month = target_month AND end_time IS NULL;
                    
                EXCEPTION WHEN OTHERS THEN
                    -- Log error for this date but continue with others
                    RAISE WARNING 'Error processing date %: %', process_date, SQLERRM;
                END;
            END LOOP;
            
            -- Commit the batch
            COMMIT;
            -- Start a new transaction for the next batch
            BEGIN;
            
            -- Exit if we've processed all dates
            EXIT WHEN NOT FOUND;
        END LOOP;
        
        -- Get final count of bitcoin records
        SELECT COUNT(*) INTO initial_bitcoin_records
        FROM historical_bitcoin_calculations
        WHERE settlement_date BETWEEN start_date AND end_date;
        
        -- Update progress with success
        UPDATE reconciliation_progress
        SET 
            end_time = NOW(),
            final_bitcoin_count = initial_bitcoin_records,
            processed_dates = days_processed,
            status = 'Completed'
        WHERE year_month = target_month AND end_time IS NULL;
        
        RAISE NOTICE 'Reconciliation complete for %: Processed % dates', target_month, days_processed;
            
    EXCEPTION WHEN OTHERS THEN
        -- Log error and save checkpoint
        error_message := SQLERRM;
        RAISE NOTICE 'Error during reconciliation of %: %', target_month, error_message;
        
        UPDATE reconciliation_progress
        SET 
            end_time = NOW(),
            final_bitcoin_count = (
                SELECT COUNT(*) 
                FROM historical_bitcoin_calculations 
                WHERE settlement_date BETWEEN start_date AND end_date
            ),
            processed_dates = days_processed,
            status = 'Failed',
            error_message = error_message
        WHERE year_month = target_month AND end_time IS NULL;
    END;
    
    -- Close cursor
    CLOSE date_cursor;
END;
$$ LANGUAGE plpgsql;

-- Function to check monthly reconciliation status
CREATE OR REPLACE FUNCTION check_month_status(target_month TEXT) RETURNS TABLE (
    year_month TEXT,
    status TEXT,
    completion_percentage NUMERIC,
    curtailment_count INTEGER,
    bitcoin_count INTEGER,
    expected_count INTEGER,
    missing_count INTEGER,
    s19j_pro_count INTEGER,
    s9_count INTEGER,
    m20s_count INTEGER
) AS $$
BEGIN
    RETURN QUERY
    WITH month_curtailment AS (
        SELECT 
            COUNT(*) as curtailment_count
        FROM curtailment_records
        WHERE to_char(settlement_date, 'YYYY-MM') = target_month
    ),
    month_bitcoin_models AS (
        SELECT 
            miner_model,
            COUNT(*) as model_count
        FROM historical_bitcoin_calculations
        WHERE to_char(settlement_date, 'YYYY-MM') = target_month
        GROUP BY miner_model
    ),
    month_bitcoin AS (
        SELECT 
            COUNT(*) as bitcoin_count
        FROM historical_bitcoin_calculations
        WHERE to_char(settlement_date, 'YYYY-MM') = target_month
    )
    SELECT 
        target_month,
        CASE
            WHEN mc.curtailment_count * 3 = mb.bitcoin_count THEN 'Complete'
            WHEN mb.bitcoin_count = 0 THEN 'Missing'
            ELSE 'Incomplete'
        END,
        ROUND(
            mb.bitcoin_count * 100.0 / (mc.curtailment_count * 3),
            2
        ),
        mc.curtailment_count,
        mb.bitcoin_count,
        mc.curtailment_count * 3 as expected_count,
        (mc.curtailment_count * 3) - mb.bitcoin_count as missing_count,
        COALESCE((SELECT model_count FROM month_bitcoin_models WHERE miner_model = 'S19J_PRO'), 0) as s19_count,
        COALESCE((SELECT model_count FROM month_bitcoin_models WHERE miner_model = 'S9'), 0) as s9_count,
        COALESCE((SELECT model_count FROM month_bitcoin_models WHERE miner_model = 'M20S'), 0) as m20s_count
    FROM month_curtailment mc, month_bitcoin mb;
END;
$$ LANGUAGE plpgsql;

-- Function to reconcile a specific year
CREATE OR REPLACE FUNCTION reconcile_year(
    target_year TEXT,
    difficulty_value NUMERIC,
    batch_size INTEGER DEFAULT 5,
    max_months INTEGER DEFAULT NULL
) RETURNS void AS $$
DECLARE
    months_to_process TEXT[];
    current_month TEXT;
    months_processed INTEGER := 0;
BEGIN
    -- Get months to process for this year (ordered by priority)
    WITH year_months AS (
        SELECT DISTINCT to_char(settlement_date, 'YYYY-MM') as year_month
        FROM curtailment_records
        WHERE EXTRACT(YEAR FROM settlement_date) = target_year::INTEGER
    ),
    month_status AS (
        SELECT 
            ym.year_month,
            COALESCE((
                SELECT COUNT(*)
                FROM historical_bitcoin_calculations
                WHERE to_char(settlement_date, 'YYYY-MM') = ym.year_month
            ), 0) as bitcoin_count,
            COALESCE((
                SELECT COUNT(*)
                FROM curtailment_records
                WHERE to_char(settlement_date, 'YYYY-MM') = ym.year_month
            ), 0) as curtailment_count
        FROM year_months ym
    )
    SELECT array_agg(year_month ORDER BY 
        CASE 
            WHEN bitcoin_count = 0 THEN 1                  -- Missing months first
            WHEN bitcoin_count < curtailment_count * 3 THEN 2  -- Then incomplete months
            ELSE 3                                              -- Then complete months
        END,
        curtailment_count DESC                               -- Highest curtailment count first
    ) INTO months_to_process
    FROM month_status
    WHERE bitcoin_count < curtailment_count * 3;
    
    RAISE NOTICE 'Processing year % with % months to reconcile', target_year, array_length(months_to_process, 1);
    
    -- Process each month
    FOREACH current_month IN ARRAY months_to_process
    LOOP
        -- Exit if we've reached the max months limit
        IF max_months IS NOT NULL AND months_processed >= max_months THEN
            EXIT;
        END IF;
        
        RAISE NOTICE 'Reconciling month: %', current_month;
        
        -- Process this month
        PERFORM reconcile_month(current_month, difficulty_value, batch_size);
        
        months_processed := months_processed + 1;
    END LOOP;
    
    RAISE NOTICE 'Completed reconciliation of % for % months', target_year, months_processed;
END;
$$ LANGUAGE plpgsql;

-- Function to check yearly reconciliation status
CREATE OR REPLACE FUNCTION check_year_status(target_year TEXT) RETURNS TABLE (
    year TEXT,
    status TEXT,
    completion_percentage NUMERIC,
    curtailment_count INTEGER,
    bitcoin_count INTEGER,
    expected_count INTEGER,
    missing_count INTEGER
) AS $$
BEGIN
    RETURN QUERY
    WITH year_curtailment AS (
        SELECT 
            COUNT(*) as curtailment_count
        FROM curtailment_records
        WHERE EXTRACT(YEAR FROM settlement_date) = target_year::INTEGER
    ),
    year_bitcoin AS (
        SELECT 
            COUNT(*) as bitcoin_count
        FROM historical_bitcoin_calculations
        WHERE EXTRACT(YEAR FROM settlement_date) = target_year::INTEGER
    )
    SELECT 
        target_year,
        CASE
            WHEN yc.curtailment_count * 3 = yb.bitcoin_count THEN 'Complete'
            WHEN yb.bitcoin_count = 0 THEN 'Missing'
            ELSE 'Incomplete'
        END,
        ROUND(
            yb.bitcoin_count * 100.0 / (yc.curtailment_count * 3),
            2
        ),
        yc.curtailment_count,
        yb.bitcoin_count,
        yc.curtailment_count * 3 as expected_count,
        (yc.curtailment_count * 3) - yb.bitcoin_count as missing_count
    FROM year_curtailment yc, year_bitcoin yb;
END;
$$ LANGUAGE plpgsql;

-- Create index to speed up reconciliation operations
CREATE INDEX IF NOT EXISTS curtailment_settlement_date_idx ON curtailment_records(settlement_date);
CREATE INDEX IF NOT EXISTS bitcoin_settlement_date_idx ON historical_bitcoin_calculations(settlement_date);
CREATE INDEX IF NOT EXISTS bitcoin_settlement_date_model_idx ON historical_bitcoin_calculations(settlement_date, miner_model);

-- Add documentation on how to use this script

/*
USAGE EXAMPLES:

1. Check status of a specific month:
   SELECT * FROM check_month_status('2023-06');

2. Check status of a specific year:
   SELECT * FROM check_year_status('2023');

3. Reconcile a specific month:
   SELECT reconcile_month('2023-06', 37935772752142);

4. Reconcile a specific year (all months):
   SELECT reconcile_year('2023', 37935772752142);

5. View reconciliation progress:
   SELECT * FROM reconciliation_progress ORDER BY start_time DESC;

Difficulty values by year:
- 2022: 25000000000000
- 2023: 37935772752142
- 2024: 68980189436404
- 2025: 108105433845147
*/