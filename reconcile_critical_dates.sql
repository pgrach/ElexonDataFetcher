-- Script to reconcile critical dates with missing Bitcoin calculations
-- This script will reconcile a specific set of test dates to verify our approach

-- 1. Process a specific 2023 date (January 15th, 2023)
DO $$
DECLARE
    test_date DATE := '2023-01-15';
    difficulty_2023 NUMERIC := 37935772752142;
    records_count INT;
    bitcoin_count_before INT;
    bitcoin_count_after INT;
BEGIN
    -- Get initial counts
    SELECT COUNT(*) INTO records_count
    FROM curtailment_records
    WHERE settlement_date = test_date;
    
    SELECT COUNT(*) INTO bitcoin_count_before
    FROM historical_bitcoin_calculations
    WHERE settlement_date = test_date;
    
    RAISE NOTICE 'Processing date % with % curtailment records and % initial bitcoin calculations',
        test_date, records_count, bitcoin_count_before;
    
    -- Process each curtailment record for this date
    FOR record IN 
        SELECT 
            settlement_date,
            settlement_period,
            farm_id,
            SUM(volume) AS total_volume
        FROM curtailment_records
        WHERE 
            settlement_date = test_date
        GROUP BY settlement_date, settlement_period, farm_id
    LOOP
        -- Only process non-zero volumes
        IF ABS(record.total_volume) > 0 THEN
            -- Calculate Bitcoin for S19J_PRO
            INSERT INTO historical_bitcoin_calculations (
                settlement_date, settlement_period, farm_id, miner_model,
                bitcoin_mined, calculated_at, difficulty
            )
            VALUES (
                record.settlement_date,
                record.settlement_period,
                record.farm_id,
                'S19J_PRO',
                ABS(record.total_volume) * 0.00021 * (50000000000000 / difficulty_2023),
                NOW(),
                difficulty_2023
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
                record.settlement_date,
                record.settlement_period,
                record.farm_id,
                'S9',
                ABS(record.total_volume) * 0.00011 * (50000000000000 / difficulty_2023),
                NOW(),
                difficulty_2023
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
                record.settlement_date,
                record.settlement_period,
                record.farm_id,
                'M20S',
                ABS(record.total_volume) * 0.00016 * (50000000000000 / difficulty_2023),
                NOW(),
                difficulty_2023
            )
            ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
            DO UPDATE SET 
                bitcoin_mined = EXCLUDED.bitcoin_mined,
                calculated_at = EXCLUDED.calculated_at,
                difficulty = EXCLUDED.difficulty;
        END IF;
    END LOOP;
    
    -- Get final count
    SELECT COUNT(*) INTO bitcoin_count_after
    FROM historical_bitcoin_calculations
    WHERE settlement_date = test_date;
    
    -- Log the results
    RAISE NOTICE 'Completed processing % - Bitcoin calculations increased from % to % (added %)',
        test_date, bitcoin_count_before, bitcoin_count_after, bitcoin_count_after - bitcoin_count_before;
END;
$$;

-- 2. Process a specific 2023 date (June 20th, 2023)
DO $$
DECLARE
    test_date DATE := '2023-06-20';
    difficulty_2023 NUMERIC := 37935772752142;
    records_count INT;
    bitcoin_count_before INT;
    bitcoin_count_after INT;
BEGIN
    -- Get initial counts
    SELECT COUNT(*) INTO records_count
    FROM curtailment_records
    WHERE settlement_date = test_date;
    
    SELECT COUNT(*) INTO bitcoin_count_before
    FROM historical_bitcoin_calculations
    WHERE settlement_date = test_date;
    
    RAISE NOTICE 'Processing date % with % curtailment records and % initial bitcoin calculations',
        test_date, records_count, bitcoin_count_before;
    
    -- Process each curtailment record for this date
    FOR record IN 
        SELECT 
            settlement_date,
            settlement_period,
            farm_id,
            SUM(volume) AS total_volume
        FROM curtailment_records
        WHERE 
            settlement_date = test_date
        GROUP BY settlement_date, settlement_period, farm_id
    LOOP
        -- Only process non-zero volumes
        IF ABS(record.total_volume) > 0 THEN
            -- Calculate Bitcoin for S19J_PRO
            INSERT INTO historical_bitcoin_calculations (
                settlement_date, settlement_period, farm_id, miner_model,
                bitcoin_mined, calculated_at, difficulty
            )
            VALUES (
                record.settlement_date,
                record.settlement_period,
                record.farm_id,
                'S19J_PRO',
                ABS(record.total_volume) * 0.00021 * (50000000000000 / difficulty_2023),
                NOW(),
                difficulty_2023
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
                record.settlement_date,
                record.settlement_period,
                record.farm_id,
                'S9',
                ABS(record.total_volume) * 0.00011 * (50000000000000 / difficulty_2023),
                NOW(),
                difficulty_2023
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
                record.settlement_date,
                record.settlement_period,
                record.farm_id,
                'M20S',
                ABS(record.total_volume) * 0.00016 * (50000000000000 / difficulty_2023),
                NOW(),
                difficulty_2023
            )
            ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
            DO UPDATE SET 
                bitcoin_mined = EXCLUDED.bitcoin_mined,
                calculated_at = EXCLUDED.calculated_at,
                difficulty = EXCLUDED.difficulty;
        END IF;
    END LOOP;
    
    -- Get final count
    SELECT COUNT(*) INTO bitcoin_count_after
    FROM historical_bitcoin_calculations
    WHERE settlement_date = test_date;
    
    -- Log the results
    RAISE NOTICE 'Completed processing % - Bitcoin calculations increased from % to % (added %)',
        test_date, bitcoin_count_before, bitcoin_count_after, bitcoin_count_after - bitcoin_count_before;
END;
$$;

-- 3. Process a specific 2025 date (Feb 28th, 2025)
DO $$
DECLARE
    test_date DATE := '2025-02-28';
    difficulty_2025 NUMERIC := 108105433845147;
    records_count INT;
    bitcoin_count_before INT;
    bitcoin_count_after INT;
BEGIN
    -- Get initial counts
    SELECT COUNT(*) INTO records_count
    FROM curtailment_records
    WHERE settlement_date = test_date;
    
    SELECT COUNT(*) INTO bitcoin_count_before
    FROM historical_bitcoin_calculations
    WHERE settlement_date = test_date;
    
    RAISE NOTICE 'Processing date % with % curtailment records and % initial bitcoin calculations',
        test_date, records_count, bitcoin_count_before;
    
    -- Process each curtailment record for this date
    FOR record IN 
        SELECT 
            settlement_date,
            settlement_period,
            farm_id,
            SUM(volume) AS total_volume
        FROM curtailment_records
        WHERE 
            settlement_date = test_date
        GROUP BY settlement_date, settlement_period, farm_id
    LOOP
        -- Only process non-zero volumes
        IF ABS(record.total_volume) > 0 THEN
            -- Calculate Bitcoin for S19J_PRO
            INSERT INTO historical_bitcoin_calculations (
                settlement_date, settlement_period, farm_id, miner_model,
                bitcoin_mined, calculated_at, difficulty
            )
            VALUES (
                record.settlement_date,
                record.settlement_period,
                record.farm_id,
                'S19J_PRO',
                ABS(record.total_volume) * 0.00021 * (50000000000000 / difficulty_2025),
                NOW(),
                difficulty_2025
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
                record.settlement_date,
                record.settlement_period,
                record.farm_id,
                'S9',
                ABS(record.total_volume) * 0.00011 * (50000000000000 / difficulty_2025),
                NOW(),
                difficulty_2025
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
                record.settlement_date,
                record.settlement_period,
                record.farm_id,
                'M20S',
                ABS(record.total_volume) * 0.00016 * (50000000000000 / difficulty_2025),
                NOW(),
                difficulty_2025
            )
            ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
            DO UPDATE SET 
                bitcoin_mined = EXCLUDED.bitcoin_mined,
                calculated_at = EXCLUDED.calculated_at,
                difficulty = EXCLUDED.difficulty;
        END IF;
    END LOOP;
    
    -- Get final count
    SELECT COUNT(*) INTO bitcoin_count_after
    FROM historical_bitcoin_calculations
    WHERE settlement_date = test_date;
    
    -- Log the results
    RAISE NOTICE 'Completed processing % - Bitcoin calculations increased from % to % (added %)',
        test_date, bitcoin_count_before, bitcoin_count_after, bitcoin_count_after - bitcoin_count_before;
END;
$$;