-- Create a temporary function to process Bitcoin calculations for 
-- January 2023 using a sample date
CREATE OR REPLACE FUNCTION reconcile_january_2023_sample() RETURNS void AS $$
DECLARE
    target_date date := '2023-01-15';
    difficulty_value numeric := 35364968305537; -- Example difficulty from 2023-01
    total_curtailment_records integer;
    total_bitcoin_records_before integer;
    total_bitcoin_records_after integer;
    current_farm_id text;
    current_period integer;
    current_record RECORD;
    farm_cursor CURSOR FOR 
        SELECT DISTINCT farm_id, settlement_period
        FROM curtailment_records
        WHERE settlement_date = target_date
        ORDER BY farm_id, settlement_period;
BEGIN
    -- Count initial records
    SELECT COUNT(*) INTO total_curtailment_records
    FROM curtailment_records
    WHERE settlement_date = target_date;
    
    SELECT COUNT(*) INTO total_bitcoin_records_before
    FROM historical_bitcoin_calculations
    WHERE settlement_date = target_date;
    
    RAISE NOTICE 'Starting reconciliation for %: % curtailment records, % bitcoin records', 
        target_date, total_curtailment_records, total_bitcoin_records_before;
    
    -- Process each farm and period
    OPEN farm_cursor;
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
                -- Simplified calculation for demonstration
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
                -- Simplified calculation for demonstration
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
                -- Simplified calculation for demonstration
                ABS(current_record.total_volume) * 0.00016 * (50000000000000 / difficulty_value),
                NOW(),
                difficulty_value
            )
            ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
            DO UPDATE SET 
                bitcoin_mined = EXCLUDED.bitcoin_mined,
                calculated_at = EXCLUDED.calculated_at,
                difficulty = EXCLUDED.difficulty;
                
            RAISE NOTICE 'Processed: %, period %, farm %, volume %', 
                current_record.settlement_date, 
                current_record.settlement_period,
                current_record.farm_id,
                current_record.total_volume;
        END LOOP;
    END LOOP;
    CLOSE farm_cursor;
    
    -- Get final count
    SELECT COUNT(*) INTO total_bitcoin_records_after
    FROM historical_bitcoin_calculations
    WHERE settlement_date = target_date;
    
    RAISE NOTICE 'Reconciliation complete for %: Before: % records, After: % records', 
        target_date, total_bitcoin_records_before, total_bitcoin_records_after;
END;
$$ LANGUAGE plpgsql;

-- Execute the reconciliation function
SELECT reconcile_january_2023_sample();

-- Check the results
SELECT 
  settlement_date,
  miner_model,
  COUNT(*) as record_count,
  SUM(bitcoin_mined) as total_bitcoin_mined
FROM historical_bitcoin_calculations
WHERE settlement_date = '2023-01-15'
GROUP BY settlement_date, miner_model
ORDER BY settlement_date, miner_model;