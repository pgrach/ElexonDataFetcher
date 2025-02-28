-- Master script to execute the full reconciliation process
-- This script will achieve 100% reconciliation between curtailment_records and historical_bitcoin_calculations

-- First, initialize the process and load the required functions
\i full_reconciliation_implementation.sql

-- Create a master progress tracking table for the overall process
CREATE TABLE IF NOT EXISTS master_reconciliation_progress (
    phase TEXT PRIMARY KEY,
    description TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    expected_records INTEGER,
    added_records INTEGER,
    status TEXT
);

-- Initialize with the four phases (one for each year)
INSERT INTO master_reconciliation_progress 
    (phase, description, expected_records, status)
VALUES
    ('phase_1', 'Reconcile 2023 data (highest priority)', 391863, 'Pending'),
    ('phase_2', 'Reconcile 2022 data (second priority)', 514262, 'Pending'),
    ('phase_3', 'Reconcile 2025 data (third priority)', 122118, 'Pending'),
    ('phase_4', 'Reconcile 2024 data (final priority)', 156351, 'Pending')
ON CONFLICT (phase) DO NOTHING;

-- Helper function to update master progress
CREATE OR REPLACE FUNCTION update_master_progress(
    p_phase TEXT,
    p_status TEXT,
    p_added_records INTEGER DEFAULT NULL
) RETURNS void AS $$
BEGIN
    UPDATE master_reconciliation_progress
    SET 
        status = p_status,
        added_records = COALESCE(p_added_records, added_records)
    WHERE phase = p_phase;
    
    IF p_status = 'In Progress' THEN
        UPDATE master_reconciliation_progress
        SET start_time = NOW()
        WHERE phase = p_phase;
    ELSIF p_status IN ('Completed', 'Failed') THEN
        UPDATE master_reconciliation_progress
        SET end_time = NOW()
        WHERE phase = p_phase;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to check overall reconciliation status
CREATE OR REPLACE FUNCTION check_overall_status() RETURNS TABLE (
    total_curtailment INTEGER,
    total_bitcoin INTEGER,
    total_expected INTEGER,
    overall_percentage NUMERIC,
    status TEXT
) AS $$
BEGIN
    RETURN QUERY
    WITH reconciliation_summary AS (
        SELECT 
            (SELECT COUNT(*) FROM curtailment_records) as total_curtailment,
            (SELECT COUNT(*) FROM historical_bitcoin_calculations) as total_bitcoin,
            (SELECT COUNT(*) FROM curtailment_records) * 3 as total_expected
        )
        SELECT 
            total_curtailment,
            total_bitcoin,
            total_expected,
            ROUND(total_bitcoin * 100.0 / total_expected, 2) as overall_percentage,
            CASE 
                WHEN total_bitcoin >= total_expected THEN 'FULLY RECONCILED'
                WHEN total_bitcoin >= total_expected * 0.99 THEN 'NEARLY COMPLETE (>99%)'
                ELSE 'INCOMPLETE'
            END as status
        FROM reconciliation_summary;
END;
$$ LANGUAGE plpgsql;

-- Show initial reconciliation status
\echo 'Initial Reconciliation Status:'
SELECT * FROM check_overall_status();

-- PHASE 1: Reconcile 2023 (highest priority at 0.06% completion)
\echo '====== PHASE 1: Reconciling 2023 data (highest priority) ======'
SELECT update_master_progress('phase_1', 'In Progress');

-- Get initial state for 2023
\echo 'Initial state for 2023:'
SELECT * FROM check_year_status('2023');

-- Process 2023 data with appropriate difficulty value
-- Use the proper difficulty for 2023
SELECT reconcile_year('2023', 37935772752142);

-- Get final state for 2023
\echo 'Final state for 2023:'
SELECT * FROM check_year_status('2023');

-- Update master progress
SELECT update_master_progress('phase_1', 'Completed', 
    (SELECT bitcoin_count FROM check_year_status('2023')) - 
    (SELECT 234)); -- Initial bitcoin count for 2023

-- PHASE 2: Reconcile 2022 (second priority at 16.74% completion)
\echo '====== PHASE 2: Reconciling 2022 data (second priority) ======'
SELECT update_master_progress('phase_2', 'In Progress');

-- Get initial state for 2022
\echo 'Initial state for 2022:'
SELECT * FROM check_year_status('2022');

-- Process 2022 data with appropriate difficulty value
SELECT reconcile_year('2022', 25000000000000);

-- Get final state for 2022
\echo 'Final state for 2022:'
SELECT * FROM check_year_status('2022');

-- Update master progress
SELECT update_master_progress('phase_2', 'Completed',
    (SELECT bitcoin_count FROM check_year_status('2022')) - 
    (SELECT 103387)); -- Initial bitcoin count for 2022

-- PHASE 3: Reconcile 2025 (third priority at 49.54% completion)
\echo '====== PHASE 3: Reconciling 2025 data (third priority) ======'
SELECT update_master_progress('phase_3', 'In Progress');

-- Get initial state for 2025
\echo 'Initial state for 2025:'
SELECT * FROM check_year_status('2025');

-- Process 2025 data with appropriate difficulty value
SELECT reconcile_year('2025', 108105433845147);

-- Get final state for 2025
\echo 'Final state for 2025:'
SELECT * FROM check_year_status('2025');

-- Update master progress
SELECT update_master_progress('phase_3', 'Completed',
    (SELECT bitcoin_count FROM check_year_status('2025')) - 
    (SELECT 119913)); -- Initial bitcoin count for 2025

-- PHASE 4: Reconcile 2024 (final priority at 81.28% completion)
\echo '====== PHASE 4: Reconciling 2024 data (final priority) ======'
SELECT update_master_progress('phase_4', 'In Progress');

-- Get initial state for 2024
\echo 'Initial state for 2024:'
SELECT * FROM check_year_status('2024');

-- Process 2024 data with appropriate difficulty value
SELECT reconcile_year('2024', 68980189436404);

-- Get final state for 2024
\echo 'Final state for 2024:'
SELECT * FROM check_year_status('2024');

-- Update master progress
SELECT update_master_progress('phase_4', 'Completed',
    (SELECT bitcoin_count FROM check_year_status('2024')) - 
    (SELECT 678951)); -- Initial bitcoin count for 2024

-- Create performance indexes if not already exist
\echo 'Creating performance indexes...'
CREATE INDEX IF NOT EXISTS curtailment_settlement_date_idx ON curtailment_records(settlement_date);
CREATE INDEX IF NOT EXISTS bitcoin_settlement_date_idx ON historical_bitcoin_calculations(settlement_date);
CREATE INDEX IF NOT EXISTS bitcoin_settlement_date_model_idx ON historical_bitcoin_calculations(settlement_date, miner_model);

-- Final verification of reconciliation status
\echo '====== FINAL RECONCILIATION STATUS ======'
SELECT * FROM check_overall_status();

-- Show complete reconciliation summary by year
\echo 'Yearly reconciliation summary:'
SELECT 
    EXTRACT(YEAR FROM settlement_date)::INTEGER as year,
    COUNT(*) as curtailment_count,
    COUNT(*) * 3 as expected_bitcoin_count,
    (
        SELECT COUNT(*) 
        FROM historical_bitcoin_calculations 
        WHERE EXTRACT(YEAR FROM settlement_date) = EXTRACT(YEAR FROM cr.settlement_date)
    ) as actual_bitcoin_count,
    ROUND(
        (SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE EXTRACT(YEAR FROM settlement_date) = EXTRACT(YEAR FROM cr.settlement_date))::NUMERIC * 100 /
        (COUNT(*) * 3)::NUMERIC,
        2
    ) as completion_percentage
FROM curtailment_records cr
GROUP BY EXTRACT(YEAR FROM settlement_date)
ORDER BY EXTRACT(YEAR FROM settlement_date);

-- Show master progress summary
\echo 'Master Reconciliation Process Summary:'
SELECT 
    phase, 
    description, 
    start_time, 
    end_time, 
    EXTRACT(EPOCH FROM (end_time - start_time))::INTEGER as duration_seconds,
    expected_records,
    added_records,
    status,
    CASE 
        WHEN added_records IS NOT NULL AND expected_records > 0 THEN
            ROUND((added_records::NUMERIC / expected_records) * 100, 2)
        ELSE NULL
    END as completion_percentage
FROM master_reconciliation_progress
ORDER BY phase;

-- Clean up temporary functions
DROP FUNCTION IF EXISTS update_master_progress(TEXT, TEXT, INTEGER);

\echo 'Reconciliation process completed!'