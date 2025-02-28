-- Complete Bitcoin Calculation Reconciliation Script
-- This master script orchestrates the full reconciliation process
-- across all time periods (2022-2025)

\echo 'Starting comprehensive Bitcoin calculation reconciliation'
\echo 'Current timestamp: ' `date`

-- Create a temporary table to track overall progress
CREATE TEMPORARY TABLE IF NOT EXISTS reconciliation_master_progress (
    phase TEXT PRIMARY KEY,
    description TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    expected_records INTEGER,
    added_records INTEGER,
    status TEXT
);

-- Initialize the progress table
INSERT INTO reconciliation_master_progress 
    (phase, description, expected_records, status)
VALUES
    ('phase_1', 'Reconcile 2023 data (highest priority)', 392097, 'Pending'),
    ('phase_2', 'Reconcile 2022 data (second priority)', 617649, 'Pending'),
    ('phase_3', 'Reconcile 2025 data (third priority)', 242031, 'Pending'),
    ('phase_4', 'Reconcile 2024 data (final priority)', 835302, 'Pending'),
    ('phase_5', 'Final verification', 0, 'Pending')
ON CONFLICT (phase) DO NOTHING;

-- Function to update phase progress
CREATE OR REPLACE FUNCTION update_reconciliation_phase(
    p_phase TEXT,
    p_status TEXT,
    p_added_records INTEGER DEFAULT NULL
) RETURNS void AS $$
BEGIN
    UPDATE reconciliation_master_progress
    SET 
        status = p_status,
        added_records = COALESCE(p_added_records, added_records)
    WHERE phase = p_phase;
    
    IF p_status = 'In Progress' THEN
        UPDATE reconciliation_master_progress
        SET start_time = NOW()
        WHERE phase = p_phase;
    ELSIF p_status IN ('Completed', 'Failed') THEN
        UPDATE reconciliation_master_progress
        SET end_time = NOW()
        WHERE phase = p_phase;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to get reconciliation statistics
CREATE OR REPLACE FUNCTION get_reconciliation_statistics() RETURNS TABLE (
    year INTEGER,
    curtailment_count INTEGER,
    bitcoin_count INTEGER,
    expected_bitcoin_count INTEGER,
    completion_percentage NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    WITH yearly_curtailment AS (
        SELECT 
            EXTRACT(YEAR FROM settlement_date) as year,
            COUNT(*) as curtailment_count
        FROM curtailment_records
        GROUP BY EXTRACT(YEAR FROM settlement_date)
    ),
    yearly_bitcoin AS (
        SELECT 
            EXTRACT(YEAR FROM settlement_date) as year,
            COUNT(*) as bitcoin_count
        FROM historical_bitcoin_calculations
        GROUP BY EXTRACT(YEAR FROM settlement_date)
    )
    SELECT 
        yc.year::INTEGER,
        yc.curtailment_count,
        COALESCE(yb.bitcoin_count, 0) as bitcoin_count,
        yc.curtailment_count * 3 as expected_bitcoin_count,
        ROUND(
            COALESCE(yb.bitcoin_count, 0)::NUMERIC * 100 / 
            (yc.curtailment_count * 3)::NUMERIC,
            2
        ) as completion_percentage
    FROM yearly_curtailment yc
    LEFT JOIN yearly_bitcoin yb ON yc.year = yb.year
    ORDER BY yc.year;
END;
$$ LANGUAGE plpgsql;

\echo 'Initial reconciliation statistics:'
SELECT * FROM get_reconciliation_statistics();

-- Set specific difficulty values for each year to ensure consistency
SET LOCAL reconciliation.difficulty_2022 = 25000000000000;
SET LOCAL reconciliation.difficulty_2023 = 37935772752142;
SET LOCAL reconciliation.difficulty_2024 = 68980189436404;
SET LOCAL reconciliation.difficulty_2025 = 108105433845147;

-- PHASE 1: Reconcile 2023 (highest priority)
\echo 'Phase 1: Reconciling 2023 data (highest priority)'
SELECT update_reconciliation_phase('phase_1', 'In Progress');

-- Run the 2023 reconciliation script
\i reconcile_2023.sql

-- Update phase completion
SELECT update_reconciliation_phase('phase_1', 'Completed');

\echo 'Phase 1 statistics - 2023 data'
SELECT * FROM get_reconciliation_statistics() WHERE year = 2023;

-- PHASE 2: Reconcile 2022 (second priority)
\echo 'Phase 2: Reconciling 2022 data (second priority)'
SELECT update_reconciliation_phase('phase_2', 'In Progress');

-- Run the 2022 reconciliation script
\i reconcile_2022.sql

-- Update phase completion
SELECT update_reconciliation_phase('phase_2', 'Completed');

\echo 'Phase 2 statistics - 2022 data'
SELECT * FROM get_reconciliation_statistics() WHERE year = 2022;

-- PHASE 3: Reconcile 2025 (third priority)
\echo 'Phase 3: Reconciling 2025 data (third priority)'
SELECT update_reconciliation_phase('phase_3', 'In Progress');

-- Run the 2025 reconciliation script
\i reconcile_2025.sql

-- Update phase completion
SELECT update_reconciliation_phase('phase_3', 'Completed');

\echo 'Phase 3 statistics - 2025 data'
SELECT * FROM get_reconciliation_statistics() WHERE year = 2025;

-- PHASE 4: Reconcile 2024 (final priority)
\echo 'Phase 4: Reconciling 2024 data (final priority)'
SELECT update_reconciliation_phase('phase_4', 'In Progress');

-- Run the 2024 reconciliation script
\i reconcile_2024.sql

-- Update phase completion
SELECT update_reconciliation_phase('phase_4', 'Completed');

\echo 'Phase 4 statistics - 2024 data'
SELECT * FROM get_reconciliation_statistics() WHERE year = 2024;

-- PHASE 5: Final verification
\echo 'Phase 5: Final verification'
SELECT update_reconciliation_phase('phase_5', 'In Progress');

-- Run verification checks
\echo 'Final reconciliation statistics'
SELECT * FROM get_reconciliation_statistics();

-- Calculate overall reconciliation status
WITH reconciliation_summary AS (
    SELECT 
        SUM(curtailment_count) as total_curtailment,
        SUM(bitcoin_count) as total_bitcoin,
        SUM(expected_bitcoin_count) as total_expected
    FROM get_reconciliation_statistics()
)
SELECT 
    total_curtailment,
    total_bitcoin,
    total_expected,
    ROUND(total_bitcoin * 100.0 / total_expected, 2) as overall_completion_percentage,
    CASE 
        WHEN total_bitcoin >= total_expected THEN 'FULLY RECONCILED'
        WHEN total_bitcoin >= total_expected * 0.99 THEN 'NEARLY COMPLETE (>99%)'
        ELSE 'INCOMPLETE'
    END as reconciliation_status
FROM reconciliation_summary;

-- Create missing reconciliation index
CREATE INDEX IF NOT EXISTS hbc_settlement_date_miner_model_idx 
ON historical_bitcoin_calculations(settlement_date, miner_model);

-- Update phase completion
SELECT update_reconciliation_phase('phase_5', 'Completed');

-- Show complete progress summary
\echo 'Reconciliation Process Summary'
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
FROM reconciliation_master_progress
ORDER BY phase;

-- Clean up temporary objects
DROP FUNCTION IF EXISTS update_reconciliation_phase(TEXT, TEXT, INTEGER);
DROP FUNCTION IF EXISTS get_reconciliation_statistics();

\echo 'Reconciliation process completed at: ' `date`