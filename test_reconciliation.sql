-- Test reconciliation on a small batch from 2023
-- This will verify our approach works correctly before running the full reconciliation

-- First, initialize the functions
\i full_reconciliation_implementation.sql

-- Check the initial status of 2023
\echo 'Initial status of 2023:'
SELECT * FROM check_year_status('2023');

-- Test with the most critical month: October 2023 (highest curtailment count of 22,754)
\echo 'Initial status of October 2023:'
SELECT * FROM check_month_status('2023-10');

-- Process a small batch of October 2023 dates (limit to 5 days as a test)
-- Instead of full reconciliation, we'll process specific high-priority dates
\echo 'Processing 2023-10 with 5 days limit and batch size of 2:'
BEGIN;

-- Set the specific difficulty value for 2023
SET LOCAL reconciliation.difficulty_2023 = 37935772752142;

-- Find top 5 days with highest curtailment in October 2023
WITH october_top_days AS (
    SELECT 
        settlement_date,
        COUNT(*) as curtailment_count
    FROM curtailment_records
    WHERE to_char(settlement_date, 'YYYY-MM') = '2023-10'
    GROUP BY settlement_date
    ORDER BY curtailment_count DESC
    LIMIT 5
)
SELECT to_char(settlement_date, 'YYYY-MM-DD') as date, curtailment_count 
FROM october_top_days;

-- Process each of these days
SELECT reconcile_month('2023-10', 37935772752142, 2, 5);

COMMIT;

-- Check the status after processing
\echo 'Status of October 2023 after processing:'
SELECT * FROM check_month_status('2023-10');

-- Check overall 2023 status
\echo 'Overall 2023 status after test processing:'
SELECT * FROM check_year_status('2023');

-- Check overall reconciliation status
\echo 'Overall reconciliation status:'
SELECT * FROM check_overall_status();

-- Show the reconciliation progress
\echo 'Reconciliation progress details:'
SELECT * FROM reconciliation_progress ORDER BY start_time DESC LIMIT 10;