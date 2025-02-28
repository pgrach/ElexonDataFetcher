-- Run a targeted test to verify the reconciliation process
\i full_reconciliation_script.sql

-- Check current reconciliation status
\echo '------ Current Reconciliation Status ------'
SELECT jsonb_pretty(check_reconciliation_status());

-- Process a few test dates
\echo '\n------ Process Test Date: 2023-01-15 ------'
SELECT jsonb_pretty(process_single_date('2023-01-15', 37935772752142, 'TEST-2023-01-15'));

\echo '\n------ Process Test Date: 2022-03-15 ------'
SELECT jsonb_pretty(process_single_date('2022-03-15', 25000000000000, 'TEST-2022-03-15'));

\echo '\n------ Process Test Date: 2025-02-28 ------'
SELECT jsonb_pretty(process_single_date('2025-02-28', 108105433845147, 'TEST-2025-02-28'));

-- Check status after test dates
\echo '\n------ Reconciliation Status After Test Dates ------'
SELECT jsonb_pretty(check_reconciliation_status());

-- Process a test month with limited days (October 2023, 3 days)
\echo '\n------ Process Test Month: 2023-10 (3 days) ------'
SELECT jsonb_pretty(process_month('2023-10', 37935772752142, 3, 'TEST-2023-10'));

-- Process a test year with limited months (2023, 1 month, 2 days per month)
\echo '\n------ Process Test Year: 2023 (1 month, 2 days) ------'
SELECT jsonb_pretty(process_year('2023', 37935772752142, 1, 2, 'TEST-2023'));

-- Check tracking records
\echo '\n------ Reconciliation Tracking Records ------'
SELECT 
  id, 
  batch_id, 
  year_value, 
  year_month, 
  process_date, 
  curtailment_count, 
  initial_bitcoin_count, 
  final_bitcoin_count, 
  records_added, 
  status, 
  EXTRACT(EPOCH FROM (process_end - process_start)) * 1000 as duration_ms
FROM reconciliation_tracking
ORDER BY process_start DESC
LIMIT 10;

-- Check final status
\echo '\n------ Final Reconciliation Status ------'
SELECT jsonb_pretty(check_reconciliation_status());