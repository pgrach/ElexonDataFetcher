-- Special Reconciliation Script for 2023
-- This script is optimized specifically for 2023 which has nearly all data missing (0.06% completion)

-- Load the core functions
\i full_reconciliation_implementation.sql

-- Set the correct difficulty value for 2023
\echo 'Setting 2023 difficulty value to 37935772752142'
SET LOCAL reconciliation.difficulty_2023 = 37935772752142;

-- Check initial status
\echo 'Initial 2023 reconciliation status:'
SELECT * FROM check_year_status('2023');

-- Define priority months based on curtailment count
WITH monthly_data AS (
    WITH monthly_curtailment AS (
      SELECT 
        to_char(settlement_date, 'YYYY-MM') as year_month,
        COUNT(*) as curtailment_count
      FROM curtailment_records
      WHERE EXTRACT(YEAR FROM settlement_date) = 2023
      GROUP BY to_char(settlement_date, 'YYYY-MM')
    ),
    monthly_bitcoin AS (
      SELECT 
        to_char(settlement_date, 'YYYY-MM') as year_month,
        COUNT(*) as bitcoin_count
      FROM historical_bitcoin_calculations
      WHERE EXTRACT(YEAR FROM settlement_date) = 2023
      GROUP BY to_char(settlement_date, 'YYYY-MM')
    )
    SELECT 
      mc.year_month,
      mc.curtailment_count,
      COALESCE(mb.bitcoin_count, 0) as bitcoin_count,
      mc.curtailment_count * 3 as expected_bitcoin_count,
      CASE
        WHEN COALESCE(mb.bitcoin_count, 0) = 0 THEN 'Missing'
        WHEN COALESCE(mb.bitcoin_count, 0) < mc.curtailment_count * 3 THEN 'Incomplete'
        ELSE 'Complete'
      END as status,
      ROUND(
        COALESCE(mb.bitcoin_count, 0)::NUMERIC * 100 / 
        (mc.curtailment_count * 3)::NUMERIC,
        2
      ) as completion_percentage
    FROM monthly_curtailment mc
    LEFT JOIN monthly_bitcoin mb ON mc.year_month = mb.year_month
)
SELECT * 
FROM monthly_data 
ORDER BY 
    CASE 
        WHEN status = 'Missing' THEN 1
        ELSE 2
    END,
    curtailment_count DESC;

-- Process the highest priority months first
-- These are months with most curtailment records
\echo 'Processing highest priority months in 2023'

-- Phase 1: Process October 2023 (highest curtailment at 22,754 records)
\echo 'Phase 1: Processing 2023-10'
SELECT reconcile_month('2023-10', 37935772752142, 10);
SELECT * FROM check_month_status('2023-10');

-- Phase 2: Process September 2023 (20,136 records)
\echo 'Phase 2: Processing 2023-09'
SELECT reconcile_month('2023-09', 37935772752142, 10);
SELECT * FROM check_month_status('2023-09');

-- Phase 3: Process July 2023 (12,213 records)
\echo 'Phase 3: Processing 2023-07'
SELECT reconcile_month('2023-07', 37935772752142, 10);
SELECT * FROM check_month_status('2023-07');

-- Phase 4: Process August 2023 (11,410 records)
\echo 'Phase 4: Processing 2023-08'
SELECT reconcile_month('2023-08', 37935772752142, 10);
SELECT * FROM check_month_status('2023-08');

-- Phase 5: Process November 2023 (10,805 records)
\echo 'Phase 5: Processing 2023-11'
SELECT reconcile_month('2023-11', 37935772752142, 10);
SELECT * FROM check_month_status('2023-11');

-- Phase 6: Process February 2023 (9,744 records)
\echo 'Phase 6: Processing 2023-02'
SELECT reconcile_month('2023-02', 37935772752142, 10);
SELECT * FROM check_month_status('2023-02');

-- Phase 7: Process December 2023 (13,851 records)
\echo 'Phase 7: Processing 2023-12'
SELECT reconcile_month('2023-12', 37935772752142, 10);
SELECT * FROM check_month_status('2023-12');

-- Phase 8: Process April 2023 (14,507 records)
\echo 'Phase 8: Processing 2023-04'
SELECT reconcile_month('2023-04', 37935772752142, 10);
SELECT * FROM check_month_status('2023-04');

-- Phase 9: Process May 2023 (4,172 records)
\echo 'Phase 9: Processing 2023-05'
SELECT reconcile_month('2023-05', 37935772752142, 10);
SELECT * FROM check_month_status('2023-05');

-- Phase 10: Process March 2023 (3,541 records)
\echo 'Phase 10: Processing 2023-03'
SELECT reconcile_month('2023-03', 37935772752142, 10);
SELECT * FROM check_month_status('2023-03');

-- Phase 11: Process January 2023 (6,538 records)
\echo 'Phase 11: Processing 2023-01'
SELECT reconcile_month('2023-01', 37935772752142, 10);
SELECT * FROM check_month_status('2023-01');

-- Phase 12: Process June 2023 (1,028 records)
\echo 'Phase 12: Processing 2023-06'
SELECT reconcile_month('2023-06', 37935772752142, 10);
SELECT * FROM check_month_status('2023-06');

-- Final check of 2023 status
\echo 'Final 2023 reconciliation status:'
SELECT * FROM check_year_status('2023');

-- Check to see if any months still need processing
WITH monthly_data AS (
    WITH monthly_curtailment AS (
      SELECT 
        to_char(settlement_date, 'YYYY-MM') as year_month,
        COUNT(*) as curtailment_count
      FROM curtailment_records
      WHERE EXTRACT(YEAR FROM settlement_date) = 2023
      GROUP BY to_char(settlement_date, 'YYYY-MM')
    ),
    monthly_bitcoin AS (
      SELECT 
        to_char(settlement_date, 'YYYY-MM') as year_month,
        COUNT(*) as bitcoin_count
      FROM historical_bitcoin_calculations
      WHERE EXTRACT(YEAR FROM settlement_date) = 2023
      GROUP BY to_char(settlement_date, 'YYYY-MM')
    )
    SELECT 
      mc.year_month,
      mc.curtailment_count,
      COALESCE(mb.bitcoin_count, 0) as bitcoin_count,
      mc.curtailment_count * 3 as expected_bitcoin_count,
      CASE
        WHEN COALESCE(mb.bitcoin_count, 0) = 0 THEN 'Missing'
        WHEN COALESCE(mb.bitcoin_count, 0) < mc.curtailment_count * 3 THEN 'Incomplete'
        ELSE 'Complete'
      END as status,
      ROUND(
        COALESCE(mb.bitcoin_count, 0)::NUMERIC * 100 / 
        (mc.curtailment_count * 3)::NUMERIC,
        2
      ) as completion_percentage
    FROM monthly_curtailment mc
    LEFT JOIN monthly_bitcoin mb ON mc.year_month = mb.year_month
)
SELECT * 
FROM monthly_data 
WHERE status != 'Complete'
ORDER BY completion_percentage;