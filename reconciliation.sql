-- Reconciliation SQL Reference
-- This file contains useful SQL queries for monitoring and troubleshooting reconciliation

-- Create view for overall reconciliation status
CREATE OR REPLACE VIEW reconciliation_status_view AS
SELECT 
    COUNT(DISTINCT c.id) AS total_curtailment_records,
    COUNT(DISTINCT h.id) AS total_bitcoin_calculations,
    COUNT(DISTINCT c.id) * 3 AS expected_calculations,
    ROUND((COUNT(DISTINCT h.id)::numeric / (COUNT(DISTINCT c.id) * 3)) * 100, 2) AS reconciliation_percentage,
    COUNT(DISTINCT c.id) * 3 - COUNT(DISTINCT h.id) AS missing_calculations
FROM 
    curtailment_records c
LEFT JOIN 
    historical_bitcoin_calculations h ON 
        c.settlement_date = h.settlement_date AND 
        c.settlement_period = h.settlement_period AND 
        c.bmu_id = h.bmu_id;

-- Create view for reconciliation status by date
CREATE OR REPLACE VIEW reconciliation_date_status_view AS
SELECT 
    c.settlement_date AS date,
    COUNT(DISTINCT c.id) AS total_curtailment_records,
    COUNT(DISTINCT h.id) AS total_bitcoin_calculations,
    COUNT(DISTINCT c.id) * 3 AS expected_calculations,
    ROUND((COUNT(DISTINCT h.id)::numeric / (COUNT(DISTINCT c.id) * 3)) * 100, 2) AS reconciliation_percentage,
    COUNT(DISTINCT c.id) * 3 - COUNT(DISTINCT h.id) AS missing_calculations
FROM 
    curtailment_records c
LEFT JOIN 
    historical_bitcoin_calculations h ON 
        c.settlement_date = h.settlement_date AND 
        c.settlement_period = h.settlement_period AND 
        c.bmu_id = h.bmu_id
GROUP BY 
    c.settlement_date
ORDER BY 
    c.settlement_date DESC;

-- Create view for missing calculations by date
CREATE OR REPLACE VIEW missing_calculations_by_date_view AS
WITH expected_calculations AS (
    SELECT 
        c.settlement_date,
        c.settlement_period,
        c.bmu_id,
        'S19J_PRO' AS miner_model
    FROM 
        curtailment_records c
    UNION ALL
    SELECT 
        c.settlement_date,
        c.settlement_period,
        c.bmu_id,
        'S9' AS miner_model
    FROM 
        curtailment_records c
    UNION ALL
    SELECT 
        c.settlement_date,
        c.settlement_period,
        c.bmu_id,
        'M20S' AS miner_model
    FROM 
        curtailment_records c
),
actual_calculations AS (
    SELECT 
        settlement_date,
        settlement_period,
        bmu_id,
        miner_model
    FROM 
        historical_bitcoin_calculations
),
missing_calculations AS (
    SELECT 
        e.settlement_date,
        e.settlement_period,
        e.bmu_id,
        e.miner_model
    FROM 
        expected_calculations e
    LEFT JOIN 
        actual_calculations a ON 
            e.settlement_date = a.settlement_date AND 
            e.settlement_period = a.settlement_period AND 
            e.bmu_id = a.bmu_id AND 
            e.miner_model = a.miner_model
    WHERE 
        a.settlement_date IS NULL
)
SELECT 
    settlement_date AS date,
    COUNT(*) AS missing_count,
    COUNT(DISTINCT settlement_period) AS missing_periods,
    COUNT(DISTINCT bmu_id) AS missing_farms,
    STRING_AGG(DISTINCT miner_model, ', ') AS missing_models
FROM 
    missing_calculations
GROUP BY 
    settlement_date
ORDER BY 
    settlement_date DESC;

-- Missing calculations for a specific date
SELECT 
    c.settlement_date, 
    c.settlement_period, 
    c.bmu_id,
    m.miner_model,
    CASE WHEN h.id IS NULL THEN 'Missing' ELSE 'Present' END AS status
FROM 
    curtailment_records c
CROSS JOIN 
    (SELECT unnest(ARRAY['S19J_PRO', 'S9', 'M20S']) AS miner_model) m
LEFT JOIN 
    historical_bitcoin_calculations h ON 
        c.settlement_date = h.settlement_date AND 
        c.settlement_period = h.settlement_period AND 
        c.bmu_id = h.bmu_id AND
        m.miner_model = h.miner_model
WHERE 
    c.settlement_date = '2025-02-25' -- Replace with date of interest
    AND h.id IS NULL
ORDER BY 
    c.settlement_period, 
    c.bmu_id, 
    m.miner_model;

-- Reconciliation status by miner model
SELECT 
    h.miner_model,
    COUNT(DISTINCT h.id) AS calculations,
    (SELECT COUNT(DISTINCT c.id) FROM curtailment_records c) AS total_curtailment_records,
    ROUND((COUNT(DISTINCT h.id)::numeric / (SELECT COUNT(DISTINCT c.id) FROM curtailment_records c)) * 100, 2) AS completion_percentage
FROM 
    historical_bitcoin_calculations h
GROUP BY 
    h.miner_model
ORDER BY 
    completion_percentage DESC;

-- Find days with partial reconciliation (some but not all calculations)
WITH date_stats AS (
    SELECT 
        c.settlement_date,
        COUNT(DISTINCT c.id) * 3 AS expected,
        COUNT(DISTINCT h.id) AS actual
    FROM 
        curtailment_records c
    LEFT JOIN 
        historical_bitcoin_calculations h ON 
            c.settlement_date = h.settlement_date AND 
            c.settlement_period = h.settlement_period AND 
            c.bmu_id = h.bmu_id
    GROUP BY 
        c.settlement_date
)
SELECT 
    settlement_date AS date,
    expected,
    actual,
    expected - actual AS missing,
    ROUND((actual::numeric / expected) * 100, 2) AS completion_percentage
FROM 
    date_stats
WHERE 
    actual > 0 AND actual < expected
ORDER BY 
    settlement_date DESC;

-- Reconciliation status by month
SELECT 
    TO_CHAR(c.settlement_date, 'YYYY-MM') AS month,
    COUNT(DISTINCT c.id) AS total_curtailment_records,
    COUNT(DISTINCT h.id) AS total_bitcoin_calculations,
    COUNT(DISTINCT c.id) * 3 AS expected_calculations,
    ROUND((COUNT(DISTINCT h.id)::numeric / (COUNT(DISTINCT c.id) * 3)) * 100, 2) AS reconciliation_percentage
FROM 
    curtailment_records c
LEFT JOIN 
    historical_bitcoin_calculations h ON 
        c.settlement_date = h.settlement_date AND 
        c.settlement_period = h.settlement_period AND 
        c.bmu_id = h.bmu_id
GROUP BY 
    TO_CHAR(c.settlement_date, 'YYYY-MM')
ORDER BY 
    month DESC;

-- Check for duplicate calculations (should not exist)
SELECT 
    settlement_date, 
    settlement_period, 
    bmu_id, 
    miner_model, 
    COUNT(*) AS count
FROM 
    historical_bitcoin_calculations
GROUP BY 
    settlement_date, 
    settlement_period, 
    bmu_id, 
    miner_model
HAVING 
    COUNT(*) > 1
ORDER BY 
    settlement_date DESC;

-- Find records with missing difficulty values
SELECT 
    h.settlement_date,
    COUNT(*) AS records_with_null_difficulty
FROM 
    historical_bitcoin_calculations h
WHERE 
    h.difficulty IS NULL
GROUP BY 
    h.settlement_date
ORDER BY 
    h.settlement_date DESC;

-- Check reconciliation processing time trends
WITH time_data AS (
    SELECT 
        settlement_date,
        MAX(created_at) - MIN(created_at) AS processing_time
    FROM 
        historical_bitcoin_calculations
    GROUP BY 
        settlement_date
)
SELECT 
    settlement_date,
    EXTRACT(EPOCH FROM processing_time) AS processing_seconds
FROM 
    time_data
ORDER BY 
    settlement_date DESC
LIMIT 30;

-- Show recent reconciliation activity
SELECT 
    DATE_TRUNC('hour', created_at) AS hour,
    COUNT(*) AS records_processed
FROM 
    historical_bitcoin_calculations
WHERE 
    created_at > NOW() - INTERVAL '3 days'
GROUP BY 
    DATE_TRUNC('hour', created_at)
ORDER BY 
    hour DESC;

-- Performance optimization: create indexes to speed up reconciliation queries
CREATE INDEX IF NOT EXISTS idx_curtailment_date_period_bmu 
ON curtailment_records (settlement_date, settlement_period, bmu_id);

CREATE INDEX IF NOT EXISTS idx_historical_bitcoin_date_period_bmu_model 
ON historical_bitcoin_calculations (settlement_date, settlement_period, bmu_id, miner_model);

CREATE INDEX IF NOT EXISTS idx_curtailment_date 
ON curtailment_records (settlement_date);

CREATE INDEX IF NOT EXISTS idx_historical_bitcoin_date 
ON historical_bitcoin_calculations (settlement_date);