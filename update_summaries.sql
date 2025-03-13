-- Update Monthly Bitcoin Summaries for March 2025
WITH march_totals AS (
  SELECT
    miner_model,
    SUM(bitcoin_mined::numeric) as total_bitcoin
  FROM historical_bitcoin_calculations
  WHERE settlement_date >= '2025-03-01' AND settlement_date <= '2025-03-31'
  GROUP BY miner_model
)
UPDATE bitcoin_monthly_summaries bms
SET 
  bitcoin_mined = mt.total_bitcoin,
  updated_at = NOW()
FROM march_totals mt
WHERE bms.year_month = '2025-03' AND bms.miner_model = mt.miner_model;

-- Update Yearly Bitcoin Summaries for 2025
WITH yearly_totals AS (
  SELECT
    miner_model,
    SUM(bitcoin_mined::numeric) as total_bitcoin
  FROM bitcoin_monthly_summaries
  WHERE year_month LIKE '2025-%'
  GROUP BY miner_model
)
UPDATE bitcoin_yearly_summaries bys
SET 
  bitcoin_mined = yt.total_bitcoin,
  updated_at = NOW()
FROM yearly_totals yt
WHERE bys.year = '2025' AND bys.miner_model = yt.miner_model;

-- Report updated summaries
SELECT 'Monthly Summaries' as summary_type, year_month, miner_model, bitcoin_mined
FROM bitcoin_monthly_summaries
WHERE year_month = '2025-03'
ORDER BY miner_model;

SELECT 'Yearly Summaries' as summary_type, year, miner_model, bitcoin_mined
FROM bitcoin_yearly_summaries
WHERE year = '2025'
ORDER BY miner_model;