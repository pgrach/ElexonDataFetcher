-- Bitcoin calculations for March 21, 2025
-- This script will insert Bitcoin mining potential calculations for all curtailment data on 2025-03-21

-- Clear existing calculations
DELETE FROM historical_bitcoin_calculations
WHERE settlement_date = '2025-03-21';

-- S19J_PRO calculations (100 TH/s at 3250W)
INSERT INTO historical_bitcoin_calculations (
  settlement_date, settlement_period, farm_id, miner_model,
  bitcoin_mined, difficulty, calculated_at
)
SELECT 
  settlement_date,
  settlement_period::integer,
  farm_id,
  'S19J_PRO',
  (ABS(volume::numeric) * 0.007 * (100000000000000::numeric / 113757508810853::numeric))::numeric,
  113757508810853::numeric,
  NOW()
FROM curtailment_records
WHERE settlement_date = '2025-03-21';

-- S9 calculations (13.5 TH/s at 1323W)
INSERT INTO historical_bitcoin_calculations (
  settlement_date, settlement_period, farm_id, miner_model,
  bitcoin_mined, difficulty, calculated_at
)
SELECT 
  settlement_date,
  settlement_period::integer,
  farm_id,
  'S9',
  (ABS(volume::numeric) * 0.0025 * (13500000000000::numeric / 113757508810853::numeric))::numeric,
  113757508810853::numeric,
  NOW()
FROM curtailment_records
WHERE settlement_date = '2025-03-21';

-- M20S calculations (68 TH/s at 3360W)
INSERT INTO historical_bitcoin_calculations (
  settlement_date, settlement_period, farm_id, miner_model,
  bitcoin_mined, difficulty, calculated_at
)
SELECT 
  settlement_date,
  settlement_period::integer,
  farm_id,
  'M20S',
  (ABS(volume::numeric) * 0.005 * (68000000000000::numeric / 113757508810853::numeric))::numeric,
  113757508810853::numeric,
  NOW()
FROM curtailment_records
WHERE settlement_date = '2025-03-21';

-- Verify the calculations
SELECT 
  miner_model, 
  COUNT(*) as record_count,
  ROUND(SUM(bitcoin_mined)::numeric, 8) as total_bitcoin
FROM historical_bitcoin_calculations
WHERE settlement_date = '2025-03-21'
GROUP BY miner_model
ORDER BY miner_model;