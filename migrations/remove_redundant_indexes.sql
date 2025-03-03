-- Migration to remove redundant indexes
-- This will improve database performance by reducing index maintenance overhead

-- First, remove duplicate indexes on curtailment_records table
DROP INDEX IF EXISTS idx_curtailment_date;
DROP INDEX IF EXISTS idx_curtailment_settlement_date;
-- Keep curtailment_settlement_date_idx as our primary index for settlement_date

-- Next, remove duplicate indexes on historical_bitcoin_calculations table
DROP INDEX IF EXISTS idx_bitcoin_calc_date;
DROP INDEX IF EXISTS idx_bitcoin_settlement_date;
DROP INDEX IF EXISTS idx_historical_bitcoin_settlement_date;
-- Keep bitcoin_settlement_date_idx as our primary index for settlement_date

DROP INDEX IF EXISTS idx_bitcoin_calc_date_model;
DROP INDEX IF EXISTS idx_bitcoin_settlement_date_model;
-- Keep bitcoin_settlement_date_model_idx as our primary index for (settlement_date, miner_model)

-- Remove duplicate unique constraint
DROP INDEX IF EXISTS historical_bitcoin_calculations_unique_calculation;
-- Keep historical_bitcoin_calculations_unique as the sole unique constraint