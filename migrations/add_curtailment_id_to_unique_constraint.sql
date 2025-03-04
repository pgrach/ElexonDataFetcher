-- Add curtailment_id to unique constraints
BEGIN;

-- Drop existing unique constraints
ALTER TABLE historical_bitcoin_calculations 
DROP CONSTRAINT IF EXISTS historical_bitcoin_calculations_unique_calculation;

ALTER TABLE historical_bitcoin_calculations 
DROP CONSTRAINT IF EXISTS historical_bitcoin_calculations_unique;

-- Add new unique constraint with curtailment_id
ALTER TABLE historical_bitcoin_calculations 
ADD CONSTRAINT historical_bitcoin_calculations_unique_constraint 
UNIQUE (settlement_date, settlement_period, farm_id, miner_model, curtailment_id);

COMMIT;