-- Rename farm_id to bmu_id in curtailment_records
ALTER TABLE curtailment_records RENAME COLUMN farm_id TO bmu_id;

-- Rename farm_id to bmu_id in historical_bitcoin_calculations
ALTER TABLE historical_bitcoin_calculations RENAME COLUMN farm_id TO bmu_id;

-- Rename farm_id to bmu_id in materialized view tables (if they exist)
DO $$
BEGIN
  IF EXISTS (SELECT FROM information_schema.columns WHERE table_name = 'settlement_period_mining' AND column_name = 'farm_id') THEN
    ALTER TABLE settlement_period_mining RENAME COLUMN farm_id TO bmu_id;
  END IF;
  
  IF EXISTS (SELECT FROM information_schema.columns WHERE table_name = 'daily_mining_potential' AND column_name = 'farm_id') THEN
    ALTER TABLE daily_mining_potential RENAME COLUMN farm_id TO bmu_id;
  END IF;
  
  IF EXISTS (SELECT FROM information_schema.columns WHERE table_name = 'yearly_mining_potential' AND column_name = 'farm_id') THEN
    ALTER TABLE yearly_mining_potential RENAME COLUMN farm_id TO bmu_id;
  END IF;
END $$;