-- Create settlement_period_mining table if it doesn't exist
CREATE TABLE IF NOT EXISTS settlement_period_mining (
    id SERIAL PRIMARY KEY,
    settlement_date DATE NOT NULL,
    settlement_period INTEGER NOT NULL,
    farm_id TEXT NOT NULL,
    miner_model TEXT NOT NULL,
    curtailed_energy DECIMAL(15,6) NOT NULL,
    bitcoin_mined DECIMAL(20,10) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(settlement_date, settlement_period, farm_id, miner_model)
);

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_settlement_period_mining_date ON settlement_period_mining(settlement_date);
CREATE INDEX IF NOT EXISTS idx_settlement_period_mining_farm ON settlement_period_mining(farm_id);
CREATE INDEX IF NOT EXISTS idx_settlement_period_mining_model ON settlement_period_mining(miner_model);
CREATE INDEX IF NOT EXISTS idx_settlement_period_mining_date_period ON settlement_period_mining(settlement_date, settlement_period);

-- Create daily_mining_potential table if it doesn't exist
CREATE TABLE IF NOT EXISTS daily_mining_potential (
    id SERIAL PRIMARY KEY,
    summary_date DATE NOT NULL,
    farm_id TEXT NOT NULL,
    miner_model TEXT NOT NULL,
    total_curtailed_energy TEXT NOT NULL,
    total_bitcoin_mined TEXT NOT NULL,
    average_value TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(summary_date, farm_id, miner_model)
);

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_daily_mining_potential_date ON daily_mining_potential(summary_date);
CREATE INDEX IF NOT EXISTS idx_daily_mining_potential_farm ON daily_mining_potential(farm_id);
CREATE INDEX IF NOT EXISTS idx_daily_mining_potential_model ON daily_mining_potential(miner_model);

-- Create yearly_mining_potential table if it doesn't exist
CREATE TABLE IF NOT EXISTS yearly_mining_potential (
    id SERIAL PRIMARY KEY,
    year TEXT NOT NULL,
    farm_id TEXT NOT NULL,
    miner_model TEXT NOT NULL,
    total_curtailed_energy TEXT NOT NULL,
    total_bitcoin_mined TEXT NOT NULL,
    average_value TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(year, farm_id, miner_model)
);

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_yearly_mining_potential_year ON yearly_mining_potential(year);
CREATE INDEX IF NOT EXISTS idx_yearly_mining_potential_farm ON yearly_mining_potential(farm_id);
CREATE INDEX IF NOT EXISTS idx_yearly_mining_potential_model ON yearly_mining_potential(miner_model);

-- Create trigger function for updated_at timestamp
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Add triggers if they don't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_settlement_period_mining_timestamp') THEN
        CREATE TRIGGER update_settlement_period_mining_timestamp
        BEFORE UPDATE ON settlement_period_mining
        FOR EACH ROW EXECUTE FUNCTION update_modified_column();
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_daily_mining_potential_timestamp') THEN
        CREATE TRIGGER update_daily_mining_potential_timestamp
        BEFORE UPDATE ON daily_mining_potential
        FOR EACH ROW EXECUTE FUNCTION update_modified_column();
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_yearly_mining_potential_timestamp') THEN
        CREATE TRIGGER update_yearly_mining_potential_timestamp
        BEFORE UPDATE ON yearly_mining_potential
        FOR EACH ROW EXECUTE FUNCTION update_modified_column();
    END IF;
END
$$;