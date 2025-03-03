-- Check if settlement_period_mining table exists
SELECT EXISTS (
    SELECT FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_name = 'settlement_period_mining'
) AS settlement_period_mining_exists \gset

-- Check if daily_mining_potential table exists
SELECT EXISTS (
    SELECT FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_name = 'daily_mining_potential'
) AS daily_mining_potential_exists \gset

-- Check if yearly_mining_potential table exists
SELECT EXISTS (
    SELECT FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_name = 'yearly_mining_potential'
) AS yearly_mining_potential_exists \gset

-- Create the settlement_period_mining table if it doesn't exist
DO $$
BEGIN
    IF :'settlement_period_mining_exists' = 'f' THEN
        CREATE TABLE settlement_period_mining (
            id SERIAL PRIMARY KEY,
            settlement_date DATE NOT NULL,
            settlement_period INTEGER NOT NULL,
            farm_id TEXT NOT NULL,
            miner_model TEXT NOT NULL,
            curtailed_energy NUMERIC NOT NULL,
            bitcoin_mined NUMERIC NOT NULL,
            value_at_price NUMERIC,
            price NUMERIC,
            difficulty NUMERIC NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE INDEX idx_settlement_period_mining_date ON settlement_period_mining(settlement_date);
        CREATE INDEX idx_settlement_period_mining_farm ON settlement_period_mining(farm_id);
        CREATE INDEX idx_settlement_period_mining_model ON settlement_period_mining(miner_model);
        
        RAISE NOTICE 'Created settlement_period_mining table';
    ELSE
        RAISE NOTICE 'settlement_period_mining table already exists';
    END IF;
END
$$;

-- Create the daily_mining_potential table if it doesn't exist
DO $$
BEGIN
    IF :'daily_mining_potential_exists' = 'f' THEN
        CREATE TABLE daily_mining_potential (
            id SERIAL PRIMARY KEY,
            summary_date DATE NOT NULL,
            farm_id TEXT NOT NULL,
            miner_model TEXT NOT NULL,
            total_curtailed_energy NUMERIC NOT NULL,
            total_bitcoin_mined NUMERIC NOT NULL,
            average_value NUMERIC,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE INDEX idx_daily_mining_potential_date ON daily_mining_potential(summary_date);
        CREATE INDEX idx_daily_mining_potential_farm ON daily_mining_potential(farm_id);
        CREATE INDEX idx_daily_mining_potential_model ON daily_mining_potential(miner_model);
        
        RAISE NOTICE 'Created daily_mining_potential table';
    ELSE
        RAISE NOTICE 'daily_mining_potential table already exists';
    END IF;
END
$$;

-- Create the yearly_mining_potential table if it doesn't exist
DO $$
BEGIN
    IF :'yearly_mining_potential_exists' = 'f' THEN
        CREATE TABLE yearly_mining_potential (
            id SERIAL PRIMARY KEY,
            year TEXT NOT NULL,
            farm_id TEXT NOT NULL,
            miner_model TEXT NOT NULL,
            total_curtailed_energy NUMERIC NOT NULL,
            total_bitcoin_mined NUMERIC NOT NULL,
            average_value NUMERIC,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE INDEX idx_yearly_mining_potential_year ON yearly_mining_potential(year);
        CREATE INDEX idx_yearly_mining_potential_farm ON yearly_mining_potential(farm_id);
        CREATE INDEX idx_yearly_mining_potential_model ON yearly_mining_potential(miner_model);
        
        RAISE NOTICE 'Created yearly_mining_potential table';
    ELSE
        RAISE NOTICE 'yearly_mining_potential table already exists';
    END IF;
END
$$;