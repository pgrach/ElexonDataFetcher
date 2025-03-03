-- Migration: Add materialized view tables for mining potential
-- This migration creates three tables that function as materialized views:
-- 1. settlement_period_mining - Period-level mining data 
-- 2. daily_mining_potential - Daily aggregated mining data
-- 3. yearly_mining_potential - Yearly aggregated mining data

-- Start transaction
BEGIN;

-- Create settlement_period_mining table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'settlement_period_mining') THEN
        CREATE TABLE settlement_period_mining (
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
        CREATE INDEX idx_settlement_period_mining_date ON settlement_period_mining(settlement_date);
        CREATE INDEX idx_settlement_period_mining_farm ON settlement_period_mining(farm_id);
        CREATE INDEX idx_settlement_period_mining_model ON settlement_period_mining(miner_model);
        CREATE INDEX idx_settlement_period_mining_date_period ON settlement_period_mining(settlement_date, settlement_period);
        
        RAISE NOTICE 'Created settlement_period_mining table with indexes';
    ELSE
        RAISE NOTICE 'settlement_period_mining table already exists, skipping creation';
    END IF;
END
$$;

-- Create daily_mining_potential table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'daily_mining_potential') THEN
        CREATE TABLE daily_mining_potential (
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
        CREATE INDEX idx_daily_mining_potential_date ON daily_mining_potential(summary_date);
        CREATE INDEX idx_daily_mining_potential_farm ON daily_mining_potential(farm_id);
        CREATE INDEX idx_daily_mining_potential_model ON daily_mining_potential(miner_model);
        
        RAISE NOTICE 'Created daily_mining_potential table with indexes';
    ELSE
        RAISE NOTICE 'daily_mining_potential table already exists, skipping creation';
    END IF;
END
$$;

-- Create yearly_mining_potential table if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'yearly_mining_potential') THEN
        CREATE TABLE yearly_mining_potential (
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
        CREATE INDEX idx_yearly_mining_potential_year ON yearly_mining_potential(year);
        CREATE INDEX idx_yearly_mining_potential_farm ON yearly_mining_potential(farm_id);
        CREATE INDEX idx_yearly_mining_potential_model ON yearly_mining_potential(miner_model);
        
        RAISE NOTICE 'Created yearly_mining_potential table with indexes';
    ELSE
        RAISE NOTICE 'yearly_mining_potential table already exists, skipping creation';
    END IF;
END
$$;

-- Create trigger functions for updated_at timestamp
DO $$
BEGIN
    CREATE OR REPLACE FUNCTION update_modified_column()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    -- Add triggers if they don't exist
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_settlement_period_mining_timestamp') THEN
        CREATE TRIGGER update_settlement_period_mining_timestamp
        BEFORE UPDATE ON settlement_period_mining
        FOR EACH ROW EXECUTE FUNCTION update_modified_column();
        
        RAISE NOTICE 'Created trigger for settlement_period_mining';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_daily_mining_potential_timestamp') THEN
        CREATE TRIGGER update_daily_mining_potential_timestamp
        BEFORE UPDATE ON daily_mining_potential
        FOR EACH ROW EXECUTE FUNCTION update_modified_column();
        
        RAISE NOTICE 'Created trigger for daily_mining_potential';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_yearly_mining_potential_timestamp') THEN
        CREATE TRIGGER update_yearly_mining_potential_timestamp
        BEFORE UPDATE ON yearly_mining_potential
        FOR EACH ROW EXECUTE FUNCTION update_modified_column();
        
        RAISE NOTICE 'Created trigger for yearly_mining_potential';
    END IF;
END
$$;

-- Commit transaction
COMMIT;

-- Final success message
DO $$
BEGIN
    RAISE NOTICE 'Migration completed successfully';
END
$$;