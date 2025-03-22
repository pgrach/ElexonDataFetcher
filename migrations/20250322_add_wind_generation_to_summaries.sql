-- Add wind generation columns to daily summaries
ALTER TABLE IF EXISTS daily_summaries
ADD COLUMN IF NOT EXISTS total_wind_generation NUMERIC DEFAULT 0,
ADD COLUMN IF NOT EXISTS wind_onshore_generation NUMERIC DEFAULT 0,
ADD COLUMN IF NOT EXISTS wind_offshore_generation NUMERIC DEFAULT 0,
ADD COLUMN IF NOT EXISTS last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add wind generation columns to monthly summaries
ALTER TABLE IF EXISTS monthly_summaries
ADD COLUMN IF NOT EXISTS total_wind_generation NUMERIC DEFAULT 0,
ADD COLUMN IF NOT EXISTS wind_onshore_generation NUMERIC DEFAULT 0,
ADD COLUMN IF NOT EXISTS wind_offshore_generation NUMERIC DEFAULT 0,
ADD COLUMN IF NOT EXISTS last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add wind generation columns to yearly summaries
ALTER TABLE IF EXISTS yearly_summaries
ADD COLUMN IF NOT EXISTS total_wind_generation NUMERIC DEFAULT 0,
ADD COLUMN IF NOT EXISTS wind_onshore_generation NUMERIC DEFAULT 0,
ADD COLUMN IF NOT EXISTS wind_offshore_generation NUMERIC DEFAULT 0,
ADD COLUMN IF NOT EXISTS last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Create indexes on the new columns for better query performance
CREATE INDEX IF NOT EXISTS idx_daily_total_wind ON daily_summaries (total_wind_generation);
CREATE INDEX IF NOT EXISTS idx_monthly_total_wind ON monthly_summaries (total_wind_generation);
CREATE INDEX IF NOT EXISTS idx_yearly_total_wind ON yearly_summaries (total_wind_generation);