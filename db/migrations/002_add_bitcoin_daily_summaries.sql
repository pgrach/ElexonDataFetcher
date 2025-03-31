-- Create Bitcoin Daily Summaries Table if it doesn't exist
CREATE TABLE IF NOT EXISTS bitcoin_daily_summaries (
  id SERIAL PRIMARY KEY,
  summary_date DATE NOT NULL,
  miner_model TEXT NOT NULL,
  bitcoin_mined NUMERIC NOT NULL,
  value_at_mining NUMERIC NOT NULL,
  average_difficulty NUMERIC,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add unique constraint to prevent duplicates
CREATE UNIQUE INDEX IF NOT EXISTS bitcoin_daily_summaries_date_model_idx ON bitcoin_daily_summaries (summary_date, miner_model);