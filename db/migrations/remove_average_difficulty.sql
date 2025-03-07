-- Remove averageDifficulty from bitcoinMonthlySummaries table
ALTER TABLE bitcoin_monthly_summaries
DROP COLUMN average_difficulty;

-- Remove averageDifficulty from bitcoinYearlySummaries table
ALTER TABLE bitcoin_yearly_summaries
DROP COLUMN average_difficulty;