# Data Reingest Guide

This guide explains how to use the data reingest reference file to fix incomplete or corrupted data for any date in the system. This process is particularly useful when you need to ensure all 48 settlement periods for a specific date are properly ingested from the Elexon API.

## Problem Background

When settlement period data is incomplete for a date (for example, having only some of the 48 periods), it causes several issues:

1. **Incomplete Data Visualization**: Charts will show gaps or partial data for the affected date
2. **Inaccurate Summaries**: Daily, monthly, and yearly summaries may be incorrect
3. **Missing Bitcoin Calculations**: Bitcoin mining calculations will be incomplete

## Solution Overview

The `data_reingest_reference.ts` script provides a complete solution that:

1. Clears existing data for the target date
2. Fetches all 48 settlement periods from the Elexon API
3. Processes the data in manageable batches to avoid timeouts
4. Updates all relevant summary tables (daily, monthly, yearly)
5. Recalculates all Bitcoin mining potential values
6. Verifies the data integrity after processing

## Usage Instructions

1. **Identify the problematic date** you need to fix (for example, if charts show incomplete data)

2. **Run the script** with the target date:
   ```bash
   npx tsx data_reingest_reference.ts 2025-04-10
   ```

3. **Monitor the logs** to track progress:
   - The script generates a log file named `reingest_YYYY-MM-DD.log` 
   - The console will also display progress information
   - Look for the "Update successful" message at the end

4. **Verify the results**:
   - Check that the daily summary has been updated with correct values
   - Verify that Bitcoin calculations exist for all periods
   - Confirm that charts and data visualizations now show complete data

## Key Performance Numbers

For reference, a typical complete reingest of March 28, 2025 processed:
- 4,684 curtailment records
- 99,904 MWh of curtailed energy
- £3,784,089.62 in payments
- 48 complete settlement periods
- Data for all 24 hours (2 settlement periods per hour)

## Behind The Scenes

The script performs these key operations:

1. **Database Clearing**: Removes existing records for the target date from:
   - `curtailment_records`
   - `historical_bitcoin_calculations`
   - `daily_summaries`

2. **Data Processing**:
   - Processes periods in batches of 6 to avoid timeouts
   - Maps BMU IDs to farm IDs using the mapping file
   - Calculates volumes and payments for each record

3. **Summary Updates**:
   - Updates daily summary for the target date
   - Recalculates monthly summary for the affected month
   - Updates yearly summary for the affected year

4. **Bitcoin Recalculations**:
   - Recalculates Bitcoin mining potential for each farm and period
   - Updates monthly and yearly Bitcoin summaries

## Troubleshooting

- **API Rate Limiting**: If you encounter API rate limiting issues, adjust the `API_THROTTLE_MS` value
- **Database Timeouts**: For very large datasets, reduce the `BATCH_SIZE` to process fewer periods at once
- **Missing Farm Mappings**: Ensure the BMU mapping file is up-to-date with all required wind farms

## Alternative Method: SQL-Based Data Reingest

If you're encountering persistent issues with the TypeScript reingest script, you can use a direct SQL approach which is more efficient for large datasets:

### 1. Generate Test Data (When API Access Is Not Available)

For development/testing purposes when the Elexon API key is not available, the following approach can be used:

```bash
# Run the fast reingest script for a specific date
npx tsx fast_reingest_march_21.ts
```

This script:
- Clears existing data for the date
- Generates representative test data for all 48 settlement periods
- Updates summary tables correctly
- Handles 5 representative wind farms with typical curtailment patterns

### 2. Update Bitcoin Calculations Separately

For better performance and reliability, use SQL directly to update Bitcoin calculations:

```sql
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

-- Add similar INSERT statements for other miner models (S9, M20S, etc.)
```

### 3. Verify All Data Components

Use this comprehensive verification query to ensure all data components are properly updated:

```sql
WITH curtailment_stats AS (
  SELECT 
    settlement_date,
    COUNT(*) AS record_count,
    COUNT(DISTINCT settlement_period) AS period_count,
    ROUND(SUM(ABS(volume::numeric))::numeric, 2) AS total_volume,
    ROUND(SUM(payment::numeric)::numeric, 2) AS total_payment
  FROM curtailment_records
  WHERE settlement_date = '2025-03-21'
  GROUP BY settlement_date
),
bitcoin_stats AS (
  SELECT 
    settlement_date,
    COUNT(*) AS record_count,
    COUNT(DISTINCT miner_model) AS model_count,
    ROUND(SUM(bitcoin_mined::numeric)::numeric, 8) AS total_bitcoin
  FROM historical_bitcoin_calculations
  WHERE settlement_date = '2025-03-21'
  GROUP BY settlement_date
),
summary_stats AS (
  SELECT
    summary_date AS date,
    total_curtailed_energy,
    total_payment
  FROM daily_summaries
  WHERE summary_date = '2025-03-21'
)
SELECT 
  c.settlement_date AS date,
  c.record_count AS curtailment_records,
  c.period_count AS periods,
  c.total_volume AS total_curtailed_mwh,
  ABS(c.total_payment) AS total_payment_gbp,
  b.record_count AS bitcoin_records,
  b.model_count AS miner_models,
  b.total_bitcoin AS total_bitcoin,
  s.total_curtailed_energy AS summary_energy,
  ABS(s.total_payment) AS summary_payment
FROM curtailment_stats c
JOIN bitcoin_stats b ON c.settlement_date = b.settlement_date
JOIN summary_stats s ON c.settlement_date = s.date;
```

## Example Successful Output

A successful reingest should show verification results similar to:

```
Verification Check for 2025-03-28: {
  "records": "4684",
  "periods": "48",
  "volume": "99904.22",
  "payment": "-3784089.62"
}
Update successful at 2025-03-28T12:34:56.789Z
```