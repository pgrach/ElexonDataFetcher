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