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

Here are reference numbers from successful reingests:

### March 28, 2025
- 4,684 curtailment records
- 99,904 MWh of curtailed energy
- £3,784,089.62 in payments
- 48 complete settlement periods

### March 22, 2025
- [Final record count] curtailment records
- [Final MWh] MWh of curtailed energy
- £[Final payment] in payments
- 48 complete settlement periods

### March 21, 2025
- 2,015 curtailment records
- 50,518.72 MWh of curtailed energy
- £1,240,439.58 in payments
- 48 complete settlement periods

## Behind The Scenes

The script performs these key operations:

1. **Database Clearing**: Removes existing records for the target date from:
   - `curtailment_records`
   - `historical_bitcoin_calculations`
   - `daily_summaries`

2. **Data Processing**:
   - Processes periods in batches (recommended: 5-10 periods per batch) to avoid timeouts
   - Maps BMU IDs to farm IDs using the mapping file
   - Calculates volumes and payments for each record
   - Handles API rate limiting with throttled requests

3. **Summary Updates**:
   - Updates daily summary for the target date
   - Recalculates monthly summary for the affected month
   - Updates yearly summary for the affected year

4. **Bitcoin Recalculations**:
   - Recalculates Bitcoin mining potential for each farm and period
   - Updates monthly and yearly Bitcoin summaries

## Troubleshooting

- **API Rate Limiting**: If you encounter API rate limiting issues, adjust the `API_THROTTLE_MS` value (default: 500ms)
- **Database Timeouts**: For very large datasets, reduce the `BATCH_SIZE` to process fewer periods at once
- **Missing Farm Mappings**: Ensure the BMU mapping file is up-to-date with all required wind farms
- **Process Timeout Issues**: Use the staged approach from `staged_reingest_march_21.ts` or `staged_reingest_march_28.ts` for very large datasets

### Using Staged Reingestion

If the complete reingest script times out, use the staged approach:

1. Create a staged reingestion script based on `staged_reingest_march_21.ts` for the target date
2. Set the `START_PERIOD` and `END_PERIOD` variables to process a smaller batch:
   ```typescript
   const START_PERIOD = 1;  // First batch: periods 1-6 
   const END_PERIOD = 6;
   ```

3. Run the script for each batch, incrementing the period numbers each time
4. For high-volume dates, use smaller batch sizes (4-6 periods) to prevent timeouts
5. After completing all batches, run a dedicated summary update script (like `update_march_21_summaries.ts`)

### Recommended Batch Sizes

Based on our experience with March 21 and March 28 reingestions:

| Volume Level | Records | MWh Range | Recommended Batch Size |
|--------------|---------|-----------|------------------------|
| Low          | < 1,000 | < 30,000  | 8-12 periods           |
| Medium       | 1-2,000 | 30-60,000 | 6-8 periods            |
| High         | 2-3,000 | 60-80,000 | 4-6 periods            |
| Very High    | > 3,000 | > 80,000  | 3-4 periods            |

For March 21, we found that processing 4-6 periods at a time was optimal to handle the large volume while avoiding timeouts.

## Examples and Use Cases

### Example 1: Complete Reingestion

For a standard date with manageable data volume, use the complete reingestion approach:

```bash
# Process all 48 settlement periods for April 10, 2025
npx tsx data_reingest_reference.ts 2025-04-10
```

Expected output:
```
Verification Check for 2025-04-10: {
  "records": "2235",
  "periods": "48",
  "volume": "52842.38",
  "payment": "-1486395.76"
}
Update successful at 2025-04-10T12:34:56.789Z
```

### Example 2: Staged Reingestion for March 21, 2025

For dates with medium to high data volume (like March 21, 2025), use the staged approach with smaller batches:

```bash
# Staged reingestion in 8 batches of 6 periods each
npx tsx staged_reingest_march_21.ts  # After setting START_PERIOD=1 and END_PERIOD=6
npx tsx staged_reingest_march_21.ts  # After setting START_PERIOD=7 and END_PERIOD=12
npx tsx staged_reingest_march_21.ts  # After setting START_PERIOD=13 and END_PERIOD=18
# ... continue with remaining batches

# After all batches complete, update summary tables
npx tsx update_march_21_summaries.ts
```

Expected final output:
```
=== Updating Summaries for March 21, 2025 ===
Raw totals from database:
- Energy: 50518.72 MWh
- Payment: -1240439.58
Daily summary updated for 2025-03-21:
- Energy: 50518.72 MWh
- Payment: £-1240439.58
Monthly summary updated for 2025-03:
- Energy: 941012.27 MWh
- Payment: £-23366675.09
Yearly summary updated for 2025:
- Energy: 2655670.60 MWh
- Payment: £-66753759.37
Updating Bitcoin calculations for 2025-03-21...
```

### Example 3: Staged Reingestion for March 28, 2025

For dates with very high data volume (like March 28, 2025), use even smaller batch sizes:

```bash
# First batch - periods 1-4
npx tsx staged_reingest_march_28.ts  # After setting START_PERIOD=1 and END_PERIOD=4

# Second batch - periods 5-8
npx tsx staged_reingest_march_28.ts  # After setting START_PERIOD=5 and END_PERIOD=8

# Continue with remaining batches...
```

Expected final output:
```
Current Status for 2025-03-28:
- Settlement Periods: 48/48
- Records: 4684
- Total Volume: 99904.22 MWh
- Total Payment: £-3784089.62

SUCCESS: All 48 settlement periods are now in the database!
SUCCESS: Final payment total £-3784089.62 matches expected total (within £100 margin)
```

### Example 4: Critical Date Processing

For direct targeting of specific settlement periods:

```bash
# Process only settlement periods 44-48 for March 9, 2025
npx tsx optimized_critical_date_processor.ts 2025-03-09 44 48
```

Expected output:
```
Verification Check for 2025-03-09 (periods 44-48): {
  "processedPeriods": "5",
  "recordsAdded": "126",
  "volume": "2856.43",
  "payment": "-85418.22"
}
Critical period processing complete at 2025-03-09T15:22:33.456Z
```