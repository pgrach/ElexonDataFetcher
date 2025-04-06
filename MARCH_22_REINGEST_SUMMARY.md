# March 22, 2025 Data Reingestion Summary

## Current Status

- **Date**: March 22, 2025
- **Current Data**: 
  - 898 curtailment records across 46 settlement periods
  - Missing periods 47-48 (23:00 hour)
  - Total Energy: 25,525.77 MWh
  - Total Payment: £63,809.23
  - Bitcoin Mined: 19.54671363 BTC

## Reingestion Approach

The reingestion process uses a staged approach where settlement periods are processed in smaller batches to prevent timeout issues with the Elexon API. This approach follows the same pattern used successfully for the March 21 reingestion.

### Scripts Created

1. **clear_march_22_data_simplified.ts**
   - Completely removes all existing data for March 22, 2025
   - Deletes from curtailment_records, historical_bitcoin_calculations, and daily_summaries tables

2. **staged_reingest_march_22_simplified.ts**
   - Processes a batch of settlement periods
   - Configured to prioritize missing periods 47-48
   - Fetches data from the Elexon API
   - Filters valid wind farm curtailment records
   - Inserts them into the database
   - Provides a summary of processed data

3. **update_march_22_summaries_simplified.ts**
   - Updates daily, monthly, and yearly summary tables
   - Recalculates Bitcoin mining potential
   - Should be run after all settlement periods are processed

## Running the Reingestion

The reingestion process should be executed in the following order:

1. First, clear all existing data:
   ```
   npx tsx clear_march_22_data_simplified.ts
   ```

2. Process the missing settlement periods (47-48):
   ```
   npx tsx staged_reingest_march_22_simplified.ts
   ```

3. Run the remainder of the settlement periods if needed (can edit START_PERIOD and END_PERIOD in the script):
   ```
   # Edit the script to set START_PERIOD=1 and END_PERIOD=46
   npx tsx staged_reingest_march_22_simplified.ts
   ```

4. Update summary tables and Bitcoin calculations:
   ```
   npx tsx update_march_22_summaries_simplified.ts
   ```

## Verification

After running the reingestion, the data should show:
- All 48 settlement periods present
- Data for the 23:00 hour correctly showing in the hourly breakdown chart
- The correct total energy and payment values

## Expected Results

Based on the March 21 pattern, we expect:
- Complete data for all 48 settlement periods
- No missing hourly data in the visualization
- Bitcoin calculations properly reflecting the full dataset