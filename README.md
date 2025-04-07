# Bitcoin Mining Reingestion & Reconciliation System

This system provides a comprehensive solution for reingesting curtailment data from the Elexon API and calculating Bitcoin mining potential across all settlement periods for multiple miner models.

## Overview

The Bitcoin Mining Analytics platform processes curtailment data from wind farms and calculates the potential Bitcoin that could have been mined if that curtailed energy had been used for Bitcoin mining. This system handles the reingestion of data for specific dates, ensuring all 48 settlement periods are properly processed and Bitcoin calculations are performed for multiple miner models.

## Features

- **Robust Data Processing**: Handles large volumes of data with batch processing to avoid timeouts
- **Error Resilience**: Includes comprehensive error handling and recovery mechanisms
- **Database Integrity**: Prevents duplicate records using ON CONFLICT clauses
- **Complete Coverage**: Ensures all 48 settlement periods are processed for each date
- **Multiple Miner Models**: Supports calculations for different Bitcoin mining hardware (S19J_PRO, S9, M20S)
- **Comprehensive Verification**: Includes built-in verification to ensure data completeness and integrity

## Usage

The system provides a unified script that handles the entire reingestion process:

```bash
npx tsx complete_reingestion_process.ts [date]
```

Example:
```bash
npx tsx complete_reingestion_process.ts 2025-03-04
```

## Process Flow

1. **Initial Check**: Verifies if data already exists for the specified date
2. **Curtailment Data Reingestion**: Processes data from the Elexon API in batches
3. **Data Cleanup**: Removes any existing Bitcoin calculations to prevent duplicates
4. **Bitcoin Calculation**: Calculates mining potential for each miner model across all periods
5. **Summary Updates**: Updates monthly and yearly Bitcoin summaries automatically
6. **Verification**: Confirms all calculations are complete and valid

## Automatic Update Chain

The system implements a robust data consistency mechanism to ensure changes propagate through all summary levels:

1. **Hourly Data Updates**: New settlement period data triggers the update process
2. **Daily Calculations**: When hourly data changes, daily Bitcoin calculations are refreshed
3. **Monthly Aggregation**: Daily updates automatically trigger monthly summary recalculation 
4. **Yearly Aggregation**: Monthly summary updates automatically trigger yearly summary recalculation

This automatic update chain ensures that all summary levels (daily, monthly, and yearly) remain consistent even when new hourly data is added or existing data is modified.

## Alternative Scripts

For targeted operations, the following scripts are available:

- `batch_process_periods.ts`: Process specific ranges of settlement periods
- `complete_reingestion_process.ts`: Full reingestion of data for an entire date
- `reingest_single_batch.ts`: Focus on smaller batches of periods
- `optimized_critical_date_processor.ts`: Handle problematic dates with improved handling of multiple records per farm

### Critical Date Processing

For dates with problematic data, especially those with multiple records per farm within the same period, use the optimized processor:

```bash
# Process all periods for a specific date
npx tsx optimized_critical_date_processor.ts 2025-03-09

# Process specific periods for a date
npx tsx optimized_critical_date_processor.ts 2025-03-09 44 48
```

This script includes improved handling of duplicate farm records and bulk processing functionality.

## Technical Details

### Database Schema

The system relies on several interconnected tables that form the data hierarchy:

**Primary Tables:**
- `curtailment_records`: Stores raw curtailment data from Elexon API
- `historical_bitcoin_calculations`: Stores Bitcoin mining potential calculations for each period and farm

**Summary Tables:**
- `daily_summaries`: Aggregates curtailment data by day
- `monthly_summaries`: Aggregates curtailment data by month
- `yearly_summaries`: Aggregates curtailment data by year
- `bitcoin_monthly_summaries`: Aggregates Bitcoin calculations by month for each miner model
- `bitcoin_yearly_summaries`: Aggregates Bitcoin calculations by year for each miner model

The automatic update chain ensures that changes to the primary tables propagate through all summary tables.

### Calculation Formula

The Bitcoin calculation uses the following formula:

1. Convert curtailed MWh to kWh
2. Determine how many miners could operate with that energy
3. Calculate the potential hash power those miners would produce
4. Calculate the Bitcoin that could be mined based on network difficulty

## Performance Results

For a typical day with 48 settlement periods:

- **Curtailment Records**: ~4,300 records totaling ~95,000 MWh
- **Bitcoin Calculations**: ~2,200 records per miner model
- **Processing Time**: ~2-5 minutes for complete reingestion and calculation

## Example Outputs

**Daily Bitcoin Yield (2025-03-06):**
- S19J_PRO: 34.58136172 BTC
- M20S: 21.28012335 BTC
- S9: 10.71975614 BTC

**Monthly Bitcoin Yield (March 2025):**
- S19J_PRO: 260.28916727 BTC
- M20S: 159.87284065 BTC
- S9: 80.60844328 BTC

**Yearly Bitcoin Yield (2025):**
- S19J_PRO: 903.24720919 BTC
- M20S: 556.74644712 BTC
- S9: 280.71290970 BTC

## Data Verification and Repair Tools

The system includes comprehensive tools for data verification and repair:

### Comprehensive Data Verification and Repair Utility

The `verify_and_fix_data.ts` utility provides a powerful all-in-one solution for verifying and repairing data integrity issues:

```bash
# Basic usage
npx tsx verify_and_fix_data.ts [date] [action] [sampling-method]

# Examples
npx tsx verify_and_fix_data.ts                        # Verifies today's data using progressive sampling
npx tsx verify_and_fix_data.ts 2025-04-01             # Verifies specific date using progressive sampling
npx tsx verify_and_fix_data.ts 2025-04-01 verify      # Only verifies without fixing
npx tsx verify_and_fix_data.ts 2025-04-01 fix         # Verifies and fixes if needed
npx tsx verify_and_fix_data.ts 2025-04-01 fix random  # Uses random sampling for verification
npx tsx verify_and_fix_data.ts 2025-04-01 force-fix   # Forces reprocessing without verification
```

#### Available Actions

- **verify** (default): Only performs verification without fixing
- **fix**: Verifies and automatically fixes if issues are found
- **force-fix**: Skips verification and forces a complete reprocessing of the date

#### Intelligent Sampling Strategies

The verification utility offers several sampling methods to balance thoroughness with API efficiency:

1. **Progressive Sampling** (default): Starts with 5 key periods (1, 12, 24, 36, 48), then adds up to 10 more random periods if issues are found. Efficiently identifies problems while minimizing API calls.

2. **Random Sampling**: Checks 10 randomly selected periods for better coverage without bias toward specific times.

3. **Fixed Key Periods**: Only checks strategic periods (1, 12, 24, 36, 48) representing morning, midday, afternoon, evening, and night. Fast but may miss issues in other periods.

4. **Full Verification**: Attempts to check all 48 periods, but may hit API rate limits. Use sparingly.

#### Comprehensive Repair Process

When issues are found, the repair process follows these steps:

1. **Curtailment Reprocessing**: Clears existing curtailment records and fetches fresh data from the Elexon API for all 48 periods
2. **Bitcoin Calculation**: Recalculates Bitcoin mining potential for all three miner models (S19J_PRO, S9, M20S) based on the updated curtailment data
3. **Summary Updates**: Updates all daily, monthly, and yearly summary tables in a full cascade
4. **Verification**: Confirms the repairs were successful by comparing initial and final data states

#### Features

This utility:
1. Combines verification and repair capabilities in a single tool
2. Uses intelligent sampling strategies to effectively find data inconsistencies
3. Provides automatic repair for identified issues
4. Generates detailed logs with before/after comparisons
5. Supports both targeted fixes and forced reprocessing for known issues

#### Logs and Reporting

Each verification and repair operation generates a detailed log file in the `logs` directory:

```
logs/verify_and_fix_YYYY-MM-DD_HHMMSS.log
```

These logs contain:
- Initial database state
- Verification results for each checked period
- Repair actions taken (if any)
- Final database state after repair
- Detailed statistics about changes made

### Legacy Elexon Data Verification

The `check_elexon_data.ts` script provides a lightweight verification that the database records match the data from the Elexon API:

```bash
# Basic usage
npx tsx check_elexon_data.ts [date] [sampling-method]

# Examples
npx tsx check_elexon_data.ts                     # Checks default date with progressive sampling
npx tsx check_elexon_data.ts 2025-03-28          # Checks specific date with progressive sampling
npx tsx check_elexon_data.ts 2025-03-28 random   # Uses random sampling of periods
npx tsx check_elexon_data.ts 2025-03-28 fixed    # Uses fixed key periods only
npx tsx check_elexon_data.ts 2025-03-28 full     # Attempts to check all 48 periods (may hit API limits)
```

If discrepancies are found, the script provides the exact commands needed to reprocess the data:
```
=== Reingestion Required ===
Run the following commands to update the data:
1. npx tsx server/services/curtailment.ts process-date 2025-03-22
2. For each model (S19J_PRO, S9, M20S):
   npx tsx server/services/bitcoinService.ts process-date 2025-03-22 MODEL_NAME
3. npx tsx server/services/bitcoinService.ts recalculate-monthly 2025-03
4. npx tsx server/services/bitcoinService.ts recalculate-yearly 2025
```

## Troubleshooting

If the process fails, check these common issues:

1. **Database Connection**: Ensure DATABASE_URL environment variable is properly set
2. **API Timeouts**: For large datasets, increase the batch size or add more pauses
3. **Duplicate Records**: Verify the ON CONFLICT clauses are working correctly
4. **Missing Periods**: Look for API errors in the logs that might indicate missing data
5. **Data Consistency**: Use verification tools like `check_elexon_data.ts` to identify inconsistencies

## File Organization and Project Structure

The project includes several scripts for data processing and verification. Here's an overview of the key files and their purposes:

### Core Data Processing Scripts

- **process_all_periods.ts**: Main script for processing all 48 settlement periods for a specific date
- **process_bitcoin_optimized.ts**: Optimized script for calculating Bitcoin mining potential with single DynamoDB fetch
- **process_complete_cascade.ts**: Processes the full data cascade (curtailment → Bitcoin → summaries)

### Data Verification and Repair

- **verify_and_fix_data.ts**: Comprehensive utility to verify and fix data integrity issues (recommended)
- **check_elexon_data.ts**: Lightweight utility to check data against Elexon API

### Fixed-Purpose Scripts

- **fix_incomplete_data_optimized.ts**: Optimized script for fixing incomplete data with all miner models at once
- **update_summaries.ts**: Updates summary tables for specific dates

### Specialized Tools

- **run_wind_data_migration.ts**: Ensures wind_generation_data table is properly set up
- **update_wind_generation_data.ts**: Updates wind generation data from Elexon API

### Optional/Legacy Scripts

These scripts are kept for backward compatibility or specific use cases but are not needed for regular operation:

- **fix_incomplete_data.ts**: Legacy version of fix_incomplete_data_optimized.ts
- **process_bitcoin.ts**: Legacy non-optimized Bitcoin processor
- **process_curtailment.ts**: Legacy script for processing curtailment data only
- **process_monthly.ts**: Legacy script for updating monthly summaries only
- **process_yearly.ts**: Legacy script for updating yearly summaries only
- **update_summaries_for_march_25.ts**: One-time script for a specific date (can be removed)
- **verify_dates.ts**: Specialized verification script for specific dates
- **verify_service.ts**: Legacy verification service

## Maintenance Recommendations

- Run the process during off-peak hours to minimize database load
- Monitor memory usage for large datasets
- Consider increasing batch sizes for faster processing if resources allow
- Periodically verify historical data integrity using the verification functions
- Run the `verify_and_fix_data.ts` script weekly on random dates to ensure ongoing data consistency

### Recommended Cleanup

The following files can be safely removed or archived as they have been replaced by more optimized versions:

1. **fix_incomplete_data.ts** - Replaced by fix_incomplete_data_optimized.ts
2. **process_bitcoin.ts** - Replaced by process_bitcoin_optimized.ts  
3. **update_summaries_for_march_25.ts** - One-time use script for a specific date
4. **backup/miningPotentialRoutes.ts** - Deprecated routes
5. **backup/miningPotentialService.ts** - Deprecated service