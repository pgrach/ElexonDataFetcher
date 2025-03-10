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

## Troubleshooting

If the process fails, check these common issues:

1. **Database Connection**: Ensure DATABASE_URL environment variable is properly set
2. **API Timeouts**: For large datasets, increase the batch size or add more pauses
3. **Duplicate Records**: Verify the ON CONFLICT clauses are working correctly
4. **Missing Periods**: Look for API errors in the logs that might indicate missing data

## Maintenance Recommendations

- Run the process during off-peak hours to minimize database load
- Monitor memory usage for large datasets
- Consider increasing batch sizes for faster processing if resources allow
- Periodically verify historical data integrity using the verification functions