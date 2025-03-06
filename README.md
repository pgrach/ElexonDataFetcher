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
5. **Verification**: Confirms all calculations are complete and valid

## Alternative Scripts

For targeted operations, the following scripts are available:

- `batch_process_periods.ts`: Process specific ranges of settlement periods
- `process_bitcoin_calculations.ts`: Calculate Bitcoin for all periods with specific models
- `process_missing_bitcoin.ts`: Find and process only missing Bitcoin calculations
- `direct_bitcoin_calc.ts`: Direct Bitcoin calculation without DynamoDB dependency

## Technical Details

### Database Schema

The system relies on two primary tables:

- `curtailment_records`: Stores raw curtailment data from Elexon API
- `historical_bitcoin_calculations`: Stores Bitcoin mining potential calculations

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

Example of Bitcoin yield for 2025-03-04:
- S19J_PRO: 39.21754996 BTC
- M20S: 23.41375966 BTC
- S9: 11.80551586 BTC

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