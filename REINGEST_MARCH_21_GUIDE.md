# March 21, 2025 Data Reingest Guide

This guide explains how to use the `reingest_march_21_2025.ts` script to reingest all settlement period data for March 21, 2025, ensuring complete and accurate data in the database.

## Purpose

The purpose of this script is to:

1. Clear all existing data for March 21, 2025 across all relevant tables
2. Reingest all 48 settlement periods from the Elexon API
3. Update all summary tables (daily, monthly, yearly)
4. Recalculate all Bitcoin mining potentials

## Prerequisites

Before running the script, ensure:

1. You have a working internet connection to access the Elexon API
2. The Elexon API key is set in the environment variable `ELEXON_API_KEY`
3. Your database connection is properly configured in the environment variable `DATABASE_URL`

## Running the Script

Execute the script using the following command:

```bash
npx tsx reingest_march_21_2025.ts
```

## Process Flow

The script follows these steps:

1. **Loading BMU Mappings**: Loads the mapping between Elexon BMU IDs and internal farm IDs from the mapping file
2. **Clearing Existing Data**: Removes all existing data for March 21, 2025 from:
   - `curtailment_records` table
   - `historical_bitcoin_calculations` table
   - `daily_summaries` table
3. **Processing Settlement Periods**: Fetches and processes all 48 settlement periods in batches of 6 periods each
4. **Updating Summaries**: Recalculates and updates the daily, monthly, and yearly summaries
5. **Updating Bitcoin Calculations**: Recalculates Bitcoin mining potential for each miner model
6. **Verification**: Verifies that all 48 settlement periods were successfully processed

## Monitoring Progress

The script creates a log file named `reingest_2025-03-21.log` in the project root directory. You can monitor this file for progress updates and any errors.

The script also outputs detailed information to the console, including:
- Number of records processed
- Total volume and payment
- Any errors encountered
- Verification results

## Expected Outcome

After successful execution:

1. The `curtailment_records` table will contain all curtailment records for March 21, 2025
2. The `historical_bitcoin_calculations` table will contain Bitcoin calculations for all periods
3. The `daily_summaries`, `monthly_summaries`, and `yearly_summaries` tables will be updated with correct totals
4. The log will show "SUCCESS: All 48 settlement periods are now in the database!"

## Troubleshooting

If the script encounters issues:

1. **API Errors**: Check that the Elexon API key is valid and set correctly
2. **Database Errors**: Verify the database connection string and database permissions
3. **Missing Periods**: If the verification shows missing periods, check the log for specific errors during those periods

For additional help, refer to the main data reingest documentation in `DATA_REINGEST_GUIDE.md`.