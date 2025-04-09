# Elexon Data Reingestion Process

This document describes the process for reingesting Elexon API data for specific dates when discrepancies are found between the API data and database records.

## Overview

The reingestion process involves:

1. Validating data discrepancies using the validation scripts
2. Deleting existing data for the target date
3. Fetching fresh data from the Elexon API
4. Inserting the new data into the database
5. Updating dependent tables (daily summaries, Bitcoin calculations, etc.)

## Available Scripts

### 1. Validation Scripts

These scripts check for data discrepancies between the Elexon API and the database:

- `validate_elexon_data_batch1.ts` - Validates periods 1-16
- `validate_elexon_data_batch2.ts` - Validates periods 17-32
- `validate_elexon_data_batch3.ts` - Validates periods 33-48
- `validate_elexon_combine_results.ts` - Combines results from all batches

Run the batch validation scripts first, then combine the results:

```bash
npx tsx validate_elexon_data_batch1.ts
npx tsx validate_elexon_data_batch2.ts
npx tsx validate_elexon_data_batch3.ts
npx tsx validate_elexon_combine_results.ts
```

### 2. Reingestion Scripts

Two options are available for reingestion:

#### Option A: Full Reingestion (All 48 Periods)

Processes all 48 settlement periods (1-48) for the target date:

```bash
./reingest_elexon_data.sh 2025-04-01
```

This is thorough but can take a long time due to API rate limits.

#### Option B: Key Periods Reingestion (Faster)

Only processes settlement periods known to have curtailment data for the target date:

```bash
./reingest_elexon_key_periods.sh 2025-04-01
```

This is much faster and covers the essential data.

### 3. Bitcoin Calculation Update

After reingestion, updates the Bitcoin calculations for the target date:

```bash
./update_bitcoin_daily_summaries_for_date.sh 2025-04-01
```

### 4. Combined Process (Recommended)

Executes the key periods reingestion and Bitcoin update in a single command:

```bash
./reingest_and_update_bitcoin.sh 2025-04-01
```

## Logging

All scripts generate detailed logs in the `logs` directory:

- Validation logs: `logs/validation_*.log`
- Reingestion logs: `logs/reingest_*.log`
- Bitcoin update logs: `logs/update_bitcoin_*.log`
- Combined logs: `logs/combined_reingestion_*.log`

## Troubleshooting

If reingestion fails:

1. Check the log files for error messages
2. Verify that the date format is correct (YYYY-MM-DD)
3. Ensure API connectivity (the scripts rely on the Elexon API)
4. Check for database constraints that might be preventing updates

## Notes

- The reingestion process will delete and replace **all** data for the target date
- Monthly and yearly summaries should be updated separately if needed
- Wind generation data for daily summaries will need to be updated using the `update_daily_summary_wind_data.ts` script