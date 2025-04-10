# Data Reprocessing Scripts

This directory contains scripts for reprocessing curtailment data from the Elexon API and recalculating Bitcoin mining potential.

## Available Scripts

### `reprocess_date.ts`

The main script for reprocessing all data for a specific date. It provides a comprehensive solution that:

1. Clears existing curtailment records and Bitcoin calculations for the target date
2. Fetches and processes new curtailment data from the Elexon API
3. Calculates daily, monthly, and yearly summaries
4. Processes Bitcoin mining calculations for all miner models
5. Updates Bitcoin summary tables at daily, monthly, and yearly levels
6. Provides a verification summary of processed data

### `run_reprocess.sh`

A convenient shell script for running the reprocessing script with parameters:

```bash
# Process a specific date with default parameters (all 48 settlement periods)
./scripts/run_reprocess.sh 2025-04-10

# Process a specific date but limit to 5 settlement periods (for testing)
./scripts/run_reprocess.sh 2025-04-10 5

# Process today's date (defaults to current date if not specified)
./scripts/run_reprocess.sh
```

## Parameters

The reprocessing scripts accept the following parameters:

- **DATE**: The date to reprocess in `YYYY-MM-DD` format (defaults to today's date)
- **MAX_PERIODS**: Maximum number of settlement periods to process (defaults to 48)

You can set these parameters either as environment variables or through the `run_reprocess.sh` script arguments.

## Implementation Details

The reprocessing script implements a comprehensive workflow:

1. **Data Preparation**:
   - Loads BMU mapping for wind farm identification
   - Clears existing curtailment and Bitcoin records for the target date

2. **Data Processing**:
   - Processes each settlement period from the Elexon API
   - Filters for valid curtailment records (negative volume, SO/CADL flags)
   - Inserts filtered records into the database

3. **Summary Calculations**:
   - Calculates daily summaries based on processed curtailment records
   - Updates monthly summaries based on daily records
   - Updates yearly summaries based on monthly records

4. **Bitcoin Calculations**:
   - Processes Bitcoin mining potential for each miner model
   - Updates Bitcoin daily, monthly, and yearly summaries

5. **Verification**:
   - Provides statistics on processed records
   - Displays total volume and payment amounts

## Example Output

```
==== Complete Data Reprocessing for 2025-04-10 ====
Using difficulty: 113757508810853

==== Clearing existing curtailment records for 2025-04-10 ====

Found 460 existing curtailment records for 2025-04-10
Cleared curtailment records for 2025-04-10
Cleared daily summaries for 2025-04-10

==== Successfully cleared existing curtailment records ====

... (processing output) ...

==== Reprocessing complete for 2025-04-10 ====
Total execution time: 4.5 seconds

==== Verification Summary ====
Records: 460
Settlement Periods: 17
Total Volume: 7312.10 MWh
Total Payment: £-163793.04
```