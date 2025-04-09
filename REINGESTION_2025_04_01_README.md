# Data Reingestion Utility for 2025-04-01

This utility provides scripts to reingest and update data for April 1, 2025 from the Elexon API. It handles the entire data pipeline including:

1. Reingesting curtailment data from Elexon API
2. Updating the daily summary for 2025-04-01
3. Updating the monthly summary for April 2025
4. Updating the yearly summary for 2025
5. Updating Bitcoin calculations for all supported miner models

## Quick Start

Run the interactive shell script and follow the prompts:

```bash
./reingest_2025_04_01.sh
```

## Available Options

The script provides four main operations:

1. **Full Reingestion** - Updates all tables (recommended)
   - Updates curtailment records, summary tables, and Bitcoin calculations
   - Good for: Complete data refresh for the specified date

2. **Curtailment Records Only** - Updates only the raw curtailment data
   - Good for: When you only need to fix issues with the raw data

3. **Summary Tables Only** - Updates only the summary tables
   - Good for: After manually fixing curtailment records
   - Updates daily, monthly, and yearly summaries

4. **Bitcoin Calculations Only** - Updates only Bitcoin-related tables
   - Good for: After manually fixing curtailment records
   - Updates historical calculations and Bitcoin summary tables

## Manual Usage

You can also run individual parts of the process directly:

```bash
# Full reingestion
npx tsx server/scripts/update_2025_04_01_complete.ts

# Only reingest curtailment records
npx tsx -e "import { reingestCurtailmentRecords } from './server/scripts/update_2025_04_01_complete'; reingestCurtailmentRecords();"

# Only update summary tables
npx tsx -e "import { updateSummaryTables } from './server/scripts/update_2025_04_01_complete'; updateSummaryTables();"

# Only update Bitcoin calculations
npx tsx -e "import { updateBitcoinCalculations } from './server/scripts/update_2025_04_01_complete'; updateBitcoinCalculations();"
```

## Data Pipeline Flow

The reingestion process follows this sequence:

1. **Data Acquisition**: Retrieve curtailment data from Elexon API for each settlement period
2. **Data Filtering**: Filter for valid wind farm curtailment records
3. **Primary Storage**: Insert filtered records into `curtailment_records` table
4. **Summary Calculation**: Update `daily_summaries`, `monthly_summaries`, and `yearly_summaries`
5. **Bitcoin Calculation**: Process Bitcoin mining potential for each curtailment record
6. **Bitcoin Summaries**: Update Bitcoin summary tables at daily, monthly, and yearly levels

## Troubleshooting

If you encounter issues during the reingestion process:

1. **API Rate Limiting**: The script includes rate limiting, but if you hit Elexon API limits, wait a few minutes and try again
2. **Database Locks**: If database is locked, ensure no other processes are writing to the same tables
3. **Missing BMU Mapping**: Verify that `data/bmu_mapping.json` exists and contains valid wind farm BMU IDs
4. **Partial Updates**: If the process fails partway through, you can run specific parts (options 2-4) to complete the update

For detailed logs of the process, check the console output during execution.