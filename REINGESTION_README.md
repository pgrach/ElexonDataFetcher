# Data Reingestion Utility for 2025-03-24

This utility provides scripts to reingest and update data for March 24, 2025 from the Elexon API. It specifically focuses on the `curtailment_records` table and related summary tables.

## Quick Start

Run the interactive shell script and follow the prompts:

```bash
./reingest_2025_03_24.sh
```

## Available Options

The script provides four main operations:

1. **Basic Reingestion** - Updates only the `curtailment_records` table using TypeScript
   - Uses: `server/scripts/run_reingest_2025_03_24.ts`
   - Good for: TypeScript developers who only need to update raw data

2. **Complete Reingestion** - Updates all tables (curtailment records + summaries + Bitcoin calculations)
   - Uses: `server/scripts/update_2025_03_24_complete.ts`
   - Good for: Full system update with all dependencies

3. **Simple JS Version** - Most reliable option for updating curtailment records
   - Uses: `server/scripts/reingest_2025_03_24_simple.js`
   - Good for: Production use when TypeScript compilation issues might occur

4. **Update Summary Tables Only** - Updates summaries after reingestion
   - Uses: `server/scripts/update_summaries_2025_03_24.js`
   - Good for: Use after options 1 or 3 to update summary tables

## Manual Usage

You can also run individual scripts directly:

```bash
# Option 1: Basic TypeScript reingestion
npx tsx server/scripts/run_reingest_2025_03_24.ts

# Option 2: Complete reingestion with all tables
npx tsx server/scripts/update_2025_03_24_complete.ts

# Option 3: Simple JS version (most reliable)
node server/scripts/reingest_2025_03_24_simple.js

# Option 4: Update summaries only
node server/scripts/update_summaries_2025_03_24.js
```

## How It Works

1. **Data Fetching**: Each script connects to the Elexon API and fetches curtailment data for March 24, 2025.

2. **Data Processing**: 
   - Filters data to include only wind farms (using the BMU mapping file)
   - Processes valid curtailment records (negative volume with SO or CADL flags)
   - Calculates payment values based on original prices

3. **Database Updates**:
   - Clears existing records for the target date
   - Inserts new records from the API
   - Updates summary tables with aggregated values

## Troubleshooting

If you encounter issues:

1. **TypeScript Errors**: Use option 3 (simple JS version) which avoids TypeScript compilation issues
2. **API Rate Limiting**: The scripts include rate limiting controls but may still hit limits if run repeatedly
3. **Database Errors**: Check PostgreSQL connection and ensure the schema is correctly set up
4. **Summary Table Errors**: If summary tables aren't updating correctly, use option 4 to update them separately

## For Developers

The reingestion system follows the same pattern as the main data pipeline:

1. `elexon.ts` - Handles API communication
2. `reingest_2025_03_24.ts` - Processes and stores data
3. `update_summaries_2025_03_24.js` - Updates summary tables

The TypeScript and JavaScript versions are functionally equivalent, but the JavaScript version bypasses TypeScript compilation issues that might occur in some environments.