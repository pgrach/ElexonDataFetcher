# Data Reprocessing Scripts

This directory contains scripts for reprocessing curtailment data from the Elexon API. These scripts allow you to refresh data for specific dates, ensuring accurate and up-to-date information throughout the system.

## BMU Mapping Fix

Before running the reprocessing scripts, you should run the BMU mapping fix script to ensure farm data is correctly processed:

```bash
# Run from the project root directory
./fix-bmu-mapping.sh
```

This script synchronizes two different BMU mapping files that are used by different parts of the system:
- `server/data/bmuMapping.json` (used by elexon.ts)
- `data/bmu_mapping.json` (used by curtailment_enhanced.ts)

Without this fix, you might see hourly breakdowns working correctly but "No farm data available" in the individual farm details view.

## Available Scripts

### 1. Reprocess April 3, 2025 Data

```bash
# Run from the project root directory
./reprocess-april3.sh
```

This script will:
- Delete existing curtailment records for April 3, 2025
- Delete associated Bitcoin calculations
- Fetch fresh data from Elexon API for all 48 settlement periods
- Update daily, monthly, and yearly summaries
- Recalculate Bitcoin mining potential for all miner models

### 2. Reprocess Any Date

```bash
# Run from the project root directory
./reprocess-any-date.sh YYYY-MM-DD
```

Example:
```bash
./reprocess-any-date.sh 2025-04-10
```

This script accepts a date parameter in YYYY-MM-DD format and performs the same reprocessing steps for that specific date.

### 3. Direct TypeScript Execution

You can also run the TypeScript scripts directly:

```bash
# For April 3, 2025
npx tsx reprocessApril3.ts

# For any date
npx tsx scripts/reprocessAnyDate.ts YYYY-MM-DD
```

## Important Notes

1. **API Rate Limits**: These scripts respect Elexon API rate limits by adding delays between requests.

2. **Database Dependencies**: The scripts handle proper deletion order (first Bitcoin calculations, then curtailment records) to maintain database integrity.

3. **Cascading Updates**: When daily summaries are updated, changes cascade to monthly and yearly summaries automatically.

4. **Complete Refresh**: The scripts perform a complete refresh of the data for the specified date, including all 48 settlement periods.

5. **Execution Time**: Depending on the number of records, reprocessing a single day could take 1-5 minutes to complete.

## Troubleshooting

If you encounter issues:

1. Check the logs for specific error messages.
2. Verify that the Elexon API is accessible.
3. Ensure the database is properly set up and accessible.