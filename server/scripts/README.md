# Server Scripts

This directory contains utility and maintenance scripts for the Bitcoin Mining Potential application.

## Directory Structure

The scripts are organized into the following categories:

### Maintenance Scripts

The `maintenance` directory contains scripts that need to be run periodically to keep the system up-to-date:

- **updateBmuMapping.ts**: Updates the BMU (Balancing Mechanism Unit) mapping from the Elexon API. This script fetches the latest wind farm data and updates the `bmuMapping.json` file used by the application.

### Data Processing Scripts

The `data` directory contains scripts for processing and managing application data:

- **ingestMonthlyData.ts**: Processes monthly data ingestion for settlement periods, fetching curtailment data from Elexon.
- **updateHistoricalCalculations.ts**: Updates historical Bitcoin calculations with proper batching and validation.
- **processDifficultyMismatch.ts**: Detects and corrects difficulty mismatches in historical Bitcoin calculation records.

## Usage

These scripts can be executed directly using the TypeScript execution engine:

```bash
# To run a maintenance script
npx tsx server/scripts/maintenance/updateBmuMapping.ts

# To run a data processing script
npx tsx server/scripts/data/ingestMonthlyData.ts
```

## Notes

- Some historical scripts have been moved to the `backup` directory. See `backup/README.md` for details.
- Most daily and periodic reconciliation is now handled by the centralized reconciliation system in `unified_reconciliation.ts` and `daily_reconciliation_check.ts`.
- For one-time operations, it's recommended to use the CLI interface provided by the `unified_reconciliation.ts` script rather than creating new scripts.