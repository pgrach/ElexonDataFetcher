# Bitcoin Mining Analytics Data Reconciliation

This document outlines the data reconciliation process for the Bitcoin Mining Analytics platform, including tools and procedures to maintain data integrity between curtailment records and Bitcoin calculations.

## Data Flow Architecture

The platform's data follows this flow:

1. **Curtailment Records** - Raw data ingested from external APIs about energy curtailment
2. **Historical Bitcoin Calculations** - Calculations of potential Bitcoin mining based on curtailment
3. **Monthly/Yearly Summaries** - Aggregated statistics for reporting

## Available Reconciliation Tools

Several specialized tools are available to maintain data integrity:

### 1. Daily Reconciliation Check

Automatically checks the reconciliation status for recent dates and processes any missing calculations:

```bash
npx tsx daily_reconciliation_check.ts [days=2] [forceProcess=false]
```

Options:
- `days` - Number of recent days to check (default: 2)
- `forceProcess` - Force processing even if no issues found (default: false)

### 2. Unified Reconciliation System

The primary tool for data integrity management with multiple functions:

```bash
npx tsx unified_reconciliation.ts [command] [options]
```

Commands:
- `status` - Show current reconciliation status
- `analyze` - Analyze missing calculations and detect issues
- `reconcile [batchSize]` - Process all missing calculations with specified batch size
- `date YYYY-MM-DD` - Process a specific date
- `range YYYY-MM-DD YYYY-MM-DD [batchSize]` - Process a date range
- `critical DATE` - Process a problematic date with extra safeguards
- `spot-fix DATE PERIOD FARM` - Fix a specific date-period-farm combination

### 3. Specialized Data Processing Scripts

For specific data management needs, use the organized scripts in the `server/scripts` directory:

#### Data Processing Scripts

Located in `server/scripts/data/`:

- **ingestMonthlyData.ts** - Processes monthly data ingestion from Elexon
- **processDifficultyMismatch.ts** - Fixes difficulty inconsistencies
- **updateHistoricalCalculations.ts** - Updates Bitcoin calculations with batching

#### Maintenance Scripts

Located in `server/scripts/maintenance/`:

- **updateBmuMapping.ts** - Updates BMU mapping data from Elexon API

See the README files in each directory for detailed usage instructions.

## Automated Monitoring and Alerts

The system includes automated monitoring to catch missing data before it causes issues:

1. **Automated Daily Check**: The `daily_reconciliation_check.ts` script runs automatically to find and fix issues.

2. **Reconciliation Reports**: The `unified_reconciliation.ts analyze` command generates detailed reports on data completeness.

## Recovery Procedures

If missing data is detected:

1. **Identify the Gap**: Use the unified reconciliation system to identify missing data:
   ```bash
   npx tsx unified_reconciliation.ts status
   ```

2. **Reprocess the Data**: Use the appropriate command for the situation:
   ```bash
   # For a specific date
   npx tsx unified_reconciliation.ts date YYYY-MM-DD
   
   # For a date range
   npx tsx unified_reconciliation.ts range YYYY-MM-DD YYYY-MM-DD
   
   # For problematic dates
   npx tsx unified_reconciliation.ts critical YYYY-MM-DD
   ```

3. **Verify Recovery**: Run verification again to confirm all data is properly reconciled:
   ```bash
   npx tsx unified_reconciliation.ts status
   ```

## Preventative Measures

To prevent missing data:

1. Set up scheduled runs of `daily_reconciliation_check.ts` to detect and fix issues early.

2. Implement robust error handling in ingestion processes.

3. Run periodic comprehensive reconciliation to ensure historical data integrity.

4. Document any manual data corrections in system logs for audit purposes.

## Troubleshooting Common Issues

Common issues and their solutions:

1. **Missing Curtailment Data**: Often caused by API access issues or network interruptions.
   - Solution: Use `unified_reconciliation.ts date YYYY-MM-DD` to re-ingest data from source APIs.

2. **Missing Bitcoin Calculations**: May occur when curtailment data exists but calculation failed.
   - Solution: Fix with `unified_reconciliation.ts reconcile` which only regenerates the calculations.

3. **Data Consistency Issues**: When totals don't match between tables.
   - Solution: Use `unified_reconciliation.ts critical YYYY-MM-DD` to apply consistency fixes.

4. **BMU Mapping Issues**: When new wind farms are added but not reflected in the system.
   - Solution: Update BMU mapping with `npx tsx server/scripts/maintenance/updateBmuMapping.ts`

## Recent Recoveries

### March 2025 Data Recovery

Successfully recovered missing data for March 1-2, 2025:

- March 1, 2025: Recovered 819 curtailment records (21,178.62 MWh)
- March 2, 2025: Recovered 2,444 curtailment records (61,575.86 MWh)
- Bitcoin calculations fully reconciled for all dates

The issue was identified and resolved using the unified reconciliation system documented in this guide.