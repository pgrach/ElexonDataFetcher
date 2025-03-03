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

### 2. Verify Date Integrity

A comprehensive tool to verify data integrity for a specified date range:

```bash
npx tsx verify_date_integrity.ts [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--auto-fix]
```

Options:
- `--start` - Start date (default: 7 days ago)
- `--end` - End date (default: today) 
- `--auto-fix` - Automatically fix missing data (default: false)

### 3. Comprehensive Reconciliation

For full system-wide reconciliation:

```bash
npx tsx comprehensive_reconciliation.ts [command]
```

Commands:
- `status` - Show current reconciliation status
- `reconcile-all` - Reconcile all dates in the database
- `reconcile-range` - Reconcile a specific date range
- `reconcile-recent` - Reconcile recent data (default: last 30 days)
- `fix-critical` - Fix dates with known issues
- `report` - Generate detailed reconciliation report

### 4. Reprocess Single Day

For targeted data reprocessing:

```bash
npx tsx server/scripts/reprocessDay.ts YYYY-MM-DD
```

## Automated Monitoring and Alerts

The system includes automated monitoring to catch missing data before it causes issues:

1. **Automated Daily Check**: The `daily_reconciliation_check.ts` script runs automatically to find and fix issues.

2. **Reconciliation Reports**: The `comprehensive_reconciliation.ts report` command generates detailed reports on data completeness.

## Recovery Procedures

If missing data is detected:

1. **Identify the Gap**: Use `verify_date_integrity.ts` to identify missing data:
   ```bash
   npx tsx verify_date_integrity.ts --start YYYY-MM-DD --end YYYY-MM-DD
   ```

2. **Reprocess the Data**: Use the auto-fix feature or manual reprocessing:
   ```bash
   npx tsx verify_date_integrity.ts --start YYYY-MM-DD --end YYYY-MM-DD --auto-fix
   ```
   or for a single day:
   ```bash
   npx tsx server/scripts/reprocessDay.ts YYYY-MM-DD
   ```

3. **Verify Recovery**: Run verification again to confirm all data is properly reconciled:
   ```bash
   npx tsx verify_date_integrity.ts --start YYYY-MM-DD --end YYYY-MM-DD
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
   - Solution: Use `reprocessDay.ts` to re-ingest data from source APIs.

2. **Missing Bitcoin Calculations**: May occur when curtailment data exists but calculation failed.
   - Solution: Fix with `verify_date_integrity.ts --auto-fix` which only regenerates the calculations.

3. **Data Consistency Issues**: When totals don't match between tables.
   - Solution: Use `comprehensive_reconciliation.ts fix-critical` to apply consistency fixes.

## Recent Recoveries

### March 2025 Data Recovery

Successfully recovered missing data for March 1-2, 2025:

- March 1, 2025: Recovered 819 curtailment records (21,178.62 MWh)
- March 2, 2025: Recovered 2,444 curtailment records (61,575.86 MWh)
- Bitcoin calculations fully reconciled for all dates

The issue was identified and resolved using the tools documented in this guide.