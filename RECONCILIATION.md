# Bitcoin Mining Data Reconciliation Guide

This document provides comprehensive guidelines for ensuring 100% reconciliation between the `curtailment_records` and `historical_bitcoin_calculations` tables in our Bitcoin mining analytics platform.

## Understanding Reconciliation

Reconciliation in our context means ensuring that every unique combination of `settlement_date`, `settlement_period`, and `farm_id` in the `curtailment_records` table has corresponding Bitcoin mining calculations for each miner model in the `historical_bitcoin_calculations` table.

For each curtailment record, we expect exactly three Bitcoin calculations (one per miner model: S19J_PRO, S9, M20S).

## Using the Comprehensive Reconciliation Tool

We've created a new tool (`comprehensive_reconcile.ts`) specifically for ensuring 100% reconciliation. This tool offers various commands for checking and fixing reconciliation issues.

### Basic Usage

```bash
# Check overall reconciliation status
npx tsx comprehensive_reconcile.ts status

# Check reconciliation for a specific date
npx tsx comprehensive_reconcile.ts check-date 2025-02-28

# Fix reconciliation for a specific date
npx tsx comprehensive_reconcile.ts fix-date 2025-02-28

# Fix all dates with missing calculations
npx tsx comprehensive_reconcile.ts fix-all

# Fix a specific date range
npx tsx comprehensive_reconcile.ts fix-range 2025-02-01 2025-02-28
```

### When to Use

1. **Daily Verification**: Run the `status` command daily to ensure ongoing reconciliation
2. **After Data Ingestion**: After importing new curtailment data, run the appropriate fix command
3. **Data Audits**: Monthly run a full reconciliation using `fix-all`
4. **Issue Investigations**: Use `check-date` to investigate specific problem dates

## Reconciliation Process

The reconciliation process follows these steps:

1. **Verification**: Check if all curtailment records have corresponding Bitcoin calculations
2. **Identification**: Identify dates, periods, and farms with missing calculations
3. **Processing**: Calculate missing Bitcoin mining values based on curtailment records
4. **Validation**: Verify that all expected calculations have been created

## Troubleshooting Common Issues

### Missing Calculations

If you notice dates with missing calculations, check:

1. **Curtailment Data**: Ensure curtailment data is complete for the date
2. **API Access**: Verify API access for difficulty data
3. **Processing Errors**: Check logs for any processing errors during calculation

Use the `fix-date` command for the specific problematic date:

```bash
npx tsx comprehensive_reconcile.ts fix-date 2023-12-25
```

### Discrepancies in Totals

If reconciliation percentages are inconsistent:

1. Check for database indices that might be affecting query performance
2. Verify there are no duplicate records in either table
3. Ensure difficulty data is available for all dates

## Best Practices for Maintaining Reconciliation

1. **Regular Checks**: Schedule daily reconciliation checks
2. **Immediate Fixes**: Address any reconciliation issues as soon as they're detected
3. **Batch Processing**: For large historical fixes, use the date range command
4. **Logging**: Monitor logs for errors during calculation processes
5. **Data Validation**: Validate new curtailment data before processing

## Monthly Reconciliation Procedure

For a thorough monthly reconciliation:

1. Check the current status: `npx tsx comprehensive_reconcile.ts status`
2. Identify any problem dates
3. Run a full reconciliation: `npx tsx comprehensive_reconcile.ts fix-all`
4. Verify 100% reconciliation has been achieved
5. Document any issues encountered and their resolutions

## Technical Details

### Key Database Tables

- `curtailment_records`: Primary source of truth for curtailment data
  - Contains settlement date, period, farm ID, volume, and payment data
  
- `historical_bitcoin_calculations`: Bitcoin mining calculations
  - Contains settlement date, period, farm ID, miner model, and Bitcoin mined
  
### Reconciliation Logic

For 100% reconciliation, this condition must be satisfied:

```
Number of unique (date-period-farm) combinations in curtailment_records × 3 miner models
= 
Number of records in historical_bitcoin_calculations
```

## Support Tools

In addition to the comprehensive reconciliation tool, these existing tools can help with specific scenarios:

- `reconcile.ts`: Basic reconciliation script
- `reconciliation.ts`: Consolidated reconciliation system with CLI interface
- `fix_december_2023.ts`: Specialized tool for December 2023 issues

When in doubt, prefer using the new `comprehensive_reconcile.ts` tool as it incorporates the best features of all previous tools.