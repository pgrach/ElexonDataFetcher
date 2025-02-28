# Bitcoin Calculation Reconciliation

This document describes the tools for ensuring 100% reconciliation between curtailment_records and historical_bitcoin_calculations tables.

## Background

For each curtailment record, we should have Bitcoin calculations for all miner models. This ensures we can accurately track and report on Bitcoin mining potential across different hardware configurations.

## Reconciliation Tools

We've created streamlined utilities to check and ensure complete reconciliation:

### 1. Check Reconciliation Status

Run the following command to check the current reconciliation status without making any changes:

```bash
npx tsx check_reconciliation_status.ts
```

This will:
- Calculate the overall reconciliation percentage
- Show counts for each miner model
- Identify dates with missing calculations (if any)

### 2. Run Full Reconciliation

To process any missing calculations and achieve 100% reconciliation:

```bash
npx tsx run_reconciliation.ts
```

This will:
- Identify dates with missing or incomplete calculations
- Process those dates in batches
- Report on the progress and results

## Implementation Details

The reconciliation process leverages the `historicalReconciliation` service which handles:

1. Verifying if a date's data is complete
2. Reprocessing curtailment data if needed
3. Calculating Bitcoin mining potential for all miner models
4. Ensuring data consistency across tables

## File Structure

- `reconcile.ts` - Core reconciliation functions
- `run_reconciliation.ts` - Script to run the full reconciliation process
- `check_reconciliation_status.ts` - Script to check current status without making changes

## Troubleshooting

If reconciliation fails for specific dates, it may be due to:

1. Missing or invalid difficulty data
2. API rate limits when fetching external data
3. Database connectivity issues

Check the output logs for specific error messages. Typically, running the reconciliation script again after a short delay will resolve these issues.