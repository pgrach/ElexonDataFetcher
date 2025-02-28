# Reconciliation System

This document provides an overview of the Bitcoin calculation reconciliation system that ensures data integrity between curtailment records and their corresponding Bitcoin mining calculations.

## Overview

The reconciliation system ensures that for every curtailment record (defined by a unique date, period, and farm combination), there are corresponding Bitcoin mining calculations for each supported miner model.

## Key Concepts

1. **Unique Period-Farm Combinations**: The system tracks unique combinations of settlement date, period, and farm ID. Each unique combination should have one calculation per miner model.

2. **Miner Models**: The system tracks calculations for three standard miner models:
   - S19J_PRO
   - S9
   - M20S

3. **Reconciliation Percentage**: The percentage of existing calculations compared to expected calculations. 100% reconciliation means all required calculations exist.

## Reconciliation Scripts

### 1. Check Reconciliation Status

`check_reconciliation_status.ts` provides a snapshot of the current reconciliation status:

```bash
npx tsx check_reconciliation_status.ts
```

This shows overall statistics including:
- Total curtailment records
- Total Bitcoin calculations by miner model
- Current reconciliation percentage

### 2. Test Reconciliation for a Specific Date

`test_reconcile_date.ts` allows testing reconciliation for a specific date:

```bash
npx tsx test_reconcile_date.ts
```

Edit the `TARGET_DATE` constant in the file to test different dates.

### 3. Run Full Reconciliation

`reconcile.ts` performs a comprehensive reconciliation across all dates with missing calculations:

```bash
npx tsx reconcile.ts
```

This script:
- Identifies dates with missing calculations
- Processes them in batches
- Reports on results

### 4. Run Reconciliation as a One-Off

`run_reconciliation.ts` provides a simplified wrapper for running reconciliation:

```bash
npx tsx run_reconciliation.ts
```

## Implementation Notes

1. **Calculation Logic**: The system bases the expected calculation count on unique period-farm combinations rather than raw curtailment record counts. This accounts for cases where multiple curtailment records exist for the same period-farm combination.

2. **Database Schema**: 
   - `curtailment_records` tracks wind farm curtailment events
   - `historical_bitcoin_calculations` stores the Bitcoin mining calculations for each curtailment event and miner model

3. **Handling Duplicates**: The system is designed to handle cases where multiple curtailment records exist for the same period-farm combination, ensuring accurate reconciliation percentages.

## Troubleshooting

If reconciliation is not reaching 100%:

1. Check for specific dates with missing calculations using `findDatesWithMissingCalculations` in `reconcile.ts`
2. Run a targeted test using `test_reconcile_date.ts` for those dates
3. Check the historical reconciliation service for any errors in processing those dates
4. Verify DynamoDB difficulty data is available for the dates in question

## Maintenance

Regular maintenance tasks:

1. Monitor reconciliation percentage via the check script
2. Run full reconciliation after data imports
3. Keep miner models up to date if new models are added