# Bitcoin Calculation Reconciliation Tools Guide

This guide provides an overview of the core reconciliation tools available in this project to help ensure 100% data integrity between `curtailment_records` and `historical_bitcoin_calculations` tables.

## Tool Overview

### Core Reconciliation Tools

1. **simple_reconcile.ts**  
   A streamlined tool for checking and fixing reconciliation issues:
   ```bash
   npx tsx simple_reconcile.ts status         # Check overall status
   npx tsx simple_reconcile.ts find           # Find dates with missing calculations
   npx tsx simple_reconcile.ts date YYYY-MM-DD # Fix a specific date
   npx tsx simple_reconcile.ts december       # Fix December 2023 specifically
   npx tsx simple_reconcile.ts all            # Fix all missing dates (use with caution)
   ```

2. **reconciliation.ts**  
   A consolidated system with multiple reconciliation operations:
   ```bash
   npx tsx reconciliation.ts status           # Check status
   npx tsx reconciliation.ts reconcile        # Fix all missing
   npx tsx reconciliation.ts date 2023-12-25  # Check/fix specific date
   npx tsx reconciliation.ts period 2023-12-21 7 # Fix specific period
   npx tsx reconciliation.ts combo 2023-12-21 7 FARM_ID MODEL # Fix specific combination
   npx tsx reconciliation.ts batch 2023-12-21 10 # Process batch with limit
   npx tsx reconciliation.ts december         # Fix December 2023
   npx tsx reconciliation.ts range 2023-12-01 2023-12-31 # Fix date range
   ```

3. **daily_reconciliation_check.ts**  
   Automatically checks and fixes the current and previous day:
   ```bash
   npx tsx daily_reconciliation_check.ts
   ```

### Status and Monitoring Tools

4. **check_reconciliation_status.ts**  
   A simple utility to quickly check overall reconciliation status:
   ```bash
   npx tsx check_reconciliation_status.ts
   ```

5. **reconciliation_progress_report.ts**  
   Generates detailed reports on reconciliation progress:
   ```bash
   npx tsx reconciliation_progress_report.ts
   ```

6. **reconciliation_progress_check.ts**  
   Provides a quick overview of current reconciliation status:
   ```bash
   npx tsx reconciliation_progress_check.ts
   ```

7. **reconciliation_visualization.ts**  
   Generates visual representations of reconciliation progress:
   ```bash
   npx tsx reconciliation_visualization.ts
   ```

## Data Model

The reconciliation process ensures consistency between:

- **curtailment_records**: Contains raw curtailment data (date, period, farmId, volume, etc.)
- **historical_bitcoin_calculations**: Contains calculated Bitcoin mining potential for each curtailment record across all miner models

## Reconciliation Process

1. **Audit**: Identify dates with missing calculations
2. **Fetch Difficulty**: Retrieve historical Bitcoin network difficulty from DynamoDB
3. **Calculate**: Process missing calculations for each miner model (S19J_PRO, S9, M20S)
4. **Verify**: Confirm that all expected calculations now exist

## Troubleshooting

**Common Issues**:
- **DynamoDB Connectivity**: If difficulty data cannot be retrieved, a default value is used
- **Timeouts**: For dates with many records, use batch processing or period-specific reconciliation
- **Partial Completions**: Check the specific miner models that are incomplete for a date

## Monitoring Progress

- Use `npx tsx simple_reconcile.ts status` to check overall reconciliation status
- Review the RECONCILIATION_PROGRESS.md file for the latest progress report
- Use `npx tsx reconciliation_progress_report.ts` for a detailed status breakdown

## Best Practices

1. Process one month at a time to avoid memory issues
2. Start with dates having the most missing calculations
3. Regularly verify your progress with status checks
4. Use the `daily_reconciliation_check.ts` in scheduled jobs to maintain reconciliation

## Technical Details

- **Difficulty Data**: Historical Bitcoin network difficulty is stored in DynamoDB
- **Miner Models**: We track calculations for three miner models (S19J_PRO, S9, M20S)
- **Batch Processing**: Large datasets are processed in controlled batches to prevent timeouts
- **Error Handling**: All tools include robust error handling and graceful failure recovery