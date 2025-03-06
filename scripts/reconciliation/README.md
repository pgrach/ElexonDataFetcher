# Bitcoin Mining Analytics Platform - Reconciliation Scripts

This directory contains scripts for data reconciliation, verification, and integrity maintenance for the Bitcoin Mining Analytics platform.

## Scripts

- `complete_reingestion_process.ts` - Comprehensive solution for reingesting Elexon API data for a specific date, processing curtailment records, and calculating Bitcoin mining potential across all settlement periods for multiple miner models.

- `daily_reconciliation_check.ts` - Automatically checks the reconciliation status for recent dates and processes any missing calculations using the centralized reconciliation system. Includes robust error handling, connection resilience, and comprehensive logging.

- `unified_reconciliation.ts` - A comprehensive solution for ensuring data integrity between the curtailment_records and historical_bitcoin_calculations tables. Combines efficient batch processing, careful connection handling, progress tracking with checkpoints, advanced retry logic, and comprehensive logging.

## Usage

You can run these scripts directly using the npx command:

```bash
# Run complete reingestion process for a specific date
npx tsx scripts/reconciliation/complete_reingestion_process.ts [date]

# Run daily reconciliation check
npx tsx scripts/reconciliation/daily_reconciliation_check.ts [days=2] [forceProcess=false]

# Run unified reconciliation with various commands
npx tsx scripts/reconciliation/unified_reconciliation.ts [command] [options]

# Unified reconciliation commands:
#   status                 - Show current reconciliation status
#   analyze                - Analyze missing calculations and detect issues
#   reconcile [batchSize]  - Process all missing calculations with specified batch size
#   date YYYY-MM-DD        - Process a specific date
#   range YYYY-MM-DD YYYY-MM-DD [batchSize] - Process a date range
#   critical DATE          - Process a problematic date with extra safeguards
#   spot-fix DATE PERIOD FARM - Fix a specific date-period-farm combination
```

## Examples

```bash
# Complete reingestion process for March 6, 2025
npx tsx scripts/reconciliation/complete_reingestion_process.ts 2025-03-06

# Run daily reconciliation check for the last 3 days
npx tsx scripts/reconciliation/daily_reconciliation_check.ts 3

# Show current reconciliation status
npx tsx scripts/reconciliation/unified_reconciliation.ts status

# Process a specific date
npx tsx scripts/reconciliation/unified_reconciliation.ts date 2025-03-06

# Process a date range with batch size 50
npx tsx scripts/reconciliation/unified_reconciliation.ts range 2025-03-01 2025-03-06 50
```

## Script Runner

Alternatively, you can use the script runner for easier access:

```bash
# Run reconciliation scripts
npx tsx scripts/run-reconciliation.ts [script] [args]
```

For example:

```bash
# Run daily reconciliation check for the last 2 days
npx tsx scripts/run-reconciliation.ts daily 2

# Run complete reingestion process for March 6, 2025
npx tsx scripts/run-reconciliation.ts complete 2025-03-06

# Run unified reconciliation status check
npx tsx scripts/run-reconciliation.ts unified status
```