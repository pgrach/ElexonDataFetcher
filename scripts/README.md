# Bitcoin Mining Analytics Platform - Scripts

This directory contains utility scripts that help with various tasks related to the Bitcoin Mining Analytics platform.

## Directory Structure

- `reconciliation/` - Scripts for data reconciliation and verification
  - `complete_reingestion_process.ts` - Comprehensive solution for reingesting Elexon API data
  - `daily_reconciliation_check.ts` - Automatic check for reconciliation status of recent dates
  - `unified_reconciliation.ts` - Unified system for ensuring data integrity

- `data-processing/` - Scripts for data processing and ingestion
  - `reingest-data.ts` - Tool for reingesting Elexon data for a specific date

- `migrations/` - Database migration scripts
  - `run_migration.ts` - Runs PostgreSQL migrations

- `utilities/` - General utility scripts

## Running Scripts

Since we can't modify package.json, you can run these scripts directly using the npx command:

### Reconciliation Scripts

```bash
# Run daily reconciliation check
npx tsx scripts/reconciliation/daily_reconciliation_check.ts [days=2] [forceProcess=false]

# Run complete reingestion process
npx tsx scripts/reconciliation/complete_reingestion_process.ts [date]

# Run unified reconciliation
npx tsx scripts/reconciliation/unified_reconciliation.ts [command] [options]
```

### Data Processing Scripts

```bash
# Reingest data for a specific date
npx tsx scripts/data-processing/reingest-data.ts <date> [options]
```

### Migration Scripts

```bash
# Run database migrations
npx tsx scripts/migrations/run_migration.ts
```

## Script Runners

Alternatively, you can use the script runners we've created:

```bash
# Run reconciliation scripts
npx tsx scripts/run-reconciliation.ts [script] [args]

# Run data processing scripts
npx tsx scripts/run-data-processing.ts [script] [args]
```

For example:

```bash
# Run daily reconciliation check for the last 2 days
npx tsx scripts/run-reconciliation.ts daily 2

# Reingest data for March 6, 2025
npx tsx scripts/run-data-processing.ts reingest 2025-03-06
```

## Checkpoints

Checkpoint files are now stored in the `data/checkpoints/` directory to keep the root directory clean.