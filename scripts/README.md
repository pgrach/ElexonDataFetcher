# Bitcoin Mining Analytics Platform - Scripts Directory

This directory contains all the utility scripts for the Bitcoin Mining Analytics platform. The scripts are organized into subdirectories by function for better maintainability.

## Directory Structure

- `reconciliation/` - Scripts for data reconciliation processes
- `data-processing/` - Scripts for data ingestion and processing
- `migrations/` - Database migration scripts
- `utilities/` - Miscellaneous utility scripts

## Key Scripts

### Reconciliation Scripts

- `reconciliation/complete_reingestion_process.ts` - Comprehensive solution for reingesting Elexon API data for a specific date
- `reconciliation/daily_reconciliation_check.ts` - Automatically checks reconciliation status for recent dates
- `reconciliation/unified_reconciliation.ts` - Advanced reconciliation system with multiple commands

### Data Processing Scripts

- `data-processing/reingest-data.ts` - Standardized way to reingest Elexon data for a specific date

### Migration Scripts

- `migrations/run_migration.ts` - PostgreSQL database migration runner

## Usage

### Reconciliation Scripts

#### Complete Reingestion Process

```bash
npx tsx reconciliation/complete_reingestion_process.ts [date]
```

Example:
```bash
npx tsx reconciliation/complete_reingestion_process.ts 2025-03-04
```

#### Daily Reconciliation Check

```bash
npx tsx reconciliation/daily_reconciliation_check.ts [days=2] [forceProcess=false]
```

Example:
```bash
npx tsx reconciliation/daily_reconciliation_check.ts 3 true
```

#### Unified Reconciliation System

```bash
npx tsx reconciliation/unified_reconciliation.ts [command] [options]
```

Commands:
- `status` - Show current reconciliation status
- `analyze` - Analyze missing calculations and detect issues
- `reconcile [batchSize]` - Process all missing calculations
- `date YYYY-MM-DD` - Process a specific date
- `range YYYY-MM-DD YYYY-MM-DD [batchSize]` - Process a date range
- `critical DATE` - Process a problematic date with extra safeguards
- `spot-fix DATE PERIOD FARM` - Fix a specific date-period-farm combination

Example:
```bash
npx tsx reconciliation/unified_reconciliation.ts date 2025-03-04
```

### Data Processing Scripts

#### Reingest Data

```bash
npx tsx data-processing/reingest-data.ts <date> [options]
```

Options:
- `--skip-bitcoin` - Skip Bitcoin calculation updates
- `--skip-verify` - Skip verification step
- `--verbose` - Show detailed logs during processing
- `--help` - Show help message

Example:
```bash
npx tsx data-processing/reingest-data.ts 2025-03-04 --verbose
```

### Migration Scripts

#### Run Migration

```bash
npx tsx migrations/run_migration.ts
```

## Script Runner Utilities

The project includes centralized script runners to simplify execution:

### Reconciliation Runner

```bash
npm run reconcile -- [script] [args]
```

Available Scripts:
- `daily` - Run the daily reconciliation check
- `complete` - Run the complete reingestion process
- `unified` - Run the unified reconciliation system

Example:
```bash
npm run reconcile -- daily 2
npm run reconcile -- complete 2025-03-06
npm run reconcile -- unified status
```

### Data Processing Runner

```bash
npm run process-data -- [script] [args]
```

Available Scripts:
- `reingest` - Reingest data for a specific date

Example:
```bash
npm run process-data -- reingest 2025-03-06
```

## Checkpoint Files

Several scripts use checkpoint files to track progress and enable resuming interrupted processes. These files are stored in the `data/checkpoints/` directory:

- `reconciliation_checkpoint.json` - Used by the unified reconciliation system
- `daily_reconciliation_checkpoint.json` - Used by the daily reconciliation check

## Logging

All scripts output logs to the console and, in some cases, to log files in the `logs/` directory. The log files are named according to the script and date of execution.

## Maintenance

Scripts in this directory are actively used and maintained. For deprecated scripts that are no longer in use, see the `backup/` directory.