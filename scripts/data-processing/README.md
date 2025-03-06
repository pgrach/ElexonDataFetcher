# Bitcoin Mining Analytics Platform - Data Processing Scripts

This directory contains scripts for data processing, ingestion, and reingestion for the Bitcoin Mining Analytics platform.

## Scripts

- `reingest-data.ts` - Standardized tool for reingesting Elexon data for a specific date, updating curtailment records, and triggering cascading updates to Bitcoin calculations.

## Usage

You can run these scripts directly using the npx command:

```bash
# Reingest data for a specific date
npx tsx scripts/data-processing/reingest-data.ts <date> [options]

# Available options:
#   --skip-bitcoin    Skip Bitcoin calculation updates
#   --skip-verify     Skip verification step
#   --verbose         Show detailed logs during processing
#   --help            Show help message
```

Examples:

```bash
# Reingest data for March 6, 2025 with detailed logs
npx tsx scripts/data-processing/reingest-data.ts 2025-03-06 --verbose

# Reingest data but skip Bitcoin calculation updates
npx tsx scripts/data-processing/reingest-data.ts 2025-03-06 --skip-bitcoin
```

## Script Runner

Alternatively, you can use the script runner for easier access:

```bash
# Run data processing scripts
npx tsx scripts/run-data-processing.ts [script] [args]
```

For example:

```bash
# Reingest data for March 6, 2025
npx tsx scripts/run-data-processing.ts reingest 2025-03-06
```