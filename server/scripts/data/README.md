# Data Processing Scripts

This directory contains scripts for data processing, ingestion, and data integrity maintenance.

## Available Scripts

### ingestMonthlyData.ts

Processes monthly data ingestion for settlement periods, fetching curtailment data from Elexon.

#### Usage

```bash
npx tsx server/scripts/data/ingestMonthlyData.ts [YYYY-MM] [startDay] [endDay]
```

Parameters:
- `YYYY-MM`: The year and month to process (e.g., 2025-03)
- `startDay` (optional): First day of the month to process (default: 1)
- `endDay` (optional): Last day of the month to process (default: last day of month)

#### Purpose

This script handles the bulk ingestion of curtailment data from the Elexon API. It:
- Processes data day by day for the specified month
- Tracks progress in the database
- Handles API rate limits and retries

### updateHistoricalCalculations.ts

Updates historical Bitcoin calculations for periods where data is missing or outdated.

#### Usage

```bash
npx tsx server/scripts/data/updateHistoricalCalculations.ts
```

#### Purpose

This script:
- Automatically identifies dates with missing or incomplete Bitcoin calculations
- Processes data in batches to avoid overwhelming the system
- Maintains progress state to allow resuming after interruptions
- Generates verification reports

### processDifficultyMismatch.ts

Detects and corrects difficulty mismatches in historical Bitcoin calculation records.

#### Usage

```bash
npx tsx server/scripts/data/processDifficultyMismatch.ts
```

#### Purpose

This script:
- Identifies records where the difficulty value doesn't match the expected value for the date
- Corrects the difficulty value in the database
- Generates a progress report of corrected records

## Common Features

All data processing scripts include:
- Progress tracking and resumability
- Comprehensive error handling
- Detailed logging
- Verification and validation steps

## Integration with the Main System

These scripts are designed to be run manually or through scheduled tasks (e.g., cron jobs). They complement the automatic daily reconciliation system but provide more targeted functionality for specific data management tasks.