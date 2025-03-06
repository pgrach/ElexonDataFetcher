# Data Reingestion Tools

This document explains how to reingest historical wind farm curtailment data from the Elexon API. The reingestion process updates curtailment records and triggers cascading updates to Bitcoin mining calculation tables.

## Overview

The reingestion process follows these steps:

1. Fetch curtailment data from Elexon API for the specified date
2. Update all curtailment_records for this date
3. Recalculate Bitcoin mining potential for all miner models
4. Update summary tables and statistics

## Using the Reingestion Tool

The `reingest-data.ts` script provides a convenient way to reingest data for a specific date.

### Prerequisites

- Node.js and npm must be installed
- Database must be properly configured (DATABASE_URL environment variable)
- AWS credentials must be configured for DynamoDB access (for Bitcoin difficulty data)

### Basic Usage

To reingest data for a specific date:

```bash
npx tsx reingest-data.ts 2025-03-04
```

This will:
1. Fetch the latest data from Elexon API for March 4, 2025
2. Update all curtailment records in the database
3. Recalculate Bitcoin mining potential for all miner models (S19J_PRO, S9, M20S)
4. Verify and display the results

### Command Line Options

The script supports several options:

- `--skip-bitcoin`: Skip Bitcoin calculation updates
- `--skip-verify`: Skip verification step
- `--verbose`: Show detailed logs during processing
- `--help`: Display help information

### Examples

Reingest data with verbose logging:
```bash
npx tsx reingest-data.ts 2025-03-05 --verbose
```

Reingest data but skip Bitcoin calculations:
```bash
npx tsx reingest-data.ts 2025-03-06 --skip-bitcoin
```

## Using the API Endpoint

For programmatic access, you can use the API endpoint:

```
POST /api/ingest/:date
```

Example using curl:
```bash
curl -X POST https://your-application-url/api/ingest/2025-03-04
```

The API endpoint will return a JSON response with statistics about the reingestion:

```json
{
  "message": "Successfully re-ingested data for 2025-03-04",
  "stats": {
    "records": 4651,
    "periods": 48,
    "volume": "98765.43",
    "payment": "-2345678.90"
  }
}
```

## Troubleshooting

If the reingestion process fails, check the following:

1. Ensure the date is in the correct format (YYYY-MM-DD)
2. Verify AWS credentials for DynamoDB access
3. Check database connectivity and permissions
4. Examine server logs for specific error messages

For persistent issues, run the script with the `--verbose` flag to get more detailed logging.