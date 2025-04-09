# Complete Data Reingestion Guide

This guide provides step-by-step instructions for reingesting all data from Elexon API and updating all dependent tables.

## Overview

You've requested to reingest all data for the curtailment records table from the Elexon API and update all dependent tables. The scripts created for this purpose handle:

1. Clearing existing data from specified tables if needed
2. Fetching fresh data from the Elexon API
3. Reinserting curtailment records
4. Updating all summary tables (daily, monthly, yearly)
5. Recalculating Bitcoin mining potential across all miner models
6. Updating wind generation data

## Step 1: Reingest All Data

The most straightforward option is to use the complete reingestion script, which handles all aspects of the process:

```bash
npm run tsx reingest_all_data.ts [startDate] [endDate]
```

This script will:
- Reingest curtailment records for all dates in the database (or the specified date range)
- Recalculate all summary tables
- Refresh all Bitcoin mining calculations
- Update wind generation data (if a date range is specified)

**Example for a specific time period:**
```bash
npm run tsx reingest_all_data.ts 2025-01-01 2025-03-31
```

## Step 2: Verify Data Integrity

After reingestion, you can verify data integrity using:

```bash
npm run tsx server/scripts/verify_data_integrity.ts
```

This will check that the data in all tables is consistent, particularly ensuring that summary tables accurately reflect the data in the curtailment records.

## Advanced Usage: Step-by-Step Approach

If you prefer more control over the reingestion process, you can perform the steps individually:

### 1. Clear Existing Data (Optional)

If you want to start with a clean slate, you can use the clear database tables script:

```bash
# Clear all tables
npm run tsx server/scripts/clear_database_tables.ts clear-all

# Or clear specific tables
npm run tsx server/scripts/clear_database_tables.ts clear-curtailment 2025-01-01 2025-03-31
```

### 2. Reingest Curtailment Records Only

```bash
npm run tsx server/scripts/reingest_all_curtailment_data.ts 2025-01-01 2025-03-31
```

### 3. Update Wind Generation Data Separately

```bash
npm run tsx server/scripts/update_wind_generation_for_dates.ts 2025-01-01 2025-03-31
```

## Troubleshooting Common Issues

### API Rate Limiting

The Elexon API has rate limits. If you encounter rate limiting:

- The scripts have built-in retry mechanisms
- Consider reingesting smaller date ranges
- Watch for rate limit messages in the console and adjust as needed

### Database Errors

If you encounter database errors:

- Check for database connection issues
- Ensure all tables exist (the scripts assume schema is already set up)
- For permission errors, check that your database user has proper access

### Missing Data

If summaries show missing data after reingestion:

1. Verify the API returned data for the specified dates
2. Check the console logs for any skipped dates or periods
3. Try running the reingestion for specific problematic dates

## Performance Considerations

- Reingesting large date ranges can take significant time
- The process is CPU and memory intensive
- Consider running during off-peak hours for large reingestions
- Monitor system resources during the process

## Next Steps

After completing the reingestion:

1. View the application in your browser to verify the data displays correctly
2. Check various summary views (daily, monthly, yearly) to confirm consistency
3. Verify Bitcoin mining calculations across different miner models

For regular data maintenance, consider setting up scheduled reingestion for recent dates only using the date range parameters.