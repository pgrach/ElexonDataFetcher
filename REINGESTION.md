# Data Reingestion Tools

This document provides instructions for using the data reingestion tools to refresh curtailment records and wind generation data from the Elexon API.

## Overview

These tools allow you to:
1. Reingest curtailment records from the Elexon API
2. Update all dependent tables (summaries and Bitcoin calculations)  
3. Update wind generation data from the Elexon API
4. Clear database tables as needed for troubleshooting or fresh starts

## Available Scripts

### Reingest All Data

This script performs a complete reingestion of all curtailment data and updates wind generation data.

```bash
npm run tsx reingest_all_data.ts [startDate] [endDate]
```

**Arguments:**
- `startDate` (optional): Beginning date for reingestion in YYYY-MM-DD format
- `endDate` (optional): Ending date for reingestion in YYYY-MM-DD format

**Example:**
```bash
npm run tsx reingest_all_data.ts 2025-01-01 2025-03-31
```

### Reingest Curtailment Records Only

This script only reingests the curtailment records and their dependent tables without updating wind generation data.

```bash
npm run tsx server/scripts/reingest_all_curtailment_data.ts [startDate] [endDate]
```

**Arguments:**
- `startDate` (optional): Beginning date for reingestion in YYYY-MM-DD format
- `endDate` (optional): Ending date for reingestion in YYYY-MM-DD format

**Example:**
```bash
npm run tsx server/scripts/reingest_all_curtailment_data.ts 2025-03-01 2025-03-31
```

### Update Wind Generation Data Only

This script only updates wind generation data without affecting curtailment records.

```bash
# Update a specific date range
npm run tsx server/scripts/update_wind_generation_for_dates.ts <startDate> <endDate>

# Update the most recent days
npm run tsx server/scripts/update_wind_generation_for_dates.ts <numberOfDays>
```

### Clear Database Tables

This script allows you to clear specific database tables or all data for troubleshooting or a fresh start.

```bash
npm run tsx server/scripts/clear_database_tables.ts <operation> [startDate] [endDate] [minerModel]
```

**Operations:**
- `clear-all` - Clear all data from all tables
- `clear-curtailment` - Clear curtailment records (optional date range)
- `clear-bitcoin` - Clear Bitcoin calculations (optional date range and miner model)
- `clear-summaries` - Clear all summary tables
- `clear-wind` - Clear wind generation data (optional date range)

**Examples:**
```bash
# Clear all curtailment records from January to March 2025
npm run tsx server/scripts/clear_database_tables.ts clear-curtailment 2025-01-01 2025-03-31

# Clear Bitcoin calculations for a specific miner model and date range
npm run tsx server/scripts/clear_database_tables.ts clear-bitcoin 2025-01-01 2025-03-31 S19J_PRO

# Clear all summary tables but keep raw data
npm run tsx server/scripts/clear_database_tables.ts clear-summaries
```

**Arguments:**
- Format 1:
  - `startDate`: Beginning date for update in YYYY-MM-DD format
  - `endDate`: Ending date for update in YYYY-MM-DD format
- Format 2:
  - `numberOfDays`: Number of recent days to update (including today)

**Examples:**
```bash
# Update a specific date range
npm run tsx server/scripts/update_wind_generation_for_dates.ts 2025-03-01 2025-03-31

# Update the last 14 days
npm run tsx server/scripts/update_wind_generation_for_dates.ts 14
```

## Data Pipeline Overview

The data pipeline consists of the following main components:

1. **Curtailment Records**: Raw data from Elexon API representing wind farm curtailment events
2. **Summary Tables**: Aggregated data at daily, monthly, and yearly levels
3. **Bitcoin Mining Calculations**: Calculations of potential Bitcoin mining based on curtailed energy
4. **Wind Generation Data**: Additional data from Elexon API about wind generation

When running a full reingestion:
1. Existing curtailment records for the specified dates are deleted
2. New records are fetched from the Elexon API
3. Summary tables are updated based on the new records
4. Bitcoin calculations are refreshed for all miner models
5. Wind generation data is updated (if specified)

## Best Practices

- Always specify a date range when possible to limit the amount of data being processed
- For large date ranges, consider breaking the process into smaller chunks
- Monitor the console output for progress information and error messages
- Run reingestion during off-peak hours for large data sets

## Troubleshooting

If you encounter errors during reingestion:

1. Check the error messages in the console for specific API or database issues
2. Verify that you have proper network connectivity to access the Elexon API
3. For database-related errors, ensure your database connection is working properly
4. If processing hangs on a specific date, try reingesting just that date separately
5. For persistent issues, check the Elexon API documentation for any service changes or limitations