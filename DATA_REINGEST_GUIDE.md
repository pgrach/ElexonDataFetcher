# Data Reingestion Guide

This guide provides a comprehensive reference for reingesting settlement data for a specific date in the energy curtailment system. This process is useful when you need to fix incomplete or corrupted data.

## When to Use Data Reingestion

You may need to perform a complete data reingestion in the following scenarios:

1. **Incomplete Data**: When some settlement periods are missing for a date
2. **Corrupted Data**: When existing data is corrupted or inaccurate
3. **Data Reconciliation**: When you need to ensure totals match expected values
4. **API Data Updates**: When the source API has updated its data for a historical date

## Prerequisites

Before starting a data reingestion process, ensure you have:

1. Database access with sufficient permissions
2. Valid BMU mappings for wind farms
3. Access to the Elexon API (if using live data)
4. Sufficient system resources (memory and CPU)

## Reingestion Process Overview

The complete reingestion process consists of the following steps:

1. **Data Clearing**: Remove existing data for the specified date
2. **Data Fetching**: Retrieve fresh data from the Elexon API for all settlement periods
3. **Data Processing**: Process and insert the new data into the database
4. **Summary Updates**: Refresh daily, monthly, and yearly summaries
5. **Bitcoin Calculations**: Update Bitcoin mining potential calculations
6. **Verification**: Confirm all data has been correctly processed

## Using the Reingestion Scripts

### Basic Usage

For a standard reingestion of a single date:

```bash
npx tsx data_reingest_reference.ts 2025-MM-DD
```

### Configuration Options

The scripts provide several configuration options:

- `TARGET_DATE`: The date for which data should be reingested (YYYY-MM-DD format)
- `BATCH_SIZE`: Number of settlement periods to process in each batch (default: 6)
- `MAX_PERIODS`: Maximum number of periods to process (default: 48)
- `API_THROTTLE_MS`: Delay between API calls to avoid rate limiting (default: 500ms)

### Examples

**Reingest a specific date:**

```bash
npx tsx data_reingest_reference.ts 2025-03-21
```

**Reingest March 28, 2025 with specific focus:**

```bash
npx tsx complete_reingest_march_28.ts
```

## Troubleshooting

### Common Issues

1. **API Rate Limiting**: If you encounter API rate limits, increase the `API_THROTTLE_MS` value
2. **Database Timeout**: For large data sets, try reducing the `BATCH_SIZE` value
3. **Missing Periods**: Check the Elexon API availability for specific periods
4. **Inconsistent Totals**: Verify BMU mappings and price calculations

### Error Logs

All reingestion scripts create detailed log files with timestamps in the `logs/` directory. Review these logs for troubleshooting:

- `reingest_YYYY-MM-DD.log`: Main reingestion log file
- `performance_YYYY-MM-DD.log`: Performance statistics
- `cache_YYYY-MM-DD.log`: API caching information

## Special Dates

Some dates may require special handling due to daylight saving changes or unusual market conditions:

1. **Daylight Saving Time**: Days with 46 or 50 periods instead of 48
2. **High Curtailment Events**: Dates with unusually high curtailment volumes
3. **Market Disruptions**: Dates with incomplete or delayed settlement data

## Database Structure Overview

The reingestion process interacts with the following key tables:

1. `curtailment_records`: Individual curtailment events by settlement period
2. `daily_summaries`: Daily aggregated curtailment data
3. `monthly_summaries`: Monthly aggregated curtailment data 
4. `yearly_summaries`: Yearly aggregated curtailment data
5. `historical_bitcoin_calculations`: Bitcoin mining potential calculations
6. `monthly_bitcoin_summaries`: Monthly Bitcoin mining summaries
7. `yearly_bitcoin_summaries`: Yearly Bitcoin mining summaries

## Technical Notes

### Data Consistency

The scripts ensure data consistency by:

1. Using database transactions for critical operations
2. Implementing ON CONFLICT rules for upserts
3. Verifying data before and after processing

### Performance Optimization

For large-scale reingestion operations:

1. Process data in batches to manage memory usage
2. Use connection pooling for database operations
3. Implement parallel processing where appropriate

## References

- **Elexon API Documentation**: [https://www.elexon.co.uk/documents/training-guidance/bsc-guidance-notes/bmrs-api-and-data-push-user-guide/](https://www.elexon.co.uk/documents/training-guidance/bsc-guidance-notes/bmrs-api-and-data-push-user-guide/)
- **PostgreSQL Documentation**: [https://www.postgresql.org/docs/](https://www.postgresql.org/docs/)
- **Bitcoin Mining Calculations**: See internal documentation for details on mining efficiency calculations