# Data Reingestion Guide

This guide provides a comprehensive reference for reingesting settlement data for a specific date in the system. Use this when you need to fix incomplete or corrupted data for any date.

## When to Use Data Reingestion

1. **Missing Settlement Periods**: When a date has incomplete settlement periods
2. **Inconsistent Data**: When summary totals don't match raw curtailment records
3. **Incorrect Totals**: When energy or payment values don't match Elexon API values
4. **Data Corruption**: When data for a specific date is corrupted or inconsistent

## Reingestion Process Overview

The complete reingestion process involves these key steps:

1. **Data Clearing**: Remove existing data for the target date
2. **Data Fetching**: Obtain correct data from the Elexon API for all periods
3. **Data Processing**: Process and insert the data into the database
4. **Summary Updates**: Recalculate daily, monthly, and yearly summaries
5. **Bitcoin Recalculations**: Update Bitcoin mining potential calculations
6. **Verification**: Confirm the totals match expected values

## Available Scripts

### 1. Complete Reingestion Script (`fixed_reingest_march_21.ts`)

Use this script to completely reingest all 48 settlement periods for a specific date.

```bash
# Run the script with TypeScript
npx tsx fixed_reingest_march_21.ts

# To adapt for another date, modify the TARGET_DATE, EXPECTED_TOTAL_PAYMENT and EXPECTED_TOTAL_ENERGY constants
```

### 2. Test Reingestion Script (`test_reingest_march_21.ts`)

Use this script to test the reingestion process with a small batch of periods.

```bash
# Run the script with TypeScript
npx tsx test_reingest_march_21.ts

# To adapt for another date, modify the TARGET_DATE and customize START_PERIOD and END_PERIOD
```

### 3. Staged Reingestion Script (`staged_reingest_march_21.ts`)

Use this script to reingest data in smaller batches (useful for troubleshooting).

```bash
# Run the script with TypeScript
npx tsx staged_reingest_march_21.ts

# Modify START_PERIOD and END_PERIOD to control the range
```

## Adapting Scripts for Other Dates

To adapt the reingestion scripts for other dates:

1. **Update the Target Date**: Change the `TARGET_DATE` constant
2. **Update Expected Values**: Set `EXPECTED_TOTAL_PAYMENT` and `EXPECTED_TOTAL_ENERGY` based on Elexon API data
3. **Review Period Logic**: Ensure the script processes all 48 settlement periods
4. **Test with a Small Batch**: Use the test script to verify the process works correctly
5. **Run Full Reingestion**: Execute the complete reingestion script

## Troubleshooting Common Issues

### 1. Missing BMU Mappings

If the script doesn't find BMU mappings:

- Check the BMU mapping file path
- Ensure the mapping file format is correct
- Verify BMU ID formats match those in the Elexon API response

### 2. API Rate Limiting

If you encounter API rate limiting:

- Increase the delay between API calls (`delay` function)
- Reduce batch sizes to process fewer periods at once
- Implement exponential backoff for retries

### 3. Database Connection Timeouts

If database operations timeout:

- Ensure the DB connection is refreshed between operations
- Process data in smaller batches
- Add explicit transaction handling

### 4. Verification Failures

If totals don't match expected values:

- Check the XML parsing logic for accuracy
- Verify price and volume calculations
- Ensure all settlement periods were processed
- Check for partial or failed API responses

## Data Integrity Considerations

To ensure complete data integrity during reingestion:

1. **Transaction Handling**: Wrap critical operations in transactions
2. **Logging**: Maintain detailed logs of all operations
3. **Verification**: Always verify totals against expected values
4. **Backup**: Take a database backup before major reingestions
5. **Documentation**: Document the reason for reingestion and the results

## Example: Complete Reingestion Workflow

Here's a typical workflow for a complete data reingestion:

1. Identify the date with problematic data
2. Determine the correct energy and payment totals from Elexon API
3. Create a backup of the current database state
4. Customize the reingestion script with the target date and expected values
5. Run a test reingestion on a small number of periods
6. Verify the test results are correct
7. Run the complete reingestion script
8. Verify all 48 settlement periods are processed
9. Confirm the corrected totals match expected values
10. Document the reingestion process and results

## Performance Optimization

To optimize the reingestion process:

1. **Batch Processing**: Process records in appropriate batch sizes
2. **Connection Management**: Refresh DB connections to prevent timeouts
3. **Parallel Processing**: Consider parallel processing for non-dependent operations
4. **Resource Monitoring**: Monitor system resources during long reingestion jobs

## Maintenance Responsibilities

Regular data maintenance responsibilities:

1. **Periodic Reconciliation**: Regularly check for data inconsistencies
2. **Process Improvement**: Refine the reingestion process based on experience
3. **Documentation Updates**: Keep this guide updated with new findings
4. **Script Maintenance**: Keep reingestion scripts up-to-date with API changes