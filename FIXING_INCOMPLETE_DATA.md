# Fixing Incomplete Data

This guide provides detailed instructions for fixing data integrity issues in the Bitcoin Mining Potential system.

## Common Data Issues

Several types of data issues can occur in the system:

1. **Missing Periods**: Some of the 48 settlement periods for a date may be missing
2. **Inconsistent Data**: Curtailment data doesn't match what's available from the Elexon API
3. **Cascade Failures**: Issues where one missing element affects multiple downstream tables
4. **External Service Failures**: Problems with DynamoDB, API rate limits, or other external services

## Verification Before Fixing

Before attempting to fix data, verify what's missing or inconsistent:

```bash
# Check a specific date
npx tsx verify_and_fix_data.ts 2025-03-27 verify

# If you're experiencing API rate limiting, use progressive sampling
npx tsx verify_and_fix_data.ts 2025-03-27 verify progressive
```

This will identify what's wrong before you attempt to fix it.

## Fixing Standard Data Issues

For most data issues, the simplest approach is to use the automatic repair function:

```bash
npx tsx verify_and_fix_data.ts 2025-03-27 fix
```

This script will:
1. Verify the data to identify issues
2. Clear existing data for the date if problems are found
3. Reprocess all 48 periods from the Elexon API
4. Recalculate Bitcoin mining potential
5. Update all summary tables

## DynamoDB Connection Issues

If you're experiencing problems with DynamoDB for Bitcoin difficulty data, you'll need to use a specialized script.

### Using fix_data_for_march_27.ts as a Template

The `fix_data_for_march_27.ts` script is designed to work without relying on DynamoDB by using a fixed difficulty value. You can use this script as a template for other dates:

1. Make a copy of the script and rename it for your date:
   ```bash
   cp fix_data_for_march_27.ts fix_data_for_yyyy_mm_dd.ts
   ```

2. Edit the DATE_TO_PROCESS constant at the top of the file:
   ```typescript
   const DATE_TO_PROCESS = '2025-03-27'; // Change this to your date
   ```

3. Run the script:
   ```bash
   npx tsx fix_data_for_yyyy_mm_dd.ts
   ```

### Customizing the Default Difficulty

The script uses a default difficulty value (71e12) which is appropriate for March 2025. If you need to process a different date, you may want to adjust this value based on historical data.

```typescript
const DEFAULT_DIFFICULTY = 71e12; // Adjust for your date period
```

Typical values by time period:
- Early 2025: ~68e12
- Mid 2025: ~71e12
- Late 2025: ~75e12

## API Rate Limiting Issues

If you're experiencing rate limiting with the Elexon API, you can adjust the batch processing parameters:

```typescript
const BATCH_SIZE = 4; // How many periods to process in parallel
const BATCH_DELAY_MS = 500; // Delay between batches
```

For more aggressive rate limiting, try:
```typescript
const BATCH_SIZE = 2; // Process fewer periods in parallel
const BATCH_DELAY_MS = 1000; // Add more delay between batches
```

## Verifying Your Fix

After running any fix, verify that the data is now consistent:

```bash
npx tsx verify_and_fix_data.ts 2025-03-27 verify
```

You should see that all periods are now properly processed and the data is consistent with the API.

## Multi-Day Repairs

If you need to fix multiple days, you can create a simple script to iterate through dates:

```typescript
import { execSync } from 'child_process';

const startDate = new Date('2025-03-25');
const endDate = new Date('2025-03-28');

for (
  let date = new Date(startDate); 
  date <= endDate; 
  date.setDate(date.getDate() + 1)
) {
  const dateStr = date.toISOString().split('T')[0];
  console.log(`\n=== Processing ${dateStr} ===\n`);
  try {
    execSync(`npx tsx verify_and_fix_data.ts ${dateStr} fix`, { stdio: 'inherit' });
  } catch (error) {
    console.error(`Error processing ${dateStr}:`, error);
  }
}
```

Save this as `fix_multiple_dates.ts` and run it with `npx tsx fix_multiple_dates.ts`.

## Cascade Update for Summaries Only

If you only need to update the summary tables based on existing curtailment and Bitcoin data:

```bash
# For monthly summaries
npx tsx server/services/bitcoinService.ts recalculate-monthly 2025-03

# For yearly summaries
npx tsx server/services/bitcoinService.ts recalculate-yearly 2025
```

## Fixing Specific Issues

### 1. Missing Periods in Curtailment Records

If only certain periods are missing:

```bash
# Identify missing periods
npx tsx verify_and_fix_data.ts 2025-03-27 verify

# Fix all data for the date
npx tsx verify_and_fix_data.ts 2025-03-27 fix
```

### 2. Complete Data Reprocessing

If you want to force a complete reprocessing regardless of current state:

```bash
npx tsx verify_and_fix_data.ts 2025-03-27 force-fix
```

### 3. Bitcoin Calculation Issues with Fixed Difficulty

If you're having problems with DynamoDB for difficulty data:

```bash
# Use the specialized script
npx tsx fix_data_for_march_27.ts
```

#### Relaxed Filtering Option

If you're seeing logs that show records from the API but "No valid curtailment records found" messages, you may need to adjust the filtering criteria. The `fix_data_for_march_27.ts` script includes a configuration option:

```typescript
// Configuration
const STRICT_FILTERING = false; // Set to false to relax filtering criteria
```

- When `STRICT_FILTERING = true`: Records must have negative volume, be SO or CADL flagged, and belong to a valid wind farm in our BMU mapping
- When `STRICT_FILTERING = false`: Only the negative volume requirement is applied

Use relaxed filtering when:
1. You see log entries showing records from the API but "No valid curtailment records found"
2. You're testing or debugging the script
3. The BMU mapping file might be missing some valid wind farm IDs

Note that relaxed filtering will include more records but may also include non-wind curtailment data.

## Troubleshooting

### Script Timeout

If the script times out due to long-running operations:

1. Check if partial data was processed
2. Run the verification again to see what still needs fixing
3. Consider running with fewer period batches and longer delays

### Database Connection Issues

If you encounter database connection issues:

1. Verify the DATABASE_URL environment variable is correct
2. Check if the database is accessible with a simple query
3. Ensure you have the necessary permissions

### Database Constraint Issues

If you encounter errors like `there is no unique or exclusion constraint matching the ON CONFLICT specification`:

1. The `fix_data_for_march_27.ts` script has been updated to handle this by checking for existing records before insertion.
2. This occurs because the `curtailment_records` table lacks a unique constraint on the combination of `settlement_date`, `settlement_period`, and `farm_id`.
3. The script now handles this by:
   - Checking if a record already exists with the same date, period, and farm ID
   - Updating the existing record if found
   - Inserting a new record otherwise

### Inconsistent Totals in Summary Tables

If summaries show incorrect totals after fixes:

1. Force a recalculation of summary tables:
   ```bash
   npx tsx server/services/bitcoinService.ts recalculate-monthly 2025-03
   npx tsx server/services/bitcoinService.ts recalculate-yearly 2025
   ```

## Logging and Monitoring

All fix scripts generate detailed logs. Review these logs to understand what was processed and identify any remaining issues.

For the `verify_and_fix_data.ts` script, logs are stored in the `logs` directory with filenames like `verify_and_fix_YYYY-MM-DD_HHMMSS.log`.

## Emergency Data Recovery

If all else fails and you need to recover data for a critical date:

1. Check if there are backups available
2. Use the `force-fix` option to completely reprocess the date
3. Manually verify the data integrity after fixes
4. Update all dependent summary tables

## Best Practices

- Always verify before fixing
- Use the least invasive fix that will solve the problem
- Check the logs for any errors or warnings
- Verify data integrity after fixes
- Consider running fixes during off-peak hours