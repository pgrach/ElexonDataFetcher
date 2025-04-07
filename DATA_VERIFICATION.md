# Data Verification and Repair Utility

This documentation provides a comprehensive guide to verifying and repairing data integrity issues in the Bitcoin mining potential system.

## Overview

The Data Verification and Repair Utility (`verify_and_fix_data.ts`) is designed to check data consistency between the Elexon API and our database, and automatically repair any discrepancies that are found. It provides a robust way to ensure data quality and fix issues that may arise from API timeouts, rate limiting, or other technical problems.

## Key Features

- **Data Verification**: Compares database records with the latest Elexon API data to identify discrepancies
- **Flexible Sampling**: Multiple sampling strategies to efficiently check data integrity while minimizing API calls
- **Automatic Repair**: Option to automatically reprocess data when issues are found
- **Comprehensive Reporting**: Detailed logs and summaries of verification results and repair actions
- **Full Data Pipeline Processing**: Handles the entire data flow from curtailment records to Bitcoin calculations and summary updates

## Usage

### Basic Commands

```bash
# Verify today's data using progressive sampling
npx tsx verify_and_fix_data.ts

# Verify a specific date using progressive sampling
npx tsx verify_and_fix_data.ts 2025-04-01

# Only verify without fixing
npx tsx verify_and_fix_data.ts 2025-04-01 verify

# Verify and automatically fix if needed
npx tsx verify_and_fix_data.ts 2025-04-01 fix

# Use random sampling instead of progressive
npx tsx verify_and_fix_data.ts 2025-04-01 fix random

# Skip verification and force a complete reprocessing
npx tsx verify_and_fix_data.ts 2025-04-01 force-fix
```

### Available Actions

- **verify** (default): Only performs verification without fixing
- **fix**: Verifies and automatically fixes if issues are found
- **force-fix**: Skips verification and forces a complete reprocessing of the date

### Sampling Methods

- **progressive** (default): Starts with key periods (1, 12, 24, 36, 48), expands if issues found
- **random**: Checks 10 random periods for broader coverage
- **fixed**: Only checks 5 fixed key periods (1, 12, 24, 36, 48)
- **full**: Checks all 48 periods (warning: may hit API rate limits)

## Verification Process

The verification process follows these steps:

1. **Database Check**: Retrieves the current state of data for the specified date
2. **Initial Sampling**: Checks a subset of settlement periods based on the sampling method
3. **Expanded Sampling** (if progressive): If issues are found, checks additional random periods
4. **Analysis**: Calculates overall mismatch percentage and identifies specific problem periods
5. **Reporting**: Provides a detailed summary of the verification results

## Repair Process

When the `fix` or `force-fix` action is specified and issues are detected, the repair process follows these steps:

1. **Curtailment Reprocessing**: Clears existing curtailment records and fetches fresh data from the Elexon API
2. **Bitcoin Calculation**: Recalculates Bitcoin mining potential based on the updated curtailment data
3. **Summary Updates**: Updates the daily, monthly, and yearly summary tables
4. **Verification**: Confirms the repairs were successful by comparing initial and final data states

## Log Files

Each verification and repair operation generates a detailed log file in the `logs` directory with the format:

```
logs/verify_and_fix_YYYY-MM-DD_HHMMSS.log
```

These logs contain:
- Initial database state
- Verification results
- Repair actions taken (if any)
- Final database state after repair
- Detailed statistics about changes made

## Common Scenarios

### 1. Routine Data Verification

It's good practice to regularly verify recent data to ensure data integrity:

```bash
# Verify yesterday's data
npx tsx verify_and_fix_data.ts $(date -d "yesterday" +%Y-%m-%d)
```

### 2. Fixing Missing Data

If you discover a date with missing or incomplete data:

```bash
# Verify and fix if needed
npx tsx verify_and_fix_data.ts 2025-03-15 fix
```

### 3. Force Reprocessing Specific Dates

For dates known to have problems, you can force a complete reprocessing:

```bash
# Force reprocessing without verification
npx tsx verify_and_fix_data.ts 2025-03-20 force-fix
```

### 4. Batch Processing Multiple Dates

To process multiple consecutive dates, you can use a simple bash loop:

```bash
# Process a range of dates
for i in {1..5}; do
  date=$(date -d "2025-03-$i" +%Y-%m-%d)
  echo "Processing $date"
  npx tsx verify_and_fix_data.ts $date fix
done
```

## Troubleshooting

### API Rate Limiting

If you encounter API rate limiting issues:
- Use the `fixed` sampling method to reduce API calls during verification
- Add delays between processing multiple dates
- Schedule verification during off-peak hours

### Execution Timeouts

For dates with large amounts of data, script execution might timeout:
- Use the `force-fix` action to skip verification and directly repair
- Process one date at a time
- Increase the timeout limit if possible

### Data Still Inconsistent After Repair

If data remains inconsistent after repair:
- Check the Elexon API status and availability
- Verify the BMU mapping file is up-to-date
- Ensure the DynamoDB difficulty data is accessible
- Try processing with a different miner model to identify potential issues

## Integration with Scheduled Tasks

For automated verification, you can add this utility to your scheduled tasks:

```bash
# Example crontab entry to verify yesterday's data every morning
0 6 * * * cd /path/to/project && npx tsx verify_and_fix_data.ts $(date -d "yesterday" +%Y-%m-%d) fix > /path/to/logs/daily_verification.log 2>&1
```

## Best Practices

1. **Regular Verification**: Verify recent data daily to catch issues early
2. **Incremental Approach**: Start with verification before applying fixes
3. **Log Review**: Regularly review log files for patterns and recurring issues
4. **Batch Carefully**: When processing multiple dates, add delays to avoid API rate limits
5. **Monitor Resources**: Be mindful of database and API resource usage during heavy verification

## Related Tools

This utility works in conjunction with several other tools in the system:

- `check_elexon_data.ts`: Lightweight verification tool focused only on API data comparison
- `process_all_periods.ts`: Processes curtailment records for all 48 settlement periods
- `process_bitcoin_optimized.ts`: Calculates Bitcoin mining potential with optimized DynamoDB access
- `fix_incomplete_data_optimized.ts`: Combined workflow for fixing a specific date's data