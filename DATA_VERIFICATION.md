# Data Verification and Repair

This document provides comprehensive information about verifying and fixing data integrity issues within the Bitcoin Mining Potential system.

## The Data Pipeline

The system's data flows through a pipeline with multiple stages:

1. **Raw Data Collection**: Fetching curtailment data from the Elexon API
2. **Curtailment Records**: Storing filtered curtailment data in the database
3. **Bitcoin Calculations**: Calculating mining potential for different miner models
4. **Summary Aggregation**: Aggregating data at daily, monthly, and yearly levels

Any issues in one stage can propagate to subsequent stages, making verification essential.

## Verification Tools

The system provides multiple verification tools with different capabilities:

### 1. `verify_and_fix_data.ts` (Primary Tool)

This is the recommended tool for both verification and repair:

```bash
npx tsx verify_and_fix_data.ts [date] [action] [sampling-method]
```

#### Available Actions
- **verify** (default): Only performs verification without fixing
- **fix**: Verifies and automatically fixes if issues are found
- **force-fix**: Skips verification and forces a complete reprocessing

#### Sampling Methods
- **progressive** (default): Starts with key periods, expands if issues found
- **random**: Checks random periods for broader coverage
- **fixed**: Only checks key periods (1, 12, 24, 36, 48)
- **full**: Attempts to check all 48 periods (warning: may hit API limits)

#### Output
- Detailed logs in the `logs` directory (verify_and_fix_YYYY-MM-DD_HHMMSS.log)
- Summary of initial data state, verification results, and repair outcomes

### 2. `check_elexon_data.ts` (Legacy Tool)

A simpler verification tool that only checks data against Elexon API:

```bash
npx tsx check_elexon_data.ts [date] [sampling-method]
```

This tool:
- Does not automatically fix issues
- Provides commands to run for fixing detected problems
- Uses the same sampling methods as verify_and_fix_data.ts

### 3. Other Verification Scripts

- **verify_dates.ts**: Basic verification of completeness across tables
- **verify_service.ts**: Verify data integrity between tables

## Verification Process

The verification process involves several steps:

### 1. Database Summary Check

First, the system checks the current state of the database:
- Number of curtailment records for the date
- Number of periods covered (out of 48)
- Total volume and payment amounts

### 2. API Data Comparison

For each period being checked:
- Fetch curtailment data from Elexon API
- Filter for valid wind farm records
- Compare counts, volumes, and payments with database records

### 3. Analysis of Discrepancies

The system analyzes any discrepancies found:
- **Missing Periods**: Periods that exist in API but not in database
- **Data Mismatches**: Differences in counts, volumes, or payments
- **Completeness Check**: Ensuring all 48 periods are covered

### 4. Repair Process (if needed)

If issues are detected and repair is requested:

1. **Clear Existing Data**: Remove curtailment records for the date
2. **Reprocess All Periods**: Fetch and process all 48 periods from API
3. **Recalculate Bitcoin**: Update Bitcoin calculations for all miner models
4. **Update Summaries**: Refresh monthly and yearly summaries

### 5. Verification After Repair

After repair, the system performs a final verification:
- Compares initial and final database states
- Reports on changes made and remaining issues (if any)

## Common Data Integrity Issues

The system is designed to identify and fix several common issues:

### 1. Missing Periods

The most common issue is missing settlement periods. This can happen due to:
- API timeouts during initial processing
- Process interruption
- Database errors during insertion

The verification tools check if all 48 periods are present and identify specific missing ones.

### 2. Data Mismatches

Sometimes stored data doesn't match what's available from the API:
- Incorrect filtering of API data
- Partial processing of periods
- Data format conversion issues

The tools compare record counts, energy volumes, and payment amounts to identify these issues.

### 3. Summary Inconsistencies

Summary tables may become inconsistent with base tables due to:
- Failed cascade updates
- Partial processing
- Manual edits

The verification tools ensure that summary tables accurately reflect the primary data.

### 4. External Service Issues

Problems with external services can impact verification:
- DynamoDB connection issues for difficulty data
- Elexon API rate limiting
- API format changes

The system provides alternative paths (like fix_data_for_march_27.ts) to handle these scenarios.

## Best Practices for Data Verification

### Regular Verification Schedule

Implement a regular verification schedule:
- Daily verification of yesterday's data
- Weekly random checks of historical data
- Monthly integrity check of summary tables

### Recommended Verification Commands

For routine verification:
```bash
# Quick verification of today's data
npx tsx verify_and_fix_data.ts

# Verify specific date with progressive sampling
npx tsx verify_and_fix_data.ts 2025-03-25 verify progressive

# Verify and fix if needed
npx tsx verify_and_fix_data.ts 2025-03-25 fix
```

For thorough verification (when API rate limits are not a concern):
```bash
# Full verification of all 48 periods
npx tsx verify_and_fix_data.ts 2025-03-25 verify full
```

### Interpreting Verification Results

The verification tools provide detailed statistics:
- **isPassing**: Overall pass/fail status
- **totalChecked**: Number of periods verified
- **totalMismatch**: Number of periods with discrepancies
- **mismatchedPeriods**: List of specific periods with issues
- **missingPeriods**: List of periods missing from database

A detailed breakdown of each period is also provided, showing:
- Database counts vs. API counts
- Database volumes vs. API volumes
- Database payments vs. API payments

### Creating Custom Verification Scripts

You can create custom verification scripts for specific needs:

1. Use `verify_and_fix_data.ts` as a template
2. Modify the verification criteria as needed
3. Add custom reporting or integration with monitoring systems

## Troubleshooting Verification Issues

### Elexon API Rate Limiting

If you encounter rate limiting issues:
1. Use progressive or random sampling instead of full verification
2. Increase delays between API calls in the script
3. Run verification during off-peak hours

### DynamoDB Connection Issues

If DynamoDB connectivity is a problem:
1. Use `fix_data_for_march_27.ts` as a template for your date
2. The script uses a fixed difficulty value instead of DynamoDB
3. See `FIXING_INCOMPLETE_DATA.md` for detailed instructions

### Large Data Volume Issues

For dates with very large data volumes:
1. Use sampling-based verification instead of full verification
2. Increase batch sizes and timeouts in the scripts
3. Consider using specialized scripts like `fix_incomplete_data_optimized.ts`

## Conclusion

Regular data verification is essential for maintaining the integrity and reliability of the Bitcoin Mining Potential system. By implementing a consistent verification process using the tools provided, you can ensure that your data remains accurate and complete.

Remember that verification is not just about finding errors but also about understanding the data flow and ensuring that all components of the system are working as expected.

For specific repair instructions, refer to `FIXING_INCOMPLETE_DATA.md`.