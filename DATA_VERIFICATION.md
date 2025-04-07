# Data Verification and Repair Tools

This document provides a comprehensive overview of the data verification tools available in the Bitcoin Mining Analytics platform and how to use them effectively.

## Understanding The Data Pipeline

The data pipeline consists of several interconnected components:

1. **Curtailment Data Collection**: Data from the Elexon API about wind farm curtailment
2. **Bitcoin Calculation**: Processing of curtailment data to determine Bitcoin mining potential
3. **Summary Tables**: Aggregation of data at daily, monthly, and yearly levels

When issues arise in this pipeline, they can propagate through the system, causing inconsistencies in the data presented to users. The verification tools are designed to detect these issues and repair them effectively.

## Available Verification Tools

### 1. Comprehensive Data Verification and Repair Utility (`verify_and_fix_data.ts`)

This is the primary tool for verifying and repairing data integrity issues. It offers a complete solution that can detect discrepancies between the database and Elexon API data, then automatically reprocess the data if needed.

#### Basic Usage

```bash
npx tsx verify_and_fix_data.ts [date] [action] [sampling-method]
```

**Examples:**
```bash
# Verify today's data using progressive sampling
npx tsx verify_and_fix_data.ts

# Verify a specific date
npx tsx verify_and_fix_data.ts 2025-04-01

# Verify and automatically fix if needed
npx tsx verify_and_fix_data.ts 2025-04-01 fix

# Skip verification and force reprocessing
npx tsx verify_and_fix_data.ts 2025-04-01 force-fix

# Use random sampling for verification
npx tsx verify_and_fix_data.ts 2025-04-01 fix random
```

#### Available Actions

- **verify** (default): Only performs verification without fixing
- **fix**: Verifies and automatically repairs if issues are found
- **force-fix**: Skips verification and forces a complete reprocessing of the date

#### Sampling Methods

To balance thoroughness with API efficiency (avoiding rate limits), the tool offers several sampling strategies:

1. **progressive** (default): Starts with 5 key periods (1, 12, 24, 36, 48), then adds up to 10 more random periods if issues are found
2. **random**: Checks 10 randomly selected periods across the day
3. **fixed**: Only checks 5 critical periods (1, 12, 24, 36, 48)
4. **full**: Attempts to check all 48 periods (warning: may hit API rate limits)

#### Log Files

Each verification and repair operation generates a detailed log file in the `logs` directory:
```
logs/verify_and_fix_YYYY-MM-DD_HHMMSS.log
```

These logs contain complete information about the verification process, including:
- Initial database state
- Verification results for each checked period
- Repair actions taken (if any)
- Final database state after repair
- Detailed statistics about changes made

### 2. Elexon Data Checker (`check_elexon_data.ts`)

This is a lightweight verification tool that only checks data against the Elexon API without performing repairs.

```bash
npx tsx check_elexon_data.ts [date] [sampling-method]
```

**Examples:**
```bash
# Check today's data
npx tsx check_elexon_data.ts

# Check specific date with random sampling
npx tsx check_elexon_data.ts 2025-03-28 random
```

If issues are detected, the tool will provide instructions for manual repair.

## Repair Process

The repair process follows a defined sequence to ensure complete data integrity:

1. **Clear Existing Data**: Remove existing curtailment records for the date
2. **Fetch Fresh Data**: Collect all 48 settlement periods from the Elexon API
3. **Calculate Bitcoin Potential**: Process Bitcoin calculations for all three miner models
4. **Update Summary Tables**: Recalculate all summary tables (daily, monthly, yearly)
5. **Verify Repair**: Confirm the repair was successful by comparing before/after states

## Common Data Issues

### Missing Periods

Missing periods occur when data for certain settlement periods is not present in the database but exists in the Elexon API. This can happen due to:
- API timeouts during initial data collection
- Process interruptions during batch processing
- Database connection issues

### Data Mismatches

Mismatches occur when the data in the database differs from what's currently in the Elexon API. This can happen due to:
- Data corrections in the Elexon API after initial collection
- Partial processing of API data
- Rounding differences in calculations

### Cascade Update Failures

Sometimes the summary tables (daily, monthly, yearly) may not be properly updated after new primary data is collected. Symptoms include:
- Inconsistencies between curtailment_records and daily_summaries
- Incorrect monthly totals that don't match the sum of daily records
- Yearly summaries that don't reflect all available data

## Best Practices

### Regular Verification

Implement a regular verification schedule to catch issues early:
- Daily verification of the previous day's data
- Weekly random sampling of data from the past month
- Monthly verification of summary tables

### Handling API Rate Limits

The Elexon API has rate limits that can affect verification and repair:
- Use progressive sampling for routine checks to minimize API calls
- Schedule full verifications during off-peak hours
- Add appropriate delays between API calls (built into the tools)

### Logging and Monitoring

Maintain good visibility into the data pipeline:
- Review log files generated by verification tools
- Monitor the number of records, periods, and volumes during verification
- Track repair operations and their impact

## Troubleshooting

### Verification Tool Errors

If the verification tools encounter errors:
1. Check database connectivity
2. Verify Elexon API access
3. Ensure proper environment setup
4. Check for sufficient disk space for logs

### Database Inconsistencies

If summaries remain inconsistent after repair:
1. Run the full cascade update: `npx tsx process_complete_cascade.ts <date>`
2. Verify monthly summaries: `npx tsx server/services/bitcoinService.ts recalculate-monthly <year-month>`
3. Verify yearly summaries: `npx tsx server/services/bitcoinService.ts recalculate-yearly <year>`

### Persistent Issues

For persistent data issues:
1. Try the force-fix option: `npx tsx verify_and_fix_data.ts <date> force-fix`
2. Check for schema changes or database migrations
3. Verify the BMU mapping file is up-to-date
4. Consider reprocessing adjacent dates if the issue spans multiple days

## Conclusion

The data verification tools provide a robust framework for ensuring data integrity in the Bitcoin Mining Analytics platform. By following the recommended practices and using these tools effectively, you can maintain accurate and reliable data throughout the system.