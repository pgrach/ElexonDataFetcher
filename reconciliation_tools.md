# Reconciliation Tools Guide

This guide provides an overview of the available tools for maintaining data consistency between curtailment records and Bitcoin calculations within our platform.

## Available Tools

### 1. Unified Reconciliation System

**Command:** `./unified_reconcile.sh [command] [options]`

A comprehensive solution that combines efficient batch processing, intelligent retry logic, and detailed reporting in a single unified tool. This is the recommended tool for most reconciliation tasks.

#### Key Commands:

- `status` - Show current reconciliation status
- `analyze` - Analyze missing calculations and diagnose issues
- `reconcile [batchSize]` - Fix all missing calculations with specified batch size
- `date YYYY-MM-DD` - Process a specific date
- `range YYYY-MM-DD YYYY-MM-DD [batchSize]` - Process a date range
- `critical YYYY-MM-DD` - Process a problematic date with extra safeguards

**Example:**
```bash
# Check overall status
./unified_reconcile.sh status

# Fix a specific date with issues
./unified_reconcile.sh date 2025-02-15
```

### 2. Daily Reconciliation Check

**Command:** `npx tsx daily_reconciliation_check.ts [days=2] [forceProcess=false]`

Automatically checks and fixes issues in recent days' data. Useful for routine maintenance.

**Parameters:**
- `days` - Number of recent days to check (default: 2)
- `forceProcess` - Whether to process all dates even if they appear complete (default: false)

**Example:**
```bash
# Check and fix the last 5 days
npx tsx daily_reconciliation_check.ts 5

# Force reprocessing of the last 3 days
npx tsx daily_reconciliation_check.ts 3 true
```

### 3. Verification Tool

**Command:** `npx tsx check_unified_reconciliation.ts`

A simple tool to verify that the unified reconciliation system is correctly set up and functioning.

**Example:**
```bash
npx tsx check_unified_reconciliation.ts
```

## Integration with Data Updater Service

The reconciliation tools are integrated with the platform's data updater service, which automatically invokes reconciliation during real-time data updates.

Key integration points:
- `updateLatestData()` function in `dataUpdater.ts` calls reconciliation functions
- Automatic reconciliation runs on schedule via the data updater service
- On-demand reconciliation can be triggered through the tools above

## Troubleshooting

### Common Issues

1. **Timeouts**
   - Symptom: Operations time out during large data processing
   - Solution: Use smaller batch sizes or try the `critical` mode for problematic dates

2. **Missing Calculations**
   - Symptom: Some dates show incomplete reconciliation
   - Solution: Run the specific date with `unified_reconcile.sh date YYYY-MM-DD`

3. **Data Discrepancies**
   - Symptom: Calculations exist but don't match expected values
   - Solution: Use `unified_reconcile.sh analyze` to identify patterns in the discrepancies

### Checking Logs

Reconciliation logs are stored in:
- `reconciliation.log` - Main reconciliation log
- `logs/daily_reconciliation_*.log` - Daily check logs

## Best Practices

1. **Regular Verification**
   - Run daily checks consistently to catch issues early
   - Periodically verify the status of historical data

2. **Batch Processing**
   - For large historical data repairs, use date ranges with manageable batch sizes
   - Start with small batches (5-10) and increase if successful

3. **Critical Dates**
   - For dates that consistently fail, use the `critical` command
   - If specific date-period-farm combinations are problematic, use `spot-fix`

4. **Performance Optimization**
   - Run resource-intensive reconciliation during off-peak hours
   - Monitor database load during large batch operations