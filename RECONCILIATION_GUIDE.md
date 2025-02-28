# Bitcoin Mining Reconciliation System Guide

## Overview

This guide explains how to use the Unified Reconciliation System to ensure data integrity between curtailment records and Bitcoin calculations. The system is designed to handle large datasets efficiently, provide robust error recovery, and offer comprehensive reporting on reconciliation progress.

## When to Use Reconciliation

Reconciliation should be performed in the following scenarios:

1. When curtailment data is updated but Bitcoin calculations might be missing
2. When historical Bitcoin difficulty data is updated
3. After system maintenance or database migrations
4. On a regular schedule (daily/weekly) to ensure ongoing data integrity
5. When investigating discrepancies in Bitcoin mining analytics

## Available Tools

### 1. Unified Reconciliation System

The primary tool for managing reconciliation is the `unified_reconciliation.ts` script, which can be run directly or through the `unified_reconcile.sh` wrapper.

```bash
# Using the TypeScript script directly
npx tsx unified_reconciliation.ts [command] [options]

# Using the shell wrapper (recommended)
./unified_reconcile.sh [command] [options]
```

### 2. Legacy Tools (for reference only)

The following tools are maintained for backward compatibility but should be phased out in favor of the unified system:

- `minimal_reconciliation.ts` - For problematic dates with timeout issues
- `efficient_reconciliation.ts` - Original batch-based reconciliation system
- `check_reconciliation_status.ts` - Simple status checker
- `daily_reconciliation_check.ts` - Automated daily check system

## Commands and Options

### Status Check

Get an overview of the current reconciliation status:

```bash
./unified_reconcile.sh status
```

This shows:
- Total curtailment records vs. Bitcoin calculations
- Complete, partial, and missing dates
- Overall completion rate

### Analysis

Perform a detailed analysis of the reconciliation status with recommendations:

```bash
./unified_reconcile.sh analyze
```

This provides:
- Database connection health check
- Long-running query detection
- Critical dates that need attention
- Recommended actions based on the analysis

### Full Reconciliation

Process all missing calculations with specified batch size:

```bash
./unified_reconcile.sh reconcile [batchSize]
```

The optional `batchSize` parameter controls how many records are processed concurrently (default: 10).

### Process Specific Date

Fix or verify a specific date:

```bash
./unified_reconcile.sh date YYYY-MM-DD
```

### Process Date Range

Fix or verify a range of dates:

```bash
./unified_reconcile.sh range YYYY-MM-DD YYYY-MM-DD [batchSize]
```

### Handle Critical Dates

For dates with persistent timeout issues, use critical mode with extra safeguards:

```bash
./unified_reconcile.sh critical YYYY-MM-DD
```

This processes the date one record at a time with extended timeout protection and careful connection management.

### Spot Fix

For targeted fixes of specific problematic records:

```bash
./unified_reconcile.sh spot-fix YYYY-MM-DD PERIOD FARM_ID
```

## Understanding the Logs

The reconciliation system creates detailed logs in the following files:

- `reconciliation.log` - Primary log file with all operations
- `reconciliation_checkpoint.json` - Progress tracking for resumability
- `reconciliation_dashboard.log` - Summary statistics for reporting

## Troubleshooting

### Connection Timeouts

If you experience database connection timeouts:

1. Reduce batch size to decrease database load
2. Use the `critical` command for problematic dates
3. Check for long-running queries with `analyze`
4. Consider using `spot-fix` for specific problematic records

### Data Discrepancies

If Bitcoin calculations don't match expected values:

1. Verify the difficulty data is correct
2. Check the curtailment record volume values
3. Ensure all miner models are being processed
4. Use `analyze` to identify patterns in missing data

### Performance Optimization

For best performance when processing large datasets:

1. Start with a smaller batch size and gradually increase
2. Process date ranges instead of all dates at once
3. Run reconciliation during off-peak hours
4. Use the checkpoint system to resume interrupted operations

## Scheduling Reconciliation

For ongoing data integrity, set up scheduled reconciliation:

```bash
# Add to crontab for daily check at 3 AM
0 3 * * * cd /path/to/project && ./unified_reconcile.sh reconcile > /path/to/logs/daily_reconcile.log 2>&1
```

## Best Practices

1. Always run `analyze` before starting reconciliation to understand the scope
2. Start with critical dates that have the largest discrepancies
3. Use appropriate batch sizes based on database capacity
4. Monitor logs for patterns in failures or timeouts
5. Keep the reconciliation system updated with the latest features

## Advanced Features

### Checkpoint-Based Recovery

The system automatically saves progress to `reconciliation_checkpoint.json`, allowing interrupted operations to be resumed later.

### Adaptive Batch Sizing

The system can automatically adjust batch sizes based on database performance to prevent timeouts.

### Exponential Backoff

When operations fail, the system uses an exponential backoff strategy for retries to prevent overwhelming the database.

## Command Reference

| Command | Description | Example |
|---------|-------------|---------|
| status | Show current reconciliation status | `./unified_reconcile.sh status` |
| analyze | Analyze missing calculations | `./unified_reconcile.sh analyze` |
| reconcile | Process all missing calculations | `./unified_reconcile.sh reconcile 5` |
| date | Process a specific date | `./unified_reconcile.sh date 2025-02-28` |
| range | Process a date range | `./unified_reconcile.sh range 2025-02-01 2025-02-28` |
| critical | Process problematic date | `./unified_reconcile.sh critical 2025-02-15` |
| spot-fix | Fix specific record | `./unified_reconcile.sh spot-fix 2025-02-28 30 T_VKNGW-1` |

## Contact

For issues or enhancement requests, please contact the development team.