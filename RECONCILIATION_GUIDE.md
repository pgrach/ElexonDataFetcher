# Unified Reconciliation System Guide

This guide explains how to use the unified reconciliation system to ensure data consistency between `curtailment_records` and `historical_bitcoin_calculations` tables.

## Overview

The unified reconciliation system provides a comprehensive solution for detecting and fixing mismatches between the `curtailment_records` and `historical_bitcoin_calculations` tables. It combines efficient batch processing, intelligent retry logic, and comprehensive logging to ensure 100% data reconciliation.

## Prerequisites

- Node.js 16.x or higher
- Access to the PostgreSQL database
- Required environment variables (especially DATABASE_URL)

## Quick Start

The simplest way to use the system is through the shell wrapper:

```bash
# Check current reconciliation status
./unified_reconcile.sh status

# Analyze any reconciliation issues
./unified_reconcile.sh analyze

# Run reconciliation with default batch size
./unified_reconcile.sh reconcile

# Process a specific date
./unified_reconcile.sh date 2025-02-15

# Process a date range
./unified_reconcile.sh range 2025-02-01 2025-02-15
```

## Command Reference

### Status Check

```bash
./unified_reconcile.sh status
```

Shows the current reconciliation status, including:
- Overall completion percentage
- Total records in each table
- Missing calculations count
- Recent problematic dates

### Analysis

```bash
./unified_reconcile.sh analyze
```

Provides a detailed analysis of reconciliation issues:
- Patterns in missing calculations
- Problematic dates and time periods
- Recommendations for targeted fixes

### Full Reconciliation

```bash
./unified_reconcile.sh reconcile [batchSize]
```

Processes all missing calculations with optional batch size parameter (default: 10):
- Fetches dates with missing calculations
- Processes them in batches with configurable concurrency
- Implements exponential backoff for connection failures
- Maintains checkpoints for resumability

### Date-Specific Processing

```bash
./unified_reconcile.sh date YYYY-MM-DD
```

Processes a specific date, with:
- Comprehensive error handling
- Multiple retry attempts
- Detailed logging

### Date Range Processing

```bash
./unified_reconcile.sh range YYYY-MM-DD YYYY-MM-DD [batchSize]
```

Processes a range of dates with optional batch size parameter:
- Automatically breaks processing into manageable batches
- Reports progress throughout execution

### Critical Date Handling

```bash
./unified_reconcile.sh critical YYYY-MM-DD
```

Processes a problematic date with extra safeguards:
- Uses minimal batch size
- Implements extended timeout parameters
- Adds additional verification steps

### Spot Fixes

```bash
./unified_reconcile.sh spot-fix DATE PERIOD FARM
```

Fixes a specific date-period-farm combination:
- Useful for targeted repairs of known issues
- Provides detailed diagnostic information

## Advanced Configuration

The system supports several configuration options that can be adjusted in the `unified_reconciliation.ts` file:

- `DEFAULT_BATCH_SIZE`: Default number of dates to process in a batch
- `MAX_CONCURRENCY`: Maximum number of concurrent date processing jobs
- `MAX_RETRIES`: Maximum number of retry attempts for failed operations
- `CHECKPOINT_INTERVAL`: How often to save processing checkpoints (ms)

## Troubleshooting

If you encounter issues with the reconciliation process:

1. Check the log file at `reconciliation.log` for detailed error messages
2. Try processing a single problematic date with the critical mode
3. Verify database connectivity and timeout settings
4. Check for locked database resources that might be causing conflicts

## Integration with Other Systems

The unified reconciliation system integrates with other parts of the platform:

- **Daily Checks**: Automated via `daily_reconciliation_check.ts`
- **Data Updater Service**: Uses reconciliation utilities for real-time updates
- **Dashboard**: The `reconciliation_dashboard.ts` script provides visualization

## Development Guidelines

When extending or modifying the reconciliation system:

1. Maintain backward compatibility with existing scripts
2. Preserve the checkpoint system for resumable operations
3. Add comprehensive logging for any new functionality
4. Test thoroughly with small batches before full-scale execution
5. Document any new commands or options

## License

This software is proprietary and confidential.