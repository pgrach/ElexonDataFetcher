# Reconciliation System User Guide

This comprehensive guide explains how to use the enhanced reconciliation system to ensure data consistency between curtailment records and historical Bitcoin calculations.

## Overview

The reconciliation system ensures that for every curtailment record in our database, we have the corresponding Bitcoin mining potential calculations for each of our three miner models (S19J_PRO, S9, and M20S). This system handles verification, error detection, and automated fixing of any discrepancies.

## Quick Start

For most cases, you'll want to run the daily reconciliation check, which automatically detects and fixes issues for recent dates:

```bash
npx tsx daily_reconciliation_check.ts
```

To check and reconcile a specific date:

```bash
./unified_reconcile.sh date 2025-02-15
```

## Available Tools

### 1. Unified Reconciliation System (`unified_reconcile.sh`)

A comprehensive shell script that provides access to all reconciliation functions:

```bash
# Show current reconciliation status
./unified_reconcile.sh status

# Analyze missing calculations and diagnose issues
./unified_reconcile.sh analyze

# Process all missing calculations with batch size 10
./unified_reconcile.sh reconcile 10

# Process a specific date
./unified_reconcile.sh date 2025-02-28

# Process a date range with batch size 5
./unified_reconcile.sh range 2025-02-01 2025-02-28 5

# Process a problematic date with extra safeguards
./unified_reconcile.sh critical 2025-02-23

# Fix a specific date-period-farm combination
./unified_reconcile.sh spot-fix 2025-02-25 12 FARM-123
```

### 2. Daily Reconciliation Check

For automated daily maintenance of recent data:

```bash
# Check and fix the last 2 days (default)
npx tsx daily_reconciliation_check.ts

# Check and fix the last 5 days
npx tsx daily_reconciliation_check.ts 5

# Force reprocessing of the last 3 days even if they appear complete
npx tsx daily_reconciliation_check.ts 3 true
```

### 3. Testing and Verification

To run a basic test of the reconciliation system:

```bash
./test_reconciliation.sh
```

To verify the reconciliation module is properly loaded:

```bash
npx tsx check_unified_reconciliation.ts
```

## Database Views and Queries

The system provides SQL queries for analyzing reconciliation status. See `reconciliation.sql` for a comprehensive collection of SQL queries.

Common database queries:

```sql
-- Check overall reconciliation status
SELECT * FROM reconciliation_status_view;

-- Find dates with missing calculations
SELECT * FROM missing_calculations_by_date_view 
ORDER BY missing_count DESC 
LIMIT 20;

-- Check specific date status
SELECT * FROM reconciliation_date_status_view 
WHERE date = '2025-02-28';
```

## Troubleshooting Guide

### Common Issues

1. **Timeouts during large batch operations**
   
   Solution: Reduce batch size or use critical mode
   ```bash
   ./unified_reconcile.sh critical 2025-02-23
   ```

2. **Missing calculations for specific periods**
   
   Solution: Use spot-fix for targeted fixing
   ```bash
   ./unified_reconcile.sh spot-fix 2025-02-25 12 FARM-123
   ```

3. **Reconciliation failures due to difficulty data**
   
   Solution: Verify difficulty data is available in DynamoDB
   ```bash
   npx tsx server/scripts/test-dynamo.ts
   ```

### Monitoring and Logs

Reconciliation logs are stored in:
- `reconciliation.log` - Main log file for reconciliation operations
- `logs/daily_reconciliation_*.log` - Logs from daily reconciliation checks

To monitor progress:

```bash
# Watch reconciliation log in real-time
tail -f reconciliation.log
```

## Performance Optimization

For optimal performance:

1. **Batch Size**: Start with small batch sizes (5-10) and increase as needed
2. **Timeout Handling**: For frequent timeouts, use critical mode for processing
3. **Database Load**: Schedule large reconciliation jobs during off-peak hours
4. **Checkpoints**: The system creates checkpoints that allow resuming interrupted operations

## Schedule and Automation

The reconciliation system is integrated with the platform's data updater service, which runs:
- Daily checks automatically each morning
- Monthly comprehensive checks on the 1st of each month
- Automated verification after real-time data updates

## Maintenance

Regular maintenance includes:

1. **Weekly Check**: Run `./unified_reconcile.sh analyze` weekly to identify any issues
2. **Monthly Verification**: Verify full month reconciliation at the beginning of each month
3. **Update Progress Tracker**: Keep `RECONCILIATION_PROGRESS.md` updated with latest status

## Further Resources

- `RECONCILIATION_PROGRESS.md` - Detailed tracking of reconciliation progress
- `RECONCILIATION_ENHANCEMENTS.md` - Planned enhancements to the reconciliation system
- `reconciliation_tools.md` - Comprehensive documentation of all reconciliation tools