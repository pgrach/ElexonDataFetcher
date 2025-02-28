# Reconciliation Tools Reference

This document provides a comprehensive reference for all tools available in the Bitcoin calculation reconciliation system.

## CLI Tools

| Tool | Description | Example Usage |
|------|-------------|---------------|
| `unified_reconcile.sh` | Main script for all reconciliation operations | `./unified_reconcile.sh date 2025-02-28` |
| `test_reconciliation.sh` | Test script for verifying reconciliation system | `./test_reconciliation.sh` |
| `daily_reconciliation_check.ts` | Automated daily checks for recent dates | `npx tsx daily_reconciliation_check.ts 3` |
| `run_reconciliation_test.ts` | Sample reconciliation tests | `npx tsx run_reconciliation_test.ts` |
| `check_unified_reconciliation.ts` | Verify the unified reconciliation module | `npx tsx check_unified_reconciliation.ts` |
| `reconciliation_dashboard.ts` | Generate a text-based dashboard | `npx tsx reconciliation_dashboard.ts` |
| `reconciliation_manager.ts` | High-level management tool | `npx tsx reconciliation_manager.ts status` |

## Core Modules

| Module | Description | Key Functions |
|--------|-------------|---------------|
| `unified_reconciliation.ts` | Core reconciliation engine | `getReconciliationStatus`, `processDate`, `processDates` |
| `daily_reconciliation_check.ts` | Daily automation engine | `runDailyCheck`, `checkDateReconciliationStatus` |
| `server/services/historicalReconciliation.ts` | Integration with application services | `reconcileDay`, `reconcileRecentData`, `auditAndFixBitcoinCalculations` |
| `server/services/dataUpdater.ts` | Data update service | `updateLatestData`, `getUpdateServiceStatus` |

## SQL Tools

| Query Type | Description | File Location |
|------------|-------------|---------------|
| Status Views | Database views for reconciliation status | `reconciliation.sql` |
| Analysis Queries | Queries for finding missing calculations | `reconciliation.sql` |
| Performance Indexes | Indexes for optimizing queries | `reconciliation.sql` |
| Troubleshooting | Queries for diagnosing issues | `reconciliation.sql` |

## Documentation

| Document | Description | Audience |
|----------|-------------|----------|
| `RECONCILIATION_GUIDE.md` | User guide for the reconciliation system | All users |
| `RECONCILIATION_PROGRESS.md` | Progress tracking document | Operations team |
| `RECONCILIATION_ENHANCEMENTS.md` | Planned enhancements roadmap | Development team |
| `reconciliation_tools.md` | Reference for all available tools | Technical users |

## Detailed Tool Reference

### unified_reconcile.sh

A comprehensive shell script providing easy access to all reconciliation functions.

**Available Commands:**

```bash
# Show current status
./unified_reconcile.sh status

# Analyze issues
./unified_reconcile.sh analyze

# Process missing calculations
./unified_reconcile.sh reconcile [batchSize]

# Process a specific date
./unified_reconcile.sh date YYYY-MM-DD

# Process a date range
./unified_reconcile.sh range START_DATE END_DATE [batchSize]

# Process a problematic date
./unified_reconcile.sh critical DATE

# Fix a specific record
./unified_reconcile.sh spot-fix DATE PERIOD FARM_ID

# Show help
./unified_reconcile.sh help
```

### daily_reconciliation_check.ts

Automated tool for checking and fixing recent dates.

**Parameters:**

- `days`: Number of recent days to check (default: 2)
- `forceProcess`: Boolean flag to force processing (default: false)

**Example Usage:**

```bash
# Check last 2 days
npx tsx daily_reconciliation_check.ts

# Check last 5 days
npx tsx daily_reconciliation_check.ts 5

# Force processing of last 3 days
npx tsx daily_reconciliation_check.ts 3 true
```

### reconciliation_dashboard.ts

Generates a comprehensive text-based dashboard of reconciliation status.

**Example Usage:**

```bash
npx tsx reconciliation_dashboard.ts
```

**Output Sections:**

- Overall reconciliation status
- Status by miner model
- Status by month
- Top problematic dates
- Recent dates status
- Database statistics

### reconciliation_manager.ts

High-level management tool for reconciliation operations.

**Available Commands:**

```bash
# Show status
npx tsx reconciliation_manager.ts status

# Analyze issues
npx tsx reconciliation_manager.ts analyze

# Fix missing calculations
npx tsx reconciliation_manager.ts fix [batchSize]

# Run diagnostics
npx tsx reconciliation_manager.ts diagnose

# Schedule regular checks
npx tsx reconciliation_manager.ts schedule

# Fix a specific date
npx tsx reconciliation_manager.ts fix-date YYYY-MM-DD

# Fix a date range
npx tsx reconciliation_manager.ts fix-range START END [batchSize]
```

## Database Views

The system creates several useful database views for monitoring reconciliation status:

1. **reconciliation_status_view**: Overall reconciliation percentage
2. **reconciliation_date_status_view**: Reconciliation status by date
3. **missing_calculations_by_date_view**: Missing calculations by date

**Example Queries:**

```sql
-- Check overall status
SELECT * FROM reconciliation_status_view;

-- Find problematic dates
SELECT * FROM missing_calculations_by_date_view 
ORDER BY missing_count DESC 
LIMIT 10;

-- Check status for a specific month
SELECT * FROM reconciliation_date_status_view 
WHERE date BETWEEN '2025-02-01' AND '2025-02-28' 
ORDER BY reconciliation_percentage ASC;
```

## Integration with Application

The reconciliation system is integrated with several parts of the application:

1. **Data Update Service**: Automatically runs reconciliation checks after data updates
2. **API Endpoints**: Provides status information to the frontend
3. **Database Schema**: Works with the core database schema for curtailment records and Bitcoin calculations
4. **External APIs**: Integrates with DynamoDB for difficulty data

## Advanced Usage

### Processing Large Date Ranges

For processing large date ranges efficiently:

```bash
# Process a month with batch size 20
./unified_reconcile.sh range 2025-01-01 2025-01-31 20
```

### Handling Problematic Dates

For dates that consistently timeout or fail:

```bash
# Use critical mode
./unified_reconcile.sh critical 2025-02-23
```

### Targeted Fixes

For fixing specific records:

```bash
# Fix a specific farm on a specific date and period
./unified_reconcile.sh spot-fix 2025-02-25 12 FARM-123
```

## Performance Considerations

- **Batch Size**: Smaller batch sizes (5-10) are recommended for initial reconciliation
- **Timeouts**: Default timeout of 30 seconds per date can be adjusted in the configuration
- **Memory Usage**: Processing large date ranges can use significant memory
- **Database Load**: Avoid running large reconciliation jobs during peak hours

## Troubleshooting

### Common Issues and Solutions

| Issue | Probable Cause | Solution |
|-------|----------------|----------|
| Timeouts | Large data volume | Reduce batch size or use critical mode |
| Missing calculations | Database connection issues | Check connection pool and retry |
| Zero records processed | Incorrect date format | Ensure YYYY-MM-DD format |
| Duplicate records | Interrupted processing | Check and clean duplicates with SQL |

### Diagnostic Tools

```bash
# Check database connections
npx tsx reconciliation_manager.ts diagnose

# Test DynamoDB connectivity
npx tsx server/scripts/test-dynamo.ts
```

## Extending the System

To add new functionality to the reconciliation system:

1. Add new functions to `unified_reconciliation.ts`
2. Update the `unified_reconcile.sh` script with new commands
3. Add any new SQL queries to `reconciliation.sql`
4. Update documentation in the relevant Markdown files