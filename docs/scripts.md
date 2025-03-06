# Bitcoin Mining Analytics Platform - Scripts Documentation

This document provides detailed information about the main scripts used in the Bitcoin Mining Analytics platform.

## Reconciliation Scripts

### 1. Complete Reingestion Process

**File**: `complete_reingestion_process.ts`

This script provides a comprehensive solution for reingesting Elexon API data for a specific date, processing curtailment records, and calculating Bitcoin mining potential.

#### Features:
- Handles API timeouts and connection issues
- Prevents duplicate records using ON CONFLICT clauses
- Processes data in efficient batches
- Supports all 48 settlement periods and multiple miner models
- Includes comprehensive logging and verification

#### Functions:
- `log(message, type)` - Logs messages with consistent formatting
- `getMinerModelInfo(minerModel)` - Gets miner hashrate and power consumption
- `checkCurtailmentData(date)` - Checks if a date has curtailment data
- `clearCurtailmentRecords(date)` - Clears existing curtailment records
- `clearBitcoinCalculations(date)` - Clears existing Bitcoin calculations
- `processCurtailmentData(date)` - Processes curtailment data reingestion
- `processBitcoinCalculationsForPeriod(date, period, model)` - Processes Bitcoin calculations for a period
- `processBitcoinBatch(date, periods, model)` - Processes Bitcoin calculations for periods in a batch
- `processBitcoinCalculations(date, model)` - Processes Bitcoin calculations for all periods
- `verifyBitcoinCalculations(date, model)` - Verifies Bitcoin calculations
- `completeReingestion(date)` - Completes reingestion and processing

#### Usage:
```bash
npx tsx complete_reingestion_process.ts [date]
```

### 2. Daily Reconciliation Check

**File**: `daily_reconciliation_check.ts`

This script automatically checks the reconciliation status for recent dates and processes any missing calculations.

#### Features:
- Robust error handling
- Connection resilience
- Comprehensive logging
- Checkpoint system for resumability

#### Functions:
- `log(message, level)` - Logs messages with appropriate level
- `sleep(ms)` - Promise-based sleep function
- `withRetry(operation, maxRetries, delay)` - Retries operations with exponential backoff
- `saveCheckpoint(checkpoint)` - Saves checkpoint data to file
- `loadCheckpoint()` - Loads checkpoint data from file
- `checkDateReconciliationStatus(date)` - Checks reconciliation status for a date
- `fixDateComprehensive(date)` - Fixes missing calculations for a date
- `runDailyCheck()` - Runs the daily reconciliation check

#### Usage:
```bash
npx tsx daily_reconciliation_check.ts [days=2] [forceProcess=false]
```

### 3. Unified Reconciliation System

**File**: `unified_reconciliation.ts`

This script provides a comprehensive solution for ensuring data integrity between curtailment_records and historical_bitcoin_calculations tables.

#### Features:
- Efficient batch processing
- Careful connection handling
- Progress tracking and checkpoints
- Advanced retry logic with exponential backoff
- Comprehensive logging and reporting

#### Functions:
- `log(message, type)` - Logs messages with consistent formatting
- `saveCheckpoint()` - Saves checkpoint data to file
- `loadCheckpoint()` - Loads checkpoint data from file
- `resetCheckpoint()` - Resets checkpoint data
- `sleep(ms)` - Promise-based sleep function
- `isTimeoutError(error)` - Detects timeout errors
- `getReconciliationStatus()` - Gets summary statistics about reconciliation status
- `findDatesWithMissingCalculations(limit)` - Finds dates with missing Bitcoin calculations
- `processDate(date, attemptNumber)` - Processes a specific date with retry logic
- `processDates(dates, batchSize)` - Processes a batch of dates concurrently
- `processDateRange(startDate, endDate, batchSize)` - Processes a date range
- `processCriticalDate(date)` - Processes a problematic date with extra safeguards
- `spotFix(date, period, farmId)` - Fixes a specific date-period-farm combination
- `analyzeReconciliationStatus()` - Analyzes current reconciliation status and provides recommendations

#### Usage:
```bash
npx tsx unified_reconciliation.ts [command] [options]
```

## Data Processing Scripts

### 1. Reingest Data

**File**: `reingest-data.ts`

This script provides a standardized way to reingest Elexon data for a specific date, update all curtailment records, and trigger cascading updates to Bitcoin calculations.

#### Features:
- Command-line argument parsing
- Flexible options for processing
- Detailed logging
- Integration with core services

#### Functions:
- `log(message, type)` - Logs messages with consistent formatting
- `printResults(stats)` - Prints formatted results
- `main()` - Main function that handles the reingestion process

#### Usage:
```bash
npx tsx reingest-data.ts <date> [options]
```

### 2. Run Migration

**File**: `run_migration.ts`

This script runs SQL migration scripts to create or update tables for the mining potential optimization.

#### Features:
- Database connection handling
- SQL execution with error handling
- Transactional migrations

#### Functions:
- `runMigration()` - Runs the migration process

#### Usage:
```bash
npx tsx run_migration.ts
```

## Checkpoint Files

The reconciliation scripts use checkpoint files to track progress and enable resuming interrupted processes:

### 1. Reconciliation Checkpoint

**File**: `reconciliation_checkpoint.json`

Used by the unified reconciliation system to track progress of missing calculation processing.

#### Structure:
```json
{
  "lastProcessedDate": "2025-03-05",
  "pendingDates": ["2025-03-06", "2025-03-07"],
  "completedDates": ["2025-03-01", "2025-03-02", "2025-03-03", "2025-03-04"],
  "startTime": 1709742856123,
  "lastUpdateTime": 1709743156789,
  "stats": {
    "totalRecords": 8564,
    "processedRecords": 7231,
    "successfulRecords": 7189,
    "failedRecords": 42,
    "timeouts": 3
  }
}
```

### 2. Daily Reconciliation Checkpoint

**File**: `daily_reconciliation_checkpoint.json`

Used by the daily reconciliation check script to track which dates have been processed.

#### Structure:
```json
{
  "lastRun": "2025-03-06",
  "dates": ["2025-03-06", "2025-03-05", "2025-03-04", "2025-03-03", "2025-03-02"],
  "processedDates": [],
  "lastProcessedDate": null,
  "status": "completed",
  "startTime": "2025-03-06T10:15:23.456Z",
  "endTime": "2025-03-06T10:25:45.789Z"
}
```

## Performance Considerations

The scripts are designed with performance in mind:

1. **Batch Processing**: Data is processed in configurable batches to avoid memory issues
2. **Connection Pooling**: Database connections are managed efficiently to avoid exhaustion
3. **Retry Logic**: Failed operations are retried with exponential backoff to handle transient issues
4. **Parallel Processing**: Some operations are performed in parallel where appropriate
5. **Checkpointing**: Progress is saved regularly to enable resuming interrupted processes

## Error Handling

The scripts implement robust error handling strategies:

1. **Categorized Errors**: Different error types are handled appropriately
2. **Retry Logic**: Transient errors are retried automatically
3. **Detailed Logging**: Errors are logged with context for diagnosis
4. **Graceful Degradation**: When possible, scripts continue processing despite partial failures
5. **Connection Recovery**: Database connection issues are handled gracefully

## Scheduling Recommendations

For optimal system performance, follow these scheduling recommendations:

1. **Daily Reconciliation Check**: Run daily at off-peak hours (e.g., 2 AM)
2. **Complete Reingestion**: Use only when necessary for specific dates
3. **Unified Reconciliation**: Run weekly for maintenance