# Bitcoin Mining Analytics Platform - Reconciliation System

This document explains the data reconciliation system used in the Bitcoin Mining Analytics platform.

## Purpose of Reconciliation

The reconciliation system ensures data integrity between the `curtailment_records` and `historical_bitcoin_calculations` tables. Its primary responsibilities are:

1. Detect missing Bitcoin calculations
2. Process any missing calculations
3. Update summary tables
4. Verify data consistency

## Reconciliation Components

The platform includes several reconciliation tools, each with a specific purpose:

### 1. Unified Reconciliation System

Located in `scripts/reconciliation/unified_reconciliation.ts`, this is the comprehensive solution that combines features from multiple reconciliation tools:

- Efficient batch processing
- Connection handling with retry logic
- Progress tracking and checkpointing
- Comprehensive logging

Usage:
```bash
npx tsx scripts/reconciliation/unified_reconciliation.ts [command] [options]
```

Commands:
- `status` - Show current reconciliation status
- `analyze` - Analyze missing calculations and detect issues
- `reconcile [batchSize]` - Process all missing calculations
- `date YYYY-MM-DD` - Process a specific date
- `range YYYY-MM-DD YYYY-MM-DD [batchSize]` - Process a date range
- `critical DATE` - Process a problematic date with extra safeguards
- `spot-fix DATE PERIOD FARM` - Fix a specific date-period-farm combination

### 2. Daily Reconciliation Check

Located in `scripts/reconciliation/daily_reconciliation_check.ts`, this script automatically checks the reconciliation status for recent dates and processes any missing calculations.

Usage:
```bash
npx tsx scripts/reconciliation/daily_reconciliation_check.ts [days=2] [forceProcess=false]
```

### 3. Complete Reingestion Process

Located in `scripts/reconciliation/complete_reingestion_process.ts`, this script provides a comprehensive solution for reingesting Elexon API data for a specific date and recalculating all Bitcoin mining potential.

Usage:
```bash
npx tsx scripts/reconciliation/complete_reingestion_process.ts [date]
```

## Reconciliation Process Flow

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│ Check Status    │────►│ Identify Missing│────►│ Process Missing │
│                 │     │ Calculations    │     │ Calculations    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│ Verify Results  │◄────│ Update Summary  │◄────│ Save Progress   │
│                 │     │ Tables          │     │ Checkpoint      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

## Checkpoint System

The reconciliation system uses checkpoints to track progress and enable resuming interrupted processes:

```typescript
interface ReconciliationCheckpoint {
  lastProcessedDate: string;
  pendingDates: string[];
  completedDates: string[];
  startTime: number;
  lastUpdateTime: number;
  stats: {
    totalRecords: number;
    processedRecords: number;
    successfulRecords: number;
    failedRecords: number;
    timeouts: number;
  };
}
```

Checkpoint files are stored in the `data/checkpoints/` directory:
- `reconciliation_checkpoint.json` - Unified reconciliation system
- `daily_reconciliation_checkpoint.json` - Daily reconciliation check

## Error Handling and Retry Logic

The reconciliation system implements robust error handling:

1. **Connection Issues**: Automatically reconnects to database
2. **Timeouts**: Uses exponential backoff retry strategy
3. **Data Inconsistencies**: Logs issues for manual intervention
4. **Process Interruptions**: Uses checkpoints for resumability

Example of retry logic:
```typescript
async function processDate(date: string, attemptNumber: number = 1): Promise<{success: boolean, message: string}> {
  try {
    // Process the date
    return { success: true, message: `Successfully processed ${date}` };
  } catch (error) {
    if (isTimeoutError(error) && attemptNumber < MAX_RETRIES) {
      const backoffTime = Math.min(BASE_RETRY_DELAY * Math.pow(2, attemptNumber - 1), MAX_RETRY_DELAY);
      log(`Timeout processing ${date}, retry ${attemptNumber} in ${backoffTime}ms`, 'warning');
      await sleep(backoffTime);
      return processDate(date, attemptNumber + 1);
    }
    return { success: false, message: `Failed to process ${date}: ${error.message}` };
  }
}
```

## Data Verification

The reconciliation system includes verification steps to ensure data integrity:

1. **Completeness Check**: Verifies all expected calculations exist
2. **Consistency Check**: Ensures calculations match curtailment records
3. **Summary Validation**: Validates that summary tables match detailed records

Example verification check:
```typescript
async function verifyBitcoinCalculations(date: string, minerModel: string): Promise<{
  expected: number;
  actual: number;
  missing: number;
  completionPercentage: number;
}> {
  // Count expected records based on curtailment data
  const expectedRecords = await countExpectedRecords(date);
  
  // Count actual Bitcoin calculation records
  const actualRecords = await countActualRecords(date, minerModel);
  
  // Calculate missing records and completion percentage
  const missingRecords = expectedRecords - actualRecords;
  const completionPercentage = expectedRecords > 0 
    ? (actualRecords / expectedRecords) * 100 
    : 100;
    
  return {
    expected: expectedRecords,
    actual: actualRecords,
    missing: missingRecords,
    completionPercentage
  };
}
```

## Automatic Update Chain

When the reconciliation system processes missing calculations, it triggers an automatic update chain:

1. Daily Bitcoin calculations → Monthly Bitcoin summaries
2. Monthly Bitcoin summaries → Yearly Bitcoin summaries

This ensures that all summary levels remain consistent with the underlying data.

## Performance Considerations

The reconciliation system is designed for performance:

1. **Batch Processing**: Processes data in configurable batches
2. **Concurrency Limits**: Prevents database connection exhaustion
3. **Targeted Processing**: Only processes dates with missing data
4. **Query Optimization**: Uses efficient SQL queries

## Monitoring and Reporting

The reconciliation system provides comprehensive reporting:

1. **Status Reports**: Shows the current state of data completeness
2. **Progress Updates**: Reports on processing progress
3. **Completion Statistics**: Provides metrics on process completion
4. **Log Files**: Generates detailed logs in the `logs/` directory

Example status report:
```
=== Reconciliation Status ===
Total curtailment records: 123,456
Total Bitcoin calculations: 115,789
Missing calculations: 7,667
Completion percentage: 93.79%

Dates with missing calculations:
2025-02-28: 82.5% complete (missing 126 calculations)
2025-03-01: 90.3% complete (missing 78 calculations)
2025-03-02: 76.1% complete (missing 192 calculations)
```

## Usage Recommendations

For optimal use of the reconciliation system:

1. Run the daily reconciliation check as a scheduled task
2. Use the unified reconciliation system for manual interventions
3. Use the complete reingestion process for problematic dates
4. Check reconciliation status regularly to ensure data integrity