# Comprehensive Historical Reconciliation Plan

## Overview

This document outlines the strategy for reconciling all historical data (2022-2024) between the `curtailment_records` and `historical_bitcoin_calculations` tables efficiently. Our current analysis shows 1,514,223 missing calculations that need to be processed.

## Goals

1. Achieve 100% reconciliation for all historical data (2022-2024)
2. Complete the process with minimal system impact
3. Provide detailed progress tracking and reporting
4. Handle failures gracefully with automatic retries
5. Ensure data consistency across all miner models

## Execution Strategy

### Phase 1: Preparation (Day 1)

1. **Segmentation by Time Period:**
   - Divide the reconciliation task into monthly chunks
   - Further subdivide each month into weekly, then daily batches
   - This creates natural checkpoints for tracking progress

2. **Progress Tracking Table:**
   - Create a `reconciliation_progress` table to track:
     - Time period being processed
     - Completion status (not started, in progress, completed, failed)
     - Percentage complete
     - Timestamp of last update
     - Error details (if any)

3. **Optimization of Query Performance:**
   - Add temporary indices to speed up large dataset handling
   - Analyze and optimize join operations for the reconciliation queries
   - Configure database connection pooling for sustained operations

### Phase 2: Initial Batch Processing (Days 2-7)

1. **Monthly Processing Order:**
   - Process most recent to oldest (2024 → 2023 → 2022)
   - This ensures the most relevant data is reconciled first

2. **Concurrent Processing Within Safe Limits:**
   - Process multiple days concurrently while monitoring system performance
   - Start with 3 concurrent day processors, adjust based on system metrics
   - Control total number of transactions per minute

3. **Throttled Execution:**
   - Implement dynamic throttling based on system load
   - Set maximum batch size per transaction (500 records)
   - Add delay between batches (configurable based on system metrics)

4. **Memory Management:**
   - Implement stream processing for large datasets
   - Release resources after each batch is complete
   - Avoid loading entire datasets into memory

### Phase 3: Verification and Gap Filling (Days 8-10)

1. **Automated Verification:**
   - After each month is completed, run verification to identify any gaps
   - Generate detailed reports for each time period

2. **Targeted Gap Filling:**
   - Create a specialised process for any missing calculations discovered
   - Prioritize dates with significant missing data

3. **Consistency Checks:**
   - Validate miner model distribution (should have equal numbers of calculations)
   - Ensure settlement periods match across tables

### Phase 4: Monitoring and Reporting

1. **Real-time Dashboards:**
   - Create a CLI dashboard showing reconciliation progress
   - Track records processed, success rate, and time remaining

2. **Alerting System:**
   - Implement alerts for any processing failures
   - Configure automatic retry for failed batches

3. **Daily Summaries:**
   - Generate daily progress reports
   - Provide detailed statistics on reconciliation status

## Implementation Details

### Batch Size Optimization

```typescript
// Dynamic batch size determination based on system load
function calculateOptimalBatchSize(systemLoad: number): number {
  const baseBatchSize = 500;
  const loadFactor = Math.max(0.1, 1 - (systemLoad / 100));
  return Math.round(baseBatchSize * loadFactor);
}
```

### Execution Rate Control

```typescript
// Control rate of execution with dynamic backoff
async function processWithBackoff(batch: string[], processingFunction: Function): Promise<void> {
  const batchStartTime = Date.now();
  await processingFunction(batch);
  const processingTime = Date.now() - batchStartTime;
  
  // Determine backoff time based on processing time
  // Ensure at least 200ms between batches, more if system is stressed
  const backoffTime = Math.max(200, 1000 - processingTime);
  await sleep(backoffTime);
}
```

### Year/Month Processing Order

To efficiently balance between recency and resource utilization, we'll process data in this order:

1. 2024: all months (most recent first)
2. 2023: December first (known issues), then remaining months newest to oldest
3. 2022: all months newest to oldest

## Timeline Estimate

Based on optimized parallel processing:

- Total records: ~1.5 million
- Processing time: **4-8 hours total**
  - 3 parallel processes per day × 12 days per month × 36 months = 1,296 parallel processes
  - With enhanced database indexing and bulk operations
  - Utilizing high parallelism with dynamic resource adjustment

## Monitoring Commands

```bash
# Check overall progress
npx tsx comprehensive_reconcile.ts status

# Check progress for a specific year-month
npx tsx comprehensive_reconcile.ts status-month 2023-12

# Get detailed report of remaining work
npx tsx comprehensive_reconcile.ts report
```

## Conclusion

This plan provides a methodical and efficient approach to reconciling all historical data while ensuring system stability. By breaking the work into manageable chunks and implementing proper resource management, we can achieve 100% reconciliation with minimal disruption to ongoing operations.