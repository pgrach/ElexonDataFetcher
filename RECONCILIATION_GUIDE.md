# Reconciliation Tool Suite Documentation

This guide provides comprehensive documentation for the Bitcoin mining reconciliation tools that ensure 100% alignment between the `curtailment_records` and `historical_bitcoin_calculations` tables.

## Overview

The reconciliation system ensures that for each unique combination of:
- `settlement_date`
- `settlement_period`
- `farm_id`

in the `curtailment_records` table, there are exactly three corresponding records in the `historical_bitcoin_calculations` table - one for each miner model (S19J_PRO, S9, M20S).

## Quick Start

To get started immediately, use the Reconciliation Manager:

```bash
# Check current reconciliation status
npx tsx reconciliation_manager.ts status

# Fix missing calculations
npx tsx reconciliation_manager.ts fix

# Analyze reconciliation issues
npx tsx reconciliation_manager.ts analyze
```

## Tool Suite Components

### 1. Reconciliation Manager (`reconciliation_manager.ts`)

The main entry point for all reconciliation operations, providing a user-friendly interface to all reconciliation tools.

**Key Commands:**
- `status` - Show current reconciliation status
- `analyze` - Analyze missing calculations and diagnose issues
- `fix [batch-size]` - Fix missing calculations with optimized batch processing
- `diagnose` - Run diagnostics on database connections
- `date YYYY-MM-DD` - Fix a specific date
- `range YYYY-MM-DD YYYY-MM-DD [batch-size]` - Fix a date range

**Example:**
```bash
npx tsx reconciliation_manager.ts status
npx tsx reconciliation_manager.ts fix 5
```

### 2. Efficient Reconciliation Tool (`efficient_reconciliation.ts`)

An optimized tool designed for high-performance reconciliation with robust error handling, checkpoint-based processing, and detailed progress tracking.

**Key Features:**
- Batch processing with adjustable batch size
- Checkpoint-based processing for resumability after interruptions
- Connection pool management to prevent timeouts
- Comprehensive logging and timeout detection

**Key Commands:**
- `status` - Show reconciliation status
- `analyze` - Analyze and identify missing calculations
- `reconcile [batch-size]` - Process all missing calculations
- `date YYYY-MM-DD` - Process a specific date
- `range YYYY-MM-DD YYYY-MM-DD [batch-size]` - Process a date range
- `resume` - Resume from last checkpoint

**Example:**
```bash
npx tsx efficient_reconciliation.ts analyze
npx tsx efficient_reconciliation.ts reconcile 5
```

### 3. Connection Timeout Analyzer (`connection_timeout_analyzer.ts`)

A diagnostic tool that analyzes database connection behavior and identifies potential timeout issues.

**Key Features:**
- Analyzes database connection performance
- Tests different query complexities
- Diagnoses potential timeout causes
- Provides specific recommendations

**Key Commands:**
- `analyze` - Run a full connection analysis
- `test` - Test connection with various query complexities
- `monitor` - Start monitoring connections in real-time

**Example:**
```bash
npx tsx connection_timeout_analyzer.ts analyze
```

### 4. Minimal Reconciliation Tool (`minimal_reconciliation.ts`)

A lightweight tool designed for problematic cases where regular tools may time out, using minimal connections and sequential processing.

**Key Features:**
- Minimal database connection usage
- Sequential processing to prevent timeouts
- Focused on fixing individual combinations

**Key Commands:**
- `sequence DATE BATCH_SIZE` - Process a date in sequential batches
- `critical-date DATE` - Fix a problematic date with extra safeguards
- `most-critical` - Find and fix the most problematic date
- `spot-fix DATE PERIOD FARM` - Fix a specific date-period-farm combination

**Example:**
```bash
npx tsx minimal_reconciliation.ts critical-date 2023-12-25
npx tsx minimal_reconciliation.ts spot-fix 2023-12-25 24 T_VKNGW-1
```

## Detailed Reconciliation Process

### How Reconciliation Works

1. **Identification:** The system identifies missing calculations by comparing unique combinations in `curtailment_records` with entries in `historical_bitcoin_calculations`.

2. **Calculation:** For each missing combination, the system:
   - Retrieves the curtailment record data
   - Fetches the difficulty value for the date
   - Calculates the Bitcoin mining potential for each miner model
   - Inserts the results into the `historical_bitcoin_calculations` table

3. **Verification:** After processing, the system verifies that all expected calculations exist.

### Expected Calculations

For 100% reconciliation:
- If there are N unique `(date, period, farm_id)` combinations in `curtailment_records`
- Then `historical_bitcoin_calculations` should have exactly N × 3 records (one for each miner model)

## Handling Timeouts

Timeouts can occur during reconciliation due to:

1. **Database Connection Issues:**
   - Large query complexity
   - Too many concurrent connections
   - Network latency

2. **Processing Volume:**
   - Too many records processed at once
   - Insufficient memory for query results

### Timeout Prevention Strategies

The tools implement several strategies to prevent timeouts:

1. **Batch Processing:**
   - Process records in smaller batches
   - Control batch size via command-line parameters

2. **Connection Management:**
   - Limit concurrent database connections
   - Release connections promptly after use
   - Refresh connections periodically

3. **Checkpointing:**
   - Save progress after each batch
   - Resume processing from checkpoints

4. **Sequential Processing:**
   - Use `minimal_reconciliation.ts` for problematic dates
   - Process combinations one by one for critical cases

## Troubleshooting

### Common Issues

1. **Timeouts During Processing:**
   - Reduce batch size: `npx tsx reconciliation_manager.ts fix 3`
   - Try minimal reconciliation: `npx tsx minimal_reconciliation.ts critical-date 2023-12-25`
   - Run diagnostics: `npx tsx connection_timeout_analyzer.ts analyze`

2. **Incomplete Reconciliation:**
   - Check specific dates: `npx tsx efficient_reconciliation.ts analyze`
   - Fix individual dates: `npx tsx reconciliation_manager.ts date 2023-12-25`

3. **Slow Performance:**
   - Diagnose connection issues: `npx tsx connection_timeout_analyzer.ts test`
   - Reduce concurrency settings in scripts
   - Process data during off-peak hours

### Timeout Diagnosis

If timeouts persist, run a detailed diagnosis:

```bash
npx tsx connection_timeout_analyzer.ts analyze
```

This will provide specific recommendations for addressing connection issues.

## Best Practices

1. **Regular Monitoring:**
   - Run `npx tsx reconciliation_manager.ts status` daily
   - Schedule regular reconciliation checks

2. **Batch Size Optimization:**
   - Start with smaller batch sizes (3-5)
   - Increase gradually if no timeouts occur
   - For problematic dates, use batch size 1

3. **Progressive Approach:**
   - Fix recent dates first
   - Then address historical dates
   - Use date ranges for systematic reconciliation

4. **Timeout Handling:**
   - If timeouts occur, reduce batch size
   - For persistent issues, use minimal reconciliation
   - Follow recommendations from connection analyzer

## Scheduling Reconciliation

For automated reconciliation, consider setting up scheduled tasks:

### Daily Quick Check:
```bash
# Run daily at 2 AM
0 2 * * * cd /path/to/project && npx tsx daily_reconciliation_check.ts >> logs/reconciliation.log 2>&1
```

### Weekly Full Reconciliation:
```bash
# Run every Sunday at 1 AM
0 1 * * 0 cd /path/to/project && npx tsx reconciliation_manager.ts fix 5 >> logs/full_reconciliation.log 2>&1
```

## Conclusion

This comprehensive tool suite ensures 100% reconciliation between curtailment records and Bitcoin calculations while avoiding timeout issues through optimized processing, intelligent batching, and robust error handling.

For any persistent issues, escalate to the database administrator for potential database-level optimizations.