# Reconciliation System Guide

This guide provides comprehensive documentation for the Bitcoin calculation reconciliation system, designed to ensure 100% reconciliation between curtailment records and historical Bitcoin calculations.

## Overview

The reconciliation system ensures that every valid curtailment record has a corresponding Bitcoin calculation for each miner model. This is critical for accurate reporting and analytics.

## Key Metrics (As of February 28, 2025)

- **Overall Reconciliation Rate**: 50.91% (1,070,394 out of 2,102,568 calculations)
- **Most Problematic Date**: 2022-10-06 (17,691 missing calculations)
- **Second Most Problematic Date**: 2022-06-11 (16,983 missing calculations)

## Reconciliation Tools

We've developed several specialized tools to address reconciliation challenges:

### Core Reconciliation Scripts

1. **efficient_reconciliation.ts**
   - Highly optimized batch processing with adjustable size
   - Checkpoint-based processing for resumability
   - Auto-retry mechanism with exponential backoff
   - Usage: `npx tsx efficient_reconciliation.ts [command] [options]`
   - Commands:
     - `status` - Show current reconciliation status
     - `analyze` - Analyze and identify missing calculations
     - `reconcile [batch-size]` - Process all missing calculations with specified batch size
     - `date YYYY-MM-DD` - Process a specific date
     - `range YYYY-MM-DD YYYY-MM-DD [batch-size]` - Process a date range

2. **minimal_reconciliation.ts**
   - Lightweight script for problematic cases where regular tools may time out
   - Uses minimal database connections, small batches, and sequential processing
   - Usage: `npx tsx minimal_reconciliation.ts [command] [options]`
   - Commands:
     - `sequence DATE BATCH_SIZE` - Process a specific date in small sequential batches
     - `critical-date DATE` - Fix a problematic date with extra safeguards
     - `most-critical` - Find and fix the most problematic date
     - `spot-fix DATE PERIOD FARM` - Fix a specific date-period-farm combination

3. **reconciliation_manager.ts**
   - Comprehensive solution integrating batch processing and timeout diagnostics
   - Usage: `npx tsx reconciliation_manager.ts [command] [options]`
   - Commands:
     - `status` - Show current reconciliation status
     - `analyze` - Analyze missing calculations and diagnose issues
     - `fix` - Fix missing calculations using optimized batch processing
     - `diagnose` - Run diagnostics on database connections and timeout issues
     - `schedule` - Schedule regular reconciliation checks

### Shell Scripts for Automation

1. **auto_reconcile.sh**
   - Automated script for reconciling missing calculations with timeout handling
   - Can be scheduled to run regularly
   - Usage: `./auto_reconcile.sh [batch-size]`

2. **process_critical_date.sh**
   - Focuses on reconciling a single critical date with careful error handling
   - Processes records one-by-one to avoid timeouts
   - Usage: `./process_critical_date.sh [YYYY-MM-DD]`
   - Default date is 2022-10-06 if none provided

3. **process_critical_batch.sh**
   - Processes a batch of the most critical dates in sequence
   - Built-in pauses between dates to let connections settle
   - Usage: `./process_critical_batch.sh`

4. **analyze_reconciliation.sh**
   - Provides a detailed analysis of the reconciliation status
   - Checks critical dates and overall progress
   - Usage: `./analyze_reconciliation.sh`

### Analysis and Monitoring

1. **reconciliation_dashboard.ts**
   - Comprehensive dashboard for viewing reconciliation status across different dimensions
   - Displays metrics by date, miner model, and time period
   - Usage: `npx tsx reconciliation_dashboard.ts`
   - Or use the interactive script: `./generate_dashboard.sh`

2. **generate_dashboard.sh**
   - Interactive dashboard with menu-driven reconciliation operations
   - Combines status reporting with execution capabilities
   - Provides options to fix critical dates, run auto reconciliation, and check daily status
   - Usage: `./generate_dashboard.sh`

3. **connection_timeout_analyzer.ts**
   - Analyzes database connection timeouts and provides diagnostics
   - Usage: `npx tsx connection_timeout_analyzer.ts [command]`
   - Commands:
     - `analyze` - Run a full connection analysis
     - `monitor` - Start monitoring connections in real-time
     - `test` - Test connection with various query complexities

4. **daily_reconciliation_check.ts**
   - Enhanced daily monitoring script with checkpoint-based processing
   - Verifies and fixes reconciliation for recent dates
   - Usage: `npx tsx daily_reconciliation_check.ts [days=2] [forceProcess=false]`
   - Parameters:
     - `days` - Number of recent days to check (default: 2)
     - `forceProcess` - Force processing even if fully reconciled (default: false)

5. **reconciliation_progress_check.ts**
   - Quick overview of current reconciliation status and completion percentages
   - Usage: `npx tsx reconciliation_progress_check.ts`

6. **reconciliation_progress_report.ts**
   - Detailed report on reconciliation status with comprehensive statistics
   - Usage: `npx tsx reconciliation_progress_report.ts`

7. **reconciliation_visualization.ts**
   - Visual representation of reconciliation progress
   - Helps identify patterns and priority areas
   - Usage: `npx tsx reconciliation_visualization.ts`

## Common Issues and Solutions

### Database Timeouts

**Causes:**
- Large volume of missing records
- Resource-intensive queries
- Connection pool exhaustion

**Solutions:**
- Use minimal_reconciliation.ts with batch size 1 for critical dates
- Process dates sequentially instead of in parallel
- Implement careful pauses between operations
- Monitor database connections with connection_timeout_analyzer.ts

### Implementation Tips

1. **For Routine Reconciliation:**
   ```bash
   ./auto_reconcile.sh 10  # Use batch size of 10
   ```

2. **For Critical Dates:**
   ```bash
   ./process_critical_date.sh 2022-10-06
   ```

3. **For Sequential Processing of Multiple Critical Dates:**
   ```bash
   ./process_critical_batch.sh
   ```

4. **For Analyzing Reconciliation Status:**
   ```bash
   ./analyze_reconciliation.sh
   ```

## Monitoring Progress

Track reconciliation progress in the `./logs/` directory:
- `auto_reconciliation_YYYY-MM-DD.log` - Logs from auto reconciliation
- `critical_date_YYYY-MM-DD.log` - Logs from critical date processing
- `critical_batch_YYYY-MM-DD.log` - Logs from batch processing
- `analysis_YYYY-MM-DD.log` - Analysis results

## Database Schema

The two main tables involved in reconciliation are:

1. **curtailment_records** - Source of truth for all curtailment events
2. **historical_bitcoin_calculations** - Bitcoin calculations for each curtailment record by miner model

Each record in curtailment_records should have three corresponding records in historical_bitcoin_calculations (one for each miner model: S19J_PRO, S9, and M20S).

## Future Improvements

1. **Automated Daily Reconciliation** - Schedule daily reconciliation checks for newly added records
2. **Enhanced Error Recovery** - Implement more sophisticated retry mechanisms
3. **Performance Optimization** - Tune database queries for better performance
4. **Web Interface** - Create a dashboard for monitoring reconciliation progress

## Support

For issues with the reconciliation system, contact the development team.