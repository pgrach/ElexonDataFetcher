# Bitcoin Mining Reconciliation System

This document provides comprehensive documentation for the Bitcoin Mining Reconciliation System, a sophisticated solution designed to ensure data consistency between curtailment records and historical Bitcoin calculations.

## Overview

The reconciliation system ensures that for every curtailment record in our database, we have the corresponding Bitcoin mining potential calculations for each supported miner model (S19J_PRO, S9, and M20S). The system handles verification, error detection, and automated fixing of any discrepancies.

## Core Components

Our reconciliation system consists of several integrated components:

1. **Unified Reconciliation System** (`unified_reconciliation.ts`): The core engine that provides comprehensive reconciliation functionality with sophisticated error handling, checkpointing, and batch processing.

2. **Comprehensive Reconciliation** (`comprehensive_reconciliation.ts`): A high-performance solution with parallel processing, intelligent prioritization, and reporting capabilities.

3. **Daily Reconciliation Check** (`daily_reconciliation_check.ts`): Automated daily maintenance tool for recent data.

4. **Scheduled Reconciliation** (`scheduled_reconciliation.ts`): Scheduled maintenance to ensure regular updates.

5. **Historical Reconciliation Service** (`server/services/historicalReconciliation.ts`): Core service that provides the functionality used by all reconciliation tools.

## Usage Guide

### Daily Maintenance

For routine maintenance of recent data:

```bash
# Check and fix the last 2 days (default)
npx tsx daily_reconciliation_check.ts

# Check and fix the last 5 days
npx tsx daily_reconciliation_check.ts 5

# Force reprocessing of the last 3 days even if they appear complete
npx tsx daily_reconciliation_check.ts 3 true
```

### Comprehensive Reconciliation

For more advanced reconciliation operations:

```bash
# Show current reconciliation status
npx tsx comprehensive_reconciliation.ts status

# Reconcile all dates in the database
npx tsx comprehensive_reconciliation.ts reconcile-all

# Reconcile a specific date range
npx tsx comprehensive_reconciliation.ts reconcile-range 2025-02-01 2025-02-28

# Reconcile recent data (last 30 days by default)
npx tsx comprehensive_reconciliation.ts reconcile-recent

# Fix dates with known issues
npx tsx comprehensive_reconciliation.ts fix-critical

# Generate detailed reconciliation report
npx tsx comprehensive_reconciliation.ts report
```

### Unified Reconciliation

For precise control over reconciliation operations:

```bash
# Show current reconciliation status
npx tsx unified_reconciliation.ts status

# Analyze missing calculations and detect issues
npx tsx unified_reconciliation.ts analyze

# Process all missing calculations with batch size 10
npx tsx unified_reconciliation.ts reconcile 10

# Process a specific date
npx tsx unified_reconciliation.ts date 2025-02-28

# Process a date range with batch size 5
npx tsx unified_reconciliation.ts range 2025-02-01 2025-02-28 5

# Process a problematic date with extra safeguards
npx tsx unified_reconciliation.ts critical 2025-02-23

# Fix a specific date-period-farm combination
npx tsx unified_reconciliation.ts spot-fix 2025-02-25 12 FARM-123
```

### Simple Reconciliation

For quick fixes of a single date:

```bash
# Reconcile a specific date
npx tsx simple_reconcile.ts 2025-02-28
```

### Batch Reconciliation

For processing historical data in batches:

```bash
# Reconcile a date range
npx tsx batch_reconcile.ts 2025-01-01 2025-01-31
```

## Monitoring and Troubleshooting

### Logs

Reconciliation logs are stored in:
- `reconciliation.log` - Main log file for reconciliation operations
- `comprehensive_reconciliation.log` - Logs from comprehensive reconciliation operations
- `unified_reconciliation.log` - Logs from unified reconciliation system
- `logs/daily_reconciliation_*.log` - Logs from daily reconciliation checks

To monitor progress:

```bash
# Watch reconciliation log in real-time
tail -f reconciliation.log
```

### Common Issues

1. **Timeouts during large batch operations**
   
   Solution: Reduce batch size or use critical mode
   ```bash
   npx tsx unified_reconciliation.ts critical 2025-02-23
   ```

2. **Missing calculations for specific periods**
   
   Solution: Use spot-fix for targeted fixing
   ```bash
   npx tsx unified_reconciliation.ts spot-fix 2025-02-25 12 FARM-123
   ```

3. **Reconciliation failures due to difficulty data**
   
   Solution: Verify difficulty data is available in DynamoDB
   ```bash
   npx tsx server/scripts/test-dynamo.ts
   ```

## Best Practices

### Performance Optimization

For optimal performance:

1. **Batch Size**: Start with small batch sizes (5-10) and increase as needed
2. **Timeout Handling**: For frequent timeouts, use critical mode for processing
3. **Database Load**: Schedule large reconciliation jobs during off-peak hours
4. **Checkpoints**: The system creates checkpoints that allow resuming interrupted operations

### Schedule and Automation

The reconciliation system is integrated with the platform's data updater service, which runs:
- Daily checks automatically each morning
- Monthly comprehensive checks on the 1st of each month
- Automated verification after real-time data updates

### Maintenance

Regular maintenance includes:

1. **Daily Check**: Run `npx tsx daily_reconciliation_check.ts` daily to keep recent data in sync
2. **Weekly Analysis**: Run `npx tsx unified_reconciliation.ts analyze` weekly to identify any issues
3. **Monthly Verification**: Verify full month reconciliation at the beginning of each month

## Enhancement Roadmap

### Short-term Goals

- Performance optimizations (parallel processing, smart batching)
- Reliability improvements (enhanced error handling, circuit breakers)
- Monitoring and alerting improvements (real-time dashboard, alert system)

### Medium-term Goals

- Advanced features (predictive reconciliation, historical analysis)
- Integration improvements (event streaming, external notifications)
- User experience improvements (interactive CLI, web UI)

### Long-term Vision

- System architecture enhancements (microservices, cloud-native optimization)
- Advanced analytics (anomaly detection, trend analysis)
- Scalability improvements (horizontal scaling, distributed processing)

## Architecture Details

### Data Flow

1. Curtailment records are the source of truth for reconciliation
2. For each curtailment record, calculations are generated for all miner models
3. The reconciliation system verifies and fixes missing or incorrect calculations

### Key Components

- **Checkpoint System**: Allows resuming interrupted operations
- **Error Handling**: Sophisticated retry mechanism with exponential backoff
- **Reporting**: Comprehensive statistics and status reporting
- **Performance Optimization**: Connection pooling and batch processing

## Technical Details

### Database Schema Relationship

The reconciliation system ensures consistency between:

1. `curtailment_records` table (source of truth)
2. `historical_bitcoin_calculations` table (derived calculations)
3. `bitcoin_monthly_summaries` table (aggregated data)

### Reconciliation Logic

For each record in `curtailment_records`:
1. Check if corresponding records exist in `historical_bitcoin_calculations` for all miner models
2. If missing, calculate and insert the missing records
3. Verify calculations for consistency and accuracy
4. Update aggregate tables as necessary

## Conclusion

The Bitcoin Mining Reconciliation System provides a robust, flexible, and efficient solution for ensuring data consistency across our platform. By following the guidelines in this document, you can effectively maintain data integrity and optimize reconciliation operations.