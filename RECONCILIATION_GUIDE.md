# Reconciliation System User Guide

## Introduction

The Reconciliation System is designed to ensure 100% alignment between the `curtailment_records` and `historical_bitcoin_calculations` tables in our database. This document provides a comprehensive guide on how to use the various reconciliation tools to address missing calculations and troubleshoot connection timeout issues.

## Quick Start

For immediate reconciliation needs, run:

```bash
./auto_reconcile.sh
```

This script will automatically analyze the database, process critical dates, and generate a progress report.

## Understanding the Reconciliation Process

Each curtailment record requires exactly three Bitcoin calculations (one for each miner model: S19J_PRO, S9, and M20S). The reconciliation system identifies and fixes missing calculations.

### Key Concepts

- **Reconciliation Rate**: The percentage of expected calculations that exist in the database.
- **Critical Dates**: Dates with the highest number of missing calculations.
- **Batch Processing**: Processing records in smaller groups to prevent timeouts.
- **Checkpointing**: Saving progress to resume after interruptions.

## Available Tools

### 1. Efficient Reconciliation Tool

The primary and most efficient tool for large-scale reconciliation:

```bash
npx tsx efficient_reconciliation.ts [command] [options]
```

Commands:
- `status`: Show current reconciliation status
- `analyze`: Identify missing calculations
- `reconcile [batch-size]`: Process all missing calculations
- `date YYYY-MM-DD`: Process a specific date
- `range YYYY-MM-DD YYYY-MM-DD [batch-size]`: Process a date range

Example:
```bash
npx tsx efficient_reconciliation.ts range 2023-01-01 2023-01-31 20
```

### 2. Minimal Reconciliation Tool

A lightweight alternative for problematic dates that cause timeouts:

```bash
npx tsx minimal_reconciliation.ts [command] [options]
```

Commands:
- `sequence DATE BATCH_SIZE`: Process a specific date in small batches
- `critical-date DATE`: Fix a problematic date with extra safeguards
- `most-critical`: Find and fix the most problematic date
- `spot-fix DATE PERIOD FARM`: Fix a specific date-period-farm combination

Example:
```bash
npx tsx minimal_reconciliation.ts critical-date 2022-10-06
```

### 3. Connection Timeout Analyzer

Diagnose and fix connection timeout issues:

```bash
npx tsx connection_timeout_analyzer.ts [command]
```

Commands:
- `analyze`: Run a full connection analysis
- `test`: Test connection with various query complexities

Example:
```bash
npx tsx connection_timeout_analyzer.ts analyze
```

### 4. Progress Reporting Tools

Check the status of reconciliation:

```bash
npx tsx reconciliation_progress_check.ts
npx tsx reconciliation_progress_report.ts
```

## Common Workflows

### Daily Reconciliation Check

To verify and fix reconciliation for recent data:

```bash
npx tsx daily_reconciliation_check.ts
```

### Fix a Specific Problematic Date

For dates that consistently fail with the efficient tool:

1. First try with the efficient tool:
   ```bash
   npx tsx efficient_reconciliation.ts date 2022-10-06
   ```

2. If timeouts occur, use the minimal tool:
   ```bash
   npx tsx minimal_reconciliation.ts critical-date 2022-10-06
   ```

3. For extreme cases, fix one period at a time:
   ```bash
   npx tsx minimal_reconciliation.ts spot-fix 2022-10-06 5 E_BABAW-1
   ```

### Process a Month of Data

To reconcile an entire month:

```bash
npx tsx efficient_reconciliation.ts range 2023-01-01 2023-01-31 20
```

## Troubleshooting

### Connection Timeouts

If you experience timeout issues:

1. Analyze the database connections:
   ```bash
   npx tsx connection_timeout_analyzer.ts analyze
   ```

2. Reduce batch size:
   ```bash
   npx tsx efficient_reconciliation.ts reconcile 5
   ```

3. Use the minimal reconciliation tool for problematic dates.

### Incomplete Reconciliation

If reconciliation is still incomplete:

1. Check the top missing dates:
   ```bash
   npx tsx reconciliation_progress_check.ts
   ```

2. Process each critical date individually.

3. Verify with the progress report:
   ```bash
   npx tsx reconciliation_progress_report.ts
   ```

## Performance Considerations

- Use smaller batch sizes (5-10) for older dates with more data
- Use larger batch sizes (20-50) for recent dates with less data
- Schedule reconciliation during off-peak hours
- Always use checkpointing to resume after interruptions

## Automation

The `auto_reconcile.sh` script provides full automation:

- Analyzes database connection health
- Processes the most critical dates first
- Handles timeout issues automatically
- Generates a comprehensive report

Add it to a cron job for regular execution:

```
0 2 * * * /path/to/auto_reconcile.sh >> /path/to/cron.log 2>&1
```

## Monitoring and Reporting

Monitor reconciliation progress with:

```bash
npx tsx reconciliation_progress_check.ts
```

Generate detailed reports:

```bash
npx tsx reconciliation_progress_report.ts
```

## Best Practices

1. Always start with database analysis to identify potential issues
2. Process the most critical dates (with most missing calculations) first
3. Use appropriate batch sizes based on date and potential timeouts
4. Verify reconciliation rate after processing
5. Schedule regular reconciliation checks to maintain data integrity

By following this guide, you'll be able to efficiently manage the reconciliation process and maintain data integrity between curtailment records and Bitcoin calculations.