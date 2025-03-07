# Data Reprocessing Guide

This guide provides instructions for reprocessing data in the Bitcoin Mining Analytics platform. It covers the various tools available, their specific use cases, and example commands.

## Overview of Tools

| Tool | Description | Best For | Time to Complete |
|------|-------------|----------|------------------|
| `check_date_status.ts` | Verification tool to check data status | Data inspection | < 1 second |
| `reprocess_date_simple.ts` | Simple, fast tool for Bitcoin calculation updates | Quick fixes | < 1 second |
| `reprocess_date.ts` | Complete reprocessing tool with curtailment support | Full reprocessing | 5-10 seconds |
| `complete_reingestion_process.ts` | Comprehensive reingestion tool | Complete re-runs | 30-60 seconds |
| `unified_reconciliation.ts` | Advanced reconciliation system | Fixing data gaps | Variable |

## Common Usage Patterns

### 1. Check Date Status

To verify the status of a specific date's data:

```bash
npx tsx check_date_status.ts <date>
```

Example:
```bash
npx tsx check_date_status.ts 2025-03-06
```

This will show:
- Number of curtailment records
- Coverage of settlement periods
- Total curtailed volume and payment
- Bitcoin calculations for each miner model
- Bitcoin mining totals

### 2. Quick Bitcoin Calculation Update

For fast reprocessing of Bitcoin calculations only:

```bash
npx tsx reprocess_date_simple.ts <date>
```

Example:
```bash
npx tsx reprocess_date_simple.ts 2025-03-06
```

This tool:
- Checks for missing Bitcoin calculations
- Updates only what's necessary
- Completes typically in under 1 second
- Doesn't modify curtailment data

### 3. Full Date Reprocessing

For reprocessing both curtailment and Bitcoin data:

```bash
npx tsx reprocess_date.ts <date> --full
```

Without the `--full` flag, it will only reprocess Bitcoin calculations if curtailment data is complete.

Example:
```bash
npx tsx reprocess_date.ts 2025-03-06
```

### 4. Complete Reingestion

For a complete reingestion from the Elexon API:

```bash
npx tsx complete_reingestion_process.ts <date>
```

Example:
```bash
npx tsx complete_reingestion_process.ts 2025-03-06
```

This will:
1. Clear existing records
2. Fetch fresh data from Elexon API
3. Process all curtailment records
4. Calculate Bitcoin mining potential
5. Update monthly and yearly summaries

### 5. Advanced Reconciliation

For advanced reconciliation operations:

```bash
npx tsx unified_reconciliation.ts <command> [options]
```

Common commands:
- `status` - Show reconciliation status
- `date YYYY-MM-DD` - Process a specific date
- `range YYYY-MM-DD YYYY-MM-DD` - Process a date range

Example:
```bash
npx tsx unified_reconciliation.ts date 2025-03-06
```

## Troubleshooting

If you encounter issues with data reprocessing:

1. **First step**: Use `check_date_status.ts` to verify the current state
2. **For Bitcoin calculation issues**: Try `reprocess_date_simple.ts` first
3. **For missing curtailment data**: Use `reprocess_date.ts --full` or `complete_reingestion_process.ts`
4. **For complex issues**: Use the `unified_reconciliation.ts` tool with appropriate commands

## Database Impact

| Tool | Clears Curtailment | Updates Bitcoin | Updates Summaries |
|------|-------------------|-----------------|-------------------|
| `check_date_status.ts` | No | No | No |
| `reprocess_date_simple.ts` | No | Yes | Yes |
| `reprocess_date.ts` | Optional | Yes | Yes |
| `complete_reingestion_process.ts` | Yes | Yes | Yes |
| `unified_reconciliation.ts` | No | Yes | Yes |

## Performance Considerations

- The simple reprocessing tool is optimized for speed and is the preferred choice for daily operations
- Full reprocessing tools may cause temporary database load spikes
- For bulk operations on many dates, use the range processing capabilities of `unified_reconciliation.ts`
- All tools implement appropriate error handling and logging

## Logging

All tools provide detailed console output and also log to the appropriate log files:
- `/logs/daily_reconciliation_YYYY-MM-DD.log`
- `/logs/reingestion_YYYYMMDD.log`
- `/logs/reconciliation.log`

## Verification

Always verify data integrity after reprocessing using:

```bash
npx tsx check_date_status.ts <date>
```

This will confirm that all records are complete and accurate.