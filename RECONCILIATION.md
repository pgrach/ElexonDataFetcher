# Bitcoin Calculation Reconciliation Tools

This directory contains various tools and scripts to ensure complete data integrity between the `curtailment_records` table (source of truth) and the `historical_bitcoin_calculations` table.

## Available Tools

### 1. Batch Reconciliation Tool (Recommended)

The most reliable and efficient tool for data reconciliation, designed to handle the full dataset:

```bash
npx tsx batch_reconcile.ts [startDate] [endDate]
```

**Examples:**
- Process all dates: `npx tsx batch_reconcile.ts`
- Process a specific date: `npx tsx batch_reconcile.ts 2025-02-28 2025-02-28`
- Process a date range: `npx tsx batch_reconcile.ts 2025-01-01 2025-02-28`

### 2. Simple Single-Date Reconciliation 

A lightweight tool for quickly reconciling a single date:

```bash
npx tsx simple_reconcile.ts [date]
```

**Example:**
- `npx tsx simple_reconcile.ts 2025-02-28`

### 3. Basic Reconciliation Script

A simple tool to fix missing Bitcoin calculations:

```bash
npx tsx reconciliation_script.ts [startDate] [endDate]
```

**Examples:**
- Process all dates: `npx tsx reconciliation_script.ts`
- Process a specific date: `npx tsx reconciliation_script.ts 2025-02-28`
- Process a date range: `npx tsx reconciliation_script.ts 2025-01-01 2025-02-28`

### 2. Comprehensive Reconciliation System

A more advanced system with multiple features:

```bash
npx tsx comprehensive_reconciliation.ts [command]
```

**Available Commands:**
- `status` - Show current reconciliation status
- `reconcile-all` - Reconcile all dates in the database
- `reconcile-range` - Reconcile a specific date range
- `reconcile-recent` - Reconcile recent data (default: last 30 days)
- `fix-critical` - Fix dates with known issues
- `report` - Generate detailed reconciliation report

**Examples:**
- Show status: `npx tsx comprehensive_reconciliation.ts status`
- Reconcile last 7 days: `npx tsx comprehensive_reconciliation.ts reconcile-recent 7`
- Reconcile specific range: `npx tsx comprehensive_reconciliation.ts reconcile-range 2025-01-01 2025-01-31`

### 3. Unified Reconciliation (Legacy/Original)

The original reconciliation system:

```bash
npx tsx unified_reconciliation.ts [command] [options]
```

**Available Commands:**
- `status` - Show current reconciliation status
- `analyze` - Analyze missing calculations and detect issues
- `reconcile [batchSize]` - Process all missing calculations
- `date YYYY-MM-DD` - Process a specific date
- `range YYYY-MM-DD YYYY-MM-DD [batchSize]` - Process a date range
- `critical DATE` - Process a problematic date with extra safeguards
- `spot-fix DATE PERIOD FARM` - Fix a specific date-period-farm combination

## Reconciliation Best Practices

1. **Automated Scheduled Checks**: Set up a cron job to run the scheduled reconciliation script:
   ```bash
   npx tsx scheduled_reconciliation.ts [days=7]
   ```
   This will automatically check and reconcile the most recent days' data.

2. **Regular Status Monitoring**: Run `npx tsx batch_reconcile.ts` periodically to find and fix any missing calculations.

3. **Issue Investigation**: If specific dates continue to have problems, use the simple_reconcile.ts script for debugging:
   ```bash
   npx tsx simple_reconcile.ts YYYY-MM-DD
   ```

4. **Comprehensive Reconciliation**: For a full database audit and reconciliation, use:
   ```bash
   npx tsx batch_reconcile.ts
   ```

5. **Targeted Fixes**: If specific date-period-farm combinations are problematic, use:
   ```bash
   npx tsx unified_reconciliation.ts spot-fix DATE PERIOD FARM
   ```

## Understanding the Reconciliation Process

The reconciliation process ensures that for every curtailment record, there are corresponding Bitcoin calculations for each miner model. The process:

1. Identifies dates with missing or incomplete calculations
2. Processes each date to generate missing calculations
3. Verifies the completeness of the reconciliation

The relationship is:
- Each curtailment record (date-period-farm) should have
- One calculation for each miner model (S19J_PRO, S9, M20S)

## Log Files

The reconciliation tools generate detailed logs that can be found in the logs directory:
- Daily logs for reconciliation activities are stored in the `logs/` directory with date-stamped filenames
- `logs/reconciliation_YYYY-MM-DD.log` - Contains detailed reconciliation events
- `logs/daily_reconciliation_YYYY-MM-DD.log` - Contains logs from scheduled checks

## Handling Failure Cases

If reconciliation fails for specific dates:

1. Check the logs for error messages
2. Use `npx tsx unified_reconciliation.ts critical DATE` for problematic dates
3. For specific period-farm combinations, use the spot-fix command

## Automating Reconciliation

To ensure consistent data integrity, set up the scheduled reconciliation script to run automatically.

### Using cron (Linux/macOS)

Add a daily scheduled task by editing your crontab:

```bash
crontab -e
```

Add this line to run reconciliation daily at 2:00 AM:

```
0 2 * * * cd /path/to/your/project && /usr/bin/env npx tsx scheduled_reconciliation.ts 7 >> /path/to/your/project/reconciliation_cron.log 2>&1
```

### Using Windows Task Scheduler

1. Create a batch file called `run_reconciliation.bat`:
   ```batch
   cd C:\path\to\your\project
   npx tsx scheduled_reconciliation.ts 7 >> reconciliation_scheduler.log 2>&1
   ```

2. Open Windows Task Scheduler and create a new task:
   - Set the trigger to run daily at 2:00 AM
   - Set the action to run your batch file

### Using GitHub Actions (for projects on GitHub)

Create a file `.github/workflows/reconciliation.yml`:

```yaml
name: Daily Reconciliation

on:
  schedule:
    - cron: '0 2 * * *'  # Run at 2:00 AM UTC daily

jobs:
  reconcile:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-node@v2
        with:
          node-version: '18'
      - run: npm install
      - run: npx tsx scheduled_reconciliation.ts 7
```