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

1. **Regular Checks**: Run `npx tsx comprehensive_reconciliation.ts status` daily to monitor reconciliation status.

2. **Incremental Updates**: Use `reconcile-recent` to ensure recent data is always reconciled.

3. **Reporting**: Generate monthly reports with `npx tsx comprehensive_reconciliation.ts report` to track progress.

4. **Troubleshooting**: If specific dates are problematic, use the spot-fix feature of unified_reconciliation to target specific combinations.

## Understanding the Reconciliation Process

The reconciliation process ensures that for every curtailment record, there are corresponding Bitcoin calculations for each miner model. The process:

1. Identifies dates with missing or incomplete calculations
2. Processes each date to generate missing calculations
3. Verifies the completeness of the reconciliation

The relationship is:
- Each curtailment record (date-period-farm) should have
- One calculation for each miner model (S19J_PRO, S9, M20S)

## Log Files

The reconciliation tools generate detailed logs:
- `reconciliation.log` - Logs from unified_reconciliation.ts
- `comprehensive_reconciliation.log` - Logs from comprehensive_reconciliation.ts

## Handling Failure Cases

If reconciliation fails for specific dates:

1. Check the logs for error messages
2. Use `npx tsx unified_reconciliation.ts critical DATE` for problematic dates
3. For specific period-farm combinations, use the spot-fix command