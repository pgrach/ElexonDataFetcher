# Bitcoin Reconciliation System

This document outlines the comprehensive reconciliation system that ensures 100% consistency between curtailment records (primary source of truth) and Bitcoin calculations.

## Overview

The reconciliation system ensures that for each unique combination of `(settlement_date, settlement_period, farm_id)` in the `curtailment_records` table, there are exactly three corresponding records in the `historical_bitcoin_calculations` table - one for each miner model (S19J_PRO, S9, M20S).

## Key Components

### 1. Daily Automated Checks

The system automatically runs daily reconciliation checks as part of the data update process:

```typescript
// From server/services/dataUpdater.ts
async function updateLatestData() {
  // Process current date data
  // ...
  
  // Ensure Bitcoin calculations are up-to-date
  await reconcileDay(today);
  
  // Verification check
  const verificationResult = await getVerificationSummary(today);
  console.log(`Verification Check for ${today}: ${JSON.stringify(verificationResult)}`);
}
```

### 2. Core Reconciliation Tools

Three main tools are available:

1. **Daily Check**: Automatic verification for the current day
   ```bash
   npx tsx daily_reconciliation_check.ts
   ```

2. **Comprehensive Check**: Checks and fixes reconciliation for any date range
   ```bash
   npx tsx comprehensive_reconcile.ts check-date YYYY-MM-DD
   npx tsx comprehensive_reconcile.ts fix-date YYYY-MM-DD
   npx tsx comprehensive_reconcile.ts fix-range YYYY-MM-DD YYYY-MM-DD
   ```

3. **Accelerated Reconciliation**: High-performance parallel processing for historical data
   ```bash
   npx tsx accelerated_reconcile.ts
   ```

### 3. Reconciliation Service

The core reconciliation function `reconcileDay` in `server/services/historicalReconciliation.ts`:

```typescript
export async function reconcileDay(date: string): Promise<void> {
  // Get all curtailment records for the specified date
  // For each unique period-farm combination:
  //   Calculate Bitcoin for each miner model
  //   Insert or update calculations in historical_bitcoin_calculations
  // Verify all combinations have been processed
}
```

## Performance Optimized Reconciliation

The accelerated reconciliation system can process all historical data in 4-8 hours using:

1. Optimized database indices and query planning
2. Massive parallel processing (multiple worker processes)
3. Efficient batching and bulk operations
4. Dynamic resource management to prevent system overload

## Monitoring and Reporting

The system provides real-time progress tracking:

```bash
# Check current reconciliation status
npx tsx reconciliation.ts status

# Get comprehensive report
npx tsx comprehensive_reconcile.ts report
```

## Reconciliation Rules

1. Each curtailment record must have Bitcoin calculations for all three miner models
2. Calculations must use the correct Bitcoin network difficulty for the date
3. Reconciliation is complete only when 100% of records have calculations
4. Settlement date/period and farm ID must match exactly between tables

## Troubleshooting

If discrepancies are detected:

1. Run comprehensive check: `npx tsx comprehensive_reconcile.ts check-date YYYY-MM-DD`
2. Fix specific date: `npx tsx comprehensive_reconcile.ts fix-date YYYY-MM-DD`
3. Check logs for any calculation errors
4. Verify Bitcoin difficulty data in DynamoDB for the date
5. For persistent issues, run deep verification: `npx tsx test_reconcile_date.ts`