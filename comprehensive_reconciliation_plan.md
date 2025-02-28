# Comprehensive Reconciliation Plan

## Overview
This document outlines the comprehensive plan for reconciling curtailment records with Bitcoin calculations. The goal is to ensure 100% reconciliation between the two data sets, with accurate Bitcoin mining calculations for each curtailment record across all miner models.

## Current Status
- Reconciliation rate: 65.02% (984,547 calculations out of 1,514,223 expected)
- Three miner models tracked: S19J_PRO (328,189 calculations), S9 (328,179 calculations), M20S (328,179 calculations)
- Missing calculations primarily in December 2023

## Consolidated Toolset

### 1. Main Reconciliation Tool (`reconciliation.ts`)
The primary tool for all reconciliation operations.
```bash
# Check current reconciliation status
npx tsx reconciliation.ts status

# Find dates with missing calculations
npx tsx reconciliation.ts find

# Fix all missing calculations
npx tsx reconciliation.ts reconcile

# Fix a specific date
npx tsx reconciliation.ts date YYYY-MM-DD
```

### 2. Quick Status Check (`check_reconciliation_status.ts`)
A simplified tool to quickly check the current reconciliation status.
```bash
npx tsx check_reconciliation_status.ts
```

### 3. December 2023 Focus Tool (`fix_december_2023.ts`)
A specialized tool for targeting the December 2023 reconciliation issues.
```bash
# Check December 2023 status
npx tsx fix_december_2023.ts status

# Fix December 2023 in batches (default batch size: 5)
npx tsx fix_december_2023.ts fix

# Fix with custom batch size
npx tsx fix_december_2023.ts fix 10
```

### 4. Single Date Testing (`test_reconcile_date.ts`)
A test tool for verifying reconciliation on a specific date.
```bash
# Edit the date in the file first, then run:
npx tsx test_reconcile_date.ts
```

### 5. SQL Queries (`reconciliation.sql`)
Consolidated SQL queries for database-level analysis and verification.

## Automated Reconciliation Processes

### 1. Daily Reconciliation
- **Automated Process**: The system runs daily to reconcile the previous day's curtailment records with Bitcoin calculations
- **Implementation**: Uses `historicalReconciliation.reconcileDay()` function
- **Monitoring**: Daily status is logged and can be checked via the reconciliation tool

### 2. Monthly Reconciliation
- **Scheduled Process**: Runs on the 1st of each month to ensure complete reconciliation of the previous month
- **Implementation**: Uses `historicalReconciliation.reconcilePreviousMonth()` function
- **Verification**: Creates monthly summary records for quick verification

## Data Integrity Checks

### Period-Farm Combinations
- Each unique period-farm combination should have exactly one calculation per miner model
- SQL checks ensure no duplicates and complete coverage

### Miner Models
- S19J_PRO, S9, and M20S models must all be calculated for each curtailment record
- Difficulty data is obtained from AWS DynamoDB for accurate calculations

### Database Schema
- `curtailment_records` - Contains curtailment data from Elexon API
- `historical_bitcoin_calculations` - Contains Bitcoin mining calculations for each curtailment record and miner model

## December 2023 Recovery Plan

1. Run the specialized December tool to identify gaps:
   ```bash
   npx tsx fix_december_2023.ts status
   ```

2. Process batches of dates with missing calculations:
   ```bash
   npx tsx fix_december_2023.ts fix
   ```

3. Repeat batch processing until reconciliation is complete

4. Verify final state:
   ```bash
   npx tsx reconciliation.ts status
   ```

## Maintaining Reconciliation

### Prevention Measures
- Improved error handling in API data ingestion
- Retry logic for difficulty data retrieval
- Transaction-based processing to prevent partial updates

### Monitoring
- Daily reconciliation status check via automated processes
- Weekly comprehensive audit using the provided tools
- Monthly detailed reconciliation report

## Conclusion
This comprehensive plan and consolidated toolset ensure a robust approach to maintaining 100% reconciliation between curtailment records and Bitcoin calculations, providing accurate insights into mining potential under various scenarios.