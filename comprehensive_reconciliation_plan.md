# Comprehensive Reconciliation Plan

## Overview
This document outlines the comprehensive plan for reconciling curtailment records with Bitcoin calculations. The goal is to ensure 100% reconciliation between the two data sets, with accurate Bitcoin mining calculations for each curtailment record across all miner models.

## Current Status
- Reconciliation rate: 65.02% (984,547 calculations out of 1,514,223 expected)
- Three miner models tracked: S19J_PRO (328,189 calculations), S9 (328,179 calculations), M20S (328,179 calculations)
- Missing calculations primarily in December 2023

## Reconciliation Process

### 1. Daily Reconciliation
- **Automated Process**: The system runs daily to reconcile the previous day's curtailment records with Bitcoin calculations
- **Implementation**: Uses `historicalReconciliation.reconcileDay()` function
- **Monitoring**: Daily status is logged and can be checked via the reconciliation tool

### 2. Monthly Reconciliation
- **Scheduled Process**: Runs on the 1st of each month to ensure complete reconciliation of the previous month
- **Implementation**: Uses `historicalReconciliation.reconcilePreviousMonth()` function
- **Verification**: Creates monthly summary records for quick verification

### 3. Manual Reconciliation
- **Reconciliation Tool**: Use the consolidated tool via `npx tsx reconciliation.ts`
- **Commands**:
  - `npx tsx reconciliation.ts status` - Check current reconciliation status
  - `npx tsx reconciliation.ts find` - Find dates with missing calculations
  - `npx tsx reconciliation.ts reconcile` - Fix all missing calculations
  - `npx tsx reconciliation.ts date YYYY-MM-DD` - Fix a specific date

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

## Recovery Plan for December 2023

1. Identify specific missing date-period-farm combinations
2. Process each missing combination using the reconciliation tool
3. Verify results with SQL validation queries
4. Create monthly summary records once full reconciliation is achieved

## Maintaining Reconciliation

### Prevention Measures
- Improved error handling in API data ingestion
- Retry logic for difficulty data retrieval
- Transaction-based processing to prevent partial updates

### Monitoring
- Daily reconciliation status check
- Weekly comprehensive audit
- Monthly detailed reconciliation report

## Conclusion
This plan ensures a robust approach to maintaining 100% reconciliation between curtailment records and Bitcoin calculations, providing accurate insights into mining potential under various scenarios.