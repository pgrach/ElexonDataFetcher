# Bitcoin Calculation Reconciliation Tools

This collection of tools is designed to identify and fix missing or incomplete Bitcoin calculations derived from wind farm curtailment data.

## Problem Statement

Our system tracks wind farm curtailment records and uses this data to calculate potential Bitcoin mining outcomes using different miner models. The analysis identified several time periods with missing or incomplete Bitcoin calculations, including:

- **Missing Data (No Bitcoin Calculations)**: 
  - 2022-04, 2022-06 through 2022-11
  - All of 2023 (January through December)

- **Incomplete Data (Partial Bitcoin Calculations)**:
  - 2022-01 through 2022-03, 2022-05, 2022-12
  - 2024-09, 2024-12
  - 2025-01, 2025-02

## Solution

The tools in this repository provide a comprehensive solution to reconcile and fix all missing Bitcoin calculations:

### Tools Overview

1. **fix_all_bitcoin_calculations.ts**
   - Main utility that processes all missing or incomplete calculations
   - Handles batching, concurrency, and progress tracking
   - Can be used to fix individual dates or full datasets

2. **reconcile_missing_calculations.ts**
   - Targets specific time periods identified in our analysis
   - Uses the main fix script for actual processing
   - Provides verification before and after fixes

3. **verify_bitcoin_calculations.ts**
   - Generates detailed reports on calculation completeness
   - Identifies any remaining issues after fixes
   - Provides month-by-month and day-by-day analysis

4. **run_bitcoin_calculation_fix.ts**
   - Orchestrates the entire reconciliation process
   - Runs verification → fixes → final verification
   - Creates a summary of completed work

## Usage

To run the complete reconciliation process:

```bash
tsx run_bitcoin_calculation_fix.ts
```

For verification only:

```bash
tsx verify_bitcoin_calculations.ts
```

To fix specific time periods:

```bash
tsx reconcile_missing_calculations.ts
```

To run the comprehensive fix directly:

```bash
tsx fix_all_bitcoin_calculations.ts
```

## How It Works

1. The tools analyze the database to identify which curtailment records are missing corresponding Bitcoin calculations
2. Missing calculations are prioritized based on completeness and chronology
3. Calculations are processed in batches with limited concurrency to avoid overwhelming the system
4. Progress is tracked and can be resumed if interrupted
5. Final verification ensures all calculations are properly completed

## Expected Outcomes

After running these tools:

- All curtailment records should have corresponding Bitcoin calculations for all three miner models
- Database tables will be properly reconciled
- Reports will show 100% completion across all time periods

## Monitoring

The scripts generate detailed logs and reports that show:

- Overall progress and completion percentages
- Any failures or issues encountered
- Time taken for each operation
- Final verification results