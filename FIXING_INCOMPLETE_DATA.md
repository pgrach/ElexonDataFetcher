# Fixing Incomplete Data

This document explains how to fix incomplete data for specific dates in the database.

## Background

The system relies on properly ingested curtailment data from the Elexon API, which is then used to calculate Bitcoin mining potential. If data for a specific date is incomplete or missing, it can lead to incorrect calculations and summaries throughout the system.

## Verification

To check if a date has incomplete data, use the `check_elexon_data.ts` script:

```bash
# Check with progressive sampling (starts with key periods, expands if issues found)
npx tsx check_elexon_data.ts 2025-03-25

# Check with fixed sampling (periods 1, 12, 24, 36, 48)
npx tsx check_elexon_data.ts 2025-03-25 fixed

# Check with random sampling (10 random periods)
npx tsx check_elexon_data.ts 2025-03-25 random

# Check all periods (warning: may hit API limits)
npx tsx check_elexon_data.ts 2025-03-25 full
```

## Fixing Incomplete Data

### Option 1: Optimized All-in-One Fix (Recommended)

To fully process a date with missing or incomplete data, use the optimized script that ensures all 48 periods are processed and DynamoDB is accessed only once:

```bash
npx tsx fix_incomplete_data_optimized.ts 2025-03-25
```

This optimized script will:
1. Process all 48 settlement periods in small batches to avoid API limits
2. Calculate Bitcoin mining for all miner models with a single DynamoDB fetch
3. Update all summaries (daily, monthly, yearly) automatically

### Option 2: Standard All-in-One Fix

The standard fix script processes data in the original way:

```bash
npx tsx fix_incomplete_data.ts 2025-03-25
```

This script will:
1. Process curtailment records for the date
2. Process Bitcoin calculations for each miner model (S19J_PRO, S9, M20S) separately
3. Update monthly summaries
4. Update yearly summaries

### Option 3: Optimized Step-by-Step Fix

If you prefer to fix the data step by step using the optimized components:

#### Step 1: Process All Curtailment Periods

```bash
npx tsx process_all_periods.ts 2025-03-25
```

#### Step 2: Process All Bitcoin Calculations with Single DynamoDB Access

```bash
npx tsx process_bitcoin_optimized.ts 2025-03-25
```

#### Step 3: Complete Cascade (if you want to run steps 1-2 in sequence)

```bash
npx tsx process_complete_cascade.ts 2025-03-25
```

### Option 4: Standard Step-by-Step Fix

If you prefer to use the original approach:

#### Step 1: Process Curtailment Data

```bash
npx tsx process_curtailment.ts 2025-03-25
```

#### Step 2: Process Bitcoin Calculations for Each Model

```bash
npx tsx process_bitcoin.ts 2025-03-25 S19J_PRO
npx tsx process_bitcoin.ts 2025-03-25 S9
npx tsx process_bitcoin.ts 2025-03-25 M20S
```

#### Step 3: Update Monthly Summaries

```bash
npx tsx process_monthly.ts 2025-03
```

#### Step 4: Update Yearly Summaries

```bash
npx tsx process_yearly.ts 2025
```

## Verifying the Fix

After the fix, run the verification script again to make sure all issues are resolved:

```bash
npx tsx check_elexon_data.ts 2025-03-25
```

## Sample Commands for March 25, 2025

Here are the exact commands to fix the data for March 25, 2025 using the optimized approach:

```bash
# Optimized all-in-one fix (recommended)
npx tsx fix_incomplete_data_optimized.ts 2025-03-25

# Or optimized step-by-step:
npx tsx process_all_periods.ts 2025-03-25
npx tsx process_bitcoin_optimized.ts 2025-03-25
```

If you prefer the standard approach:

```bash
# Standard all-in-one fix
npx tsx fix_incomplete_data.ts 2025-03-25

# Or standard step-by-step:
npx tsx process_curtailment.ts 2025-03-25
npx tsx process_bitcoin.ts 2025-03-25 S19J_PRO
npx tsx process_bitcoin.ts 2025-03-25 S9
npx tsx process_bitcoin.ts 2025-03-25 M20S
npx tsx process_monthly.ts 2025-03
npx tsx process_yearly.ts 2025
```

## Optimizations

The optimized scripts introduce important improvements:

1. **Complete Period Coverage**: The `process_all_periods.ts` script ensures all 48 settlement periods are processed in small batches to avoid API rate limits.

2. **Efficient DynamoDB Access**: The `process_bitcoin_optimized.ts` script fetches difficulty data from DynamoDB only once per date instead of multiple times for each miner model and settlement period.

3. **Parallel Processing**: The optimized scripts use batched processing to handle multiple operations concurrently.

4. **Enhanced Error Handling**: Robust retry logic and better error reporting in the optimized scripts.

## Troubleshooting

If you encounter any issues during the data fix process:

1. **API Rate Limiting**: The Elexon API has rate limits. If you hit these limits, wait a few minutes before trying again.
2. **Database Errors**: Ensure your database connection is properly configured.
3. **Validation Errors**: Check the logs for specific validation errors that might indicate issues with the data.
4. **Missing Modules**: Ensure all required dependencies are installed.

If the fix process fails, you can retry individual steps using the step-by-step approach.