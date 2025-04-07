# Fixing Incomplete Data

This document provides a guide for fixing incomplete or inconsistent data in the system. It explains the different scripts available and when to use each one.

## Available Scripts

### 1. `fix_data_for_march_27.ts`

This script is specifically designed to process data for March 27, 2025 without relying on DynamoDB for difficulty data.

**Usage:**
```bash
npx tsx fix_data_for_march_27.ts
```

**Features:**
- Uses a fixed difficulty value to avoid DynamoDB connection issues
- Handles Elexon API rate limiting with batch processing and delays
- Processes all 48 settlement periods for the specified date
- Updates all related Bitcoin calculations and summary tables
- Robust error handling and logging

**When to use:**
- When you need to specifically fix data for March 27, 2025
- When you're experiencing DynamoDB connectivity issues

### 2. `fix_incomplete_data.ts`

A general purpose script for fixing incomplete data for any date.

**Usage:**
```bash
npx tsx fix_incomplete_data.ts <DATE>
```

**Features:**
- Processes a specific date (format: YYYY-MM-DD)
- Updates curtailment records, Bitcoin calculations, and summary tables
- Uses DynamoDB for Bitcoin difficulty data

**When to use:**
- When you need to fix data for a specific date 
- When DynamoDB is accessible

### 3. `fix_incomplete_data_optimized.ts`

An optimized version of the incomplete data fixer.

**Usage:**
```bash
npx tsx fix_incomplete_data_optimized.ts <DATE>
```

**Features:**
- More efficient processing than the regular fix_incomplete_data.ts
- Processes all 48 periods in batches with efficient API usage
- Fetches difficulty data only once per date
- Handles all miner models in a single pass

**When to use:**
- When you need the most efficient data fixing for larger datasets
- When DynamoDB is accessible

### 4. `verify_and_fix_data.ts`

Combined verification and fixing utility.

**Usage:**
```bash
npx tsx verify_and_fix_data.ts <DATE> [action] [sampling-method]
```

**Features:**
- Verifies data against Elexon API before fixing
- Multiple sampling methods to efficiently check data
- Automatic fixing when verification fails
- Detailed logs of verification and repair

**When to use:**
- When you want to verify data before fixing
- When you want more control over the verification process

## Data Verification Process

Before fixing data, it's recommended to verify if the data actually needs fixing. The `verify_and_fix_data.ts` script automates this process.

Verification involves:
1. Checking if all 48 settlement periods have data
2. Comparing data with Elexon API to ensure accuracy
3. Checking consistency between related tables

## Common Data Integrity Issues

1. **Missing Periods**: Not all 48 settlement periods have data
2. **Inconsistent Data**: Curtailment records don't match Elexon API data
3. **Summary Discrepancies**: Summary tables don't reflect the actual data in the base tables
4. **DynamoDB Connection Issues**: Difficulty data is not available or accessible

## Troubleshooting

### DynamoDB Connection Issues

If you're experiencing problems with DynamoDB connections:

1. Use `fix_data_for_march_27.ts` as a template and modify it for your specific date
2. The script uses a fixed difficulty value (71e12) to avoid DynamoDB dependency
3. Adjust the `DATE_TO_PROCESS` constant to target a different date

### API Rate Limiting

If you're hitting Elexon API rate limits:

1. Increase the `BATCH_DELAY_MS` constant to add more delay between batches
2. Decrease the `BATCH_SIZE` constant to process fewer periods in parallel

### Database Consistency

To ensure consistency between tables:

1. Always run the complete process (curtailment > Bitcoin > summaries)
2. After fixing, verify data with `verify_and_fix_data.ts` to ensure consistency