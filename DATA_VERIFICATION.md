# Wind Curtailment Data Verification Guide

This guide provides instructions for verifying and fixing data integrity issues in the wind curtailment data processing pipeline. It covers common problems, verification methods, and step-by-step fixes for incomplete or inconsistent data.

## Common Data Integrity Issues

1. **BMU Mapping Inconsistency**
   - Root cause: Two different BMU mapping files exist in the system:
     - Root mapping file (`data/bmu_mapping.json`): 32 entries
     - Server mapping file (`server/data/bmuMapping.json`): 208 entries
   - Impact: The curtailment processor uses the server mapping for validation but the Elexon API integration uses the root mapping, leading to rejected records

2. **Missing Curtailment Records**
   - Symptoms: Daily summaries exist but underlying curtailment records are missing
   - Verification: Run `SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = 'YYYY-MM-DD'`

3. **Payment Calculation Issues**
   - Symptoms: NaN or incorrect payment values in records
   - Verification: Check if payment values match volume * price

4. **Incomplete Processing Due to Timeouts**
   - Symptoms: Only some settlement periods are processed for a given date
   - Verification: Run `SELECT COUNT(*), settlement_period FROM curtailment_records WHERE settlement_date = 'YYYY-MM-DD' GROUP BY settlement_period` 

## Verification Process

### Step 1: Check Daily Summary Existence

```sql
-- Check if a daily summary exists for the date
SELECT * FROM daily_summaries WHERE summary_date = 'YYYY-MM-DD';
```

### Step 2: Verify Curtailment Records

```sql
-- Count curtailment records for the date
SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = 'YYYY-MM-DD';

-- Check periods coverage (should be close to 48 periods)
SELECT COUNT(DISTINCT settlement_period) FROM curtailment_records WHERE settlement_date = 'YYYY-MM-DD';

-- Check period distribution
SELECT settlement_period, COUNT(*) 
FROM curtailment_records 
WHERE settlement_date = 'YYYY-MM-DD' 
GROUP BY settlement_period 
ORDER BY settlement_period;
```

### Step 3: Verify Data Consistency

```sql
-- Compare daily summary totals with sum of curtailment records
SELECT 
    (SELECT total_curtailed_energy FROM daily_summaries WHERE summary_date = 'YYYY-MM-DD') AS summary_energy,
    (SELECT total_payment FROM daily_summaries WHERE summary_date = 'YYYY-MM-DD') AS summary_payment,
    (SELECT SUM(ABS(volume::numeric)) FROM curtailment_records WHERE settlement_date = 'YYYY-MM-DD') AS records_energy,
    (SELECT SUM(payment::numeric) FROM curtailment_records WHERE settlement_date = 'YYYY-MM-DD') AS records_payment;
```

### Step 4: Verify Bitcoin Calculations

```sql
-- Check if Bitcoin calculations exist for date
SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE calculation_date = 'YYYY-MM-DD';

-- Check calculations by miner model
SELECT miner_model, SUM(bitcoin_mined::numeric) AS total_bitcoin 
FROM historical_bitcoin_calculations 
WHERE calculation_date = 'YYYY-MM-DD' 
GROUP BY miner_model;
```

## Fixing Data Issues

### Method 1: Using the check_elexon_data.ts Script

This script checks the database against Elexon API data to identify discrepancies:

```bash
# Check with progressive sampling (starts with key periods, expands if issues found)
npx tsx check_elexon_data.ts YYYY-MM-DD

# Check specific sampling methods
npx tsx check_elexon_data.ts YYYY-MM-DD random  # Checks 10 random periods
npx tsx check_elexon_data.ts YYYY-MM-DD fixed   # Checks 5 key periods (1, 12, 24, 36, 48)
npx tsx check_elexon_data.ts YYYY-MM-DD full    # Checks all 48 periods (may hit API limits)
```

### Method 2: Using the BMU Mapping Fix Scripts

For issues related to BMU mapping inconsistency:

```bash
# Check a few key periods (fastest, less likely to timeout)
npx tsx fix_bmu_mapping_minimal.ts YYYY-MM-DD

# Process all 48 periods with improved BMU mapping 
npx tsx fix_bmu_mapping.ts YYYY-MM-DD
```

### Method 3: Using the Verification and Repair Utility

For comprehensive verification with automatic fixing:

```bash
# Verify data without fixing
npx tsx verify_and_fix_data.ts YYYY-MM-DD verify

# Verify and automatically fix if issues are found
npx tsx verify_and_fix_data.ts YYYY-MM-DD fix

# Force a complete reprocessing of the date
npx tsx verify_and_fix_data.ts YYYY-MM-DD force-fix
```

## Recalculating Derived Data

After fixing curtailment records, you'll need to update all dependent calculations:

```bash
# Process Bitcoin calculations with optimized DynamoDB access
npx tsx process_bitcoin_optimized.ts YYYY-MM-DD

# Update full cascade (Bitcoin, monthly, yearly summaries)
npx tsx process_complete_cascade.ts YYYY-MM-DD
```

## Common Command Patterns for Data Recovery

### Example: Complete Data Recovery for March 28, 2025

```bash
# 1. Verify the data integrity first
npx tsx check_elexon_data.ts 2025-03-28

# 2. If issues found, fix the curtailment records
npx tsx fix_bmu_mapping.ts 2025-03-28

# 3. Recalculate Bitcoin mining potential
npx tsx process_bitcoin_optimized.ts 2025-03-28

# 4. Update all summary tables
npx tsx process_complete_cascade.ts 2025-03-28
```

## Preventative Measures

1. **Consistent BMU Mapping**
   - Always use the server mapping file (208 entries) which is more comprehensive
   - Use the mapping provider in `server/services/elexon.ts` rather than loading the file directly

2. **Rate Limiting Awareness**
   - Process in small batches (3-4 periods at a time)
   - Use longer delays between API calls (500ms minimum)
   - Implement exponential backoff for retries

3. **Regular Verification**
   - Run the verification script weekly to catch data anomalies early
   - Monitor daily summaries for unexpected values or missing data