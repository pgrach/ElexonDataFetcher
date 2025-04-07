# Data Reingestion Guide

## Overview
This guide outlines the process for reingesting settlement data to ensure correct and complete representation in our database. Use this guide when discrepancies are discovered between expected payment amounts or when data appears to be missing or incomplete.

## When to Reingest Data

Consider reingestion when:
1. Payment amounts differ significantly from expected values 
2. Settlement periods are missing from the database
3. API data has been updated or corrected since initial ingestion
4. Bitcoin mining calculations need to be recalculated with updated parameters

## Reingestion Workflow

### 1. Verify Current Data Status
First, analyze the current data to understand the extent of the issue:

```sql
-- Check for completeness of periods
SELECT 
  COUNT(DISTINCT settlement_period) as period_count,
  MIN(settlement_period) as min_period,
  MAX(settlement_period) as max_period
FROM curtailment_records
WHERE settlement_date = 'YYYY-MM-DD';

-- Check for total volumes and payments
SELECT 
  COUNT(*) as record_count,
  SUM(volume) as total_volume,
  SUM(payment) as total_payment
FROM curtailment_records
WHERE settlement_date = 'YYYY-MM-DD';

-- Identify specific missing periods
WITH all_periods AS (
  SELECT generate_series(1, 48) as period
)
SELECT a.period
FROM all_periods a
LEFT JOIN (
  SELECT DISTINCT settlement_period
  FROM curtailment_records
  WHERE settlement_date = 'YYYY-MM-DD'
) c ON a.period = c.settlement_period
WHERE c.settlement_period IS NULL
ORDER BY a.period;
```

### 2. Choose the Appropriate Reingestion Strategy

#### Strategy 1: Complete Reingestion
Use this when many periods are missing or when the data is significantly corrupted.

```bash
# Step 1: Create a script based on complete_reingest_march_XX.ts
# Step 2: Run the reingestion
npx tsx complete_reingest_YYYY_MM_DD.ts
```

#### Strategy 2: Targeted Period Reingestion
Use this when only specific periods are missing or problematic.

```bash
# Step 1: Create a script based on fix_march_XX_last_periods.ts
# Step 2: Set START_PERIOD and END_PERIOD to target the specific ranges
# Step 3: Run the targeted reingestion
npx tsx fix_YYYY_MM_DD_periods.ts
```

#### Strategy 3: Staged Reingestion
Use this for incremental reingestion of large datasets to avoid timeouts.

```bash
# Step 1: Create a script based on staged_reingest_march_XX.ts
# Step 2: Run the staged reingestion, processing periods in batches
npx tsx staged_reingest_YYYY_MM_DD.ts
```

### 3. Update Summary Tables
After reingestion, ensure that all summary tables are updated:

```bash
npx tsx update_summaries.ts
# Or create a specific script for the targeted date
npx tsx update_YYYY_MM_DD_summaries.ts
```

### 4. Verify Results
Validate that the reingestion was successful:

```sql
-- Check for completeness
SELECT 
  COUNT(*) as record_count,
  COUNT(DISTINCT settlement_period) as period_count,
  SUM(volume) as total_volume,
  SUM(payment) as total_payment
FROM curtailment_records
WHERE settlement_date = 'YYYY-MM-DD';

-- Verify Bitcoin calculations
SELECT
  SUM(bitcoin_mined) as total_bitcoin
FROM historical_bitcoin_calculations
WHERE settlement_date = 'YYYY-MM-DD';
```

## Script Templates

### Template 1: Complete Reingestion

Key components:
- Clear all existing data for the date
- Process all 48 settlement periods
- Update summary tables and Bitcoin calculations

### Template 2: Targeted Period Fix

Key components:
- Identify specific missing or problematic periods
- Clear only the affected period data
- Reingest only those specific periods
- Update summary tables based on the complete dataset

### Template 3: Data Verification

Key components:
- Compare local database records with API data
- Identify discrepancies in record counts, volumes, or payments
- Generate a report of problematic periods

## Common Issues and Solutions

### API Rate Limiting
- Implement throttling between API calls (using delay function)
- Process in smaller batches with pauses between batches

### Database Schema Changes
- Check column names and data types before writing scripts
- Adapt insert statements to match current schema
- Use database introspection to verify schema at runtime

### Missing BMU Mappings
- Implement hardcoded mappings for common farm IDs
- Skip records with unknown BMU IDs
- Consider adding a mapping table for future use

### Bitcoin Calculation Adjustments
- Use consistent difficulty value across all calculations
- Consider adding mining model comparison for optimization studies

## Recent Example Dates
- March 21, 2025: Complete reingestion successful (50,518.72 MWh, £1,240,439.58)
- March 22, 2025: Missing settlement periods 47-48
- March 28, 2025: Expected total payment of £3,784,089.62