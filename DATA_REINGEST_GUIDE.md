# Data Reingestion and Correction Guide

This document provides guidelines and step-by-step procedures for correcting inaccuracies in settlement data within the system.

## When to Use Data Reingestion

Data reingestion or correction may be necessary in the following scenarios:

1. **Incomplete Data**: Missing settlement periods for a specific date
2. **Inaccurate Payment Values**: Payment amounts that don't match Elexon API values
3. **Inaccurate Energy Values**: Energy curtailment values that don't match Elexon API values
4. **BMU Mapping Issues**: Incorrect or missing lead party mappings affecting farm attribution
5. **Calculation Errors**: Issues with Bitcoin mining potential calculations

## Available Correction Scripts

### Complete Reingestion Scripts

These scripts perform a full reingestion by clearing all data for a specific date and fetching fresh data from the Elexon API:

- `complete_reingest_march_21.ts`: Template for full reingestion of all 48 settlement periods
- `data_reingest_reference.ts`: General-purpose reingestion template (recommended for new dates)

### Targeted Correction Scripts

These scripts perform targeted updates without complete reingestion:

- `update_march_21_payment.ts`: Template for correcting payment values only
- `update_march_21_energy_and_payment.ts`: Template for correcting energy and payment values
- `update_march_21_correct_energy.ts`: Template for final adjustments to match exact API values

### Verification Scripts

These scripts verify data integrity without making changes:

- `verify_dates.ts`: Checks data integrity for specific dates
- `verify_service.ts`: Verifies consistency between tables

## Step-by-Step Correction Process

### 1. Identify the Issue

Before making any corrections, identify the specific issues:

```sql
-- Check daily summary for a specific date
SELECT summary_date, total_curtailed_energy, total_payment 
FROM daily_summaries 
WHERE summary_date = 'YYYY-MM-DD';

-- Check if all 48 settlement periods exist
SELECT settlement_period, COUNT(*) as record_count
FROM curtailment_records
WHERE settlement_date = 'YYYY-MM-DD'
GROUP BY settlement_period
ORDER BY settlement_period;

-- Check Bitcoin calculations
SELECT settlement_date, miner_model, SUM(bitcoin_mined) as total_bitcoin
FROM historical_bitcoin_calculations
WHERE settlement_date = 'YYYY-MM-DD'
GROUP BY settlement_date, miner_model;
```

### 2. Select the Appropriate Correction Method

#### Complete Reingestion

Use when multiple settlement periods are missing or when many values need correction:

1. Create a new script based on `data_reingest_reference.ts`
2. Set the target date
3. Run the script to clear existing data and fetch fresh data
4. Verify the updates

#### Targeted Correction

Use when only specific values need correction:

1. Create a new script based on the appropriate template
2. Set the target date and correct values
3. Run the script to update specific fields
4. Verify the updates

### 3. Verify the Corrections

After making corrections, always verify:

1. Daily summary values
2. Monthly and yearly summary updates
3. Bitcoin mining calculations
4. Frontend data display

### 4. Document the Correction

Always document the correction process:

1. Create a summary document (e.g., `MARCH_21_REINGEST_SUMMARY.md`)
2. Include initial values, correction steps, and final values
3. Document verification steps and results

## Important Considerations

### Database Integrity

All correction scripts should maintain integrity across related tables:

- `daily_summaries`
- `monthly_summaries`
- `yearly_summaries`
- `historical_bitcoin_calculations`

### Logging and Error Handling

All correction scripts should include:

- Comprehensive logging to both console and files
- Error handling with appropriate recovery
- Verification steps to confirm successful updates

### Incremental Approach

For complex corrections:

1. Start with the most critical value (usually payment amount)
2. Proceed to energy values
3. Finish with any final adjustments to match exact API values

## Conclusion

Following these guidelines ensures accurate and consistent data throughout the system, maintaining the reliability of analytics, visualizations, and reports based on this data.

For specific implementation details, refer to the existing correction scripts and the `data_reingest_reference.ts` template.