# March 22, 2025 Data Reingestion Summary

## Overview
This document summarizes the reingestion process for March 22, 2025 settlement data. The purpose of this reingestion was to ensure complete and accurate data representation, particularly for the last four settlement periods (45-48), which were either missing or incomplete in the database.

## Current State (Before Reingestion)
- Settlement Periods: 46/48 (periods 47-48 completely missing)
- Total Records: 898
- Total Volume: -25,525.77 MWh
- Total Payment: £63,809.23
- Bitcoin Mined: 37.70 BTC

### Period 45-46 Details
- Period 45: 12 records, -253.15 MWh, £113.92
- Period 46: 9 records, -186.31 MWh, £83.84

## Reingestion Scripts
For this reingestion process, we created several specialized scripts:

1. `check_march_22_periods.ts` - Analyzes existing data for March 22, 2025 to identify missing periods
2. `verify_and_fix_march_22.ts` - Compares curtailment records with Elexon API data to identify discrepancies
3. `clear_march_22_data.ts` - Completely removes all data for March 22, 2025 to prepare for full reingestion
4. `fix_march_22_last_periods.ts` - Specifically targets and reingests periods 45-48
5. `staged_reingest_march_22.ts` - Allows for batch processing periods in smaller ranges
6. `complete_reingest_march_22.ts` - Performs a complete reingestion of all 48 settlement periods
7. `update_march_22_summaries.ts` - Updates all summary tables after reingestion

## Expected Results (After Reingestion)
- Settlement Periods: 48/48 (all periods complete)
- Total Volume: Approximately -36,000 MWh (final value to be confirmed)
- Total Payment: Approximately £880,000 (EXPECTED_TOTAL_PAYMENT)
- Bitcoin Mined: Approximately 54 BTC (final value to be confirmed)

## Technical Notes
1. The script adapts to the actual database schema with the following key fields:
   - `curtailment_records`: settlement_date, settlement_period, volume, payment, farm_id, lead_party_name
   - `historical_bitcoin_calculations`: settlement_date, settlement_period, farm_id, bitcoin_mined, difficulty

2. Hardcoded BMU mappings were implemented due to the absence of a dedicated mapping table, with focus on Seagreen Wind Energy Limited farms:
   - T_SGRWO-1, T_SGRWO-2, T_SGRWO-3, T_SGRWO-4

3. Bitcoin calculations use standardized parameters:
   - Difficulty: 81.72 T (representative for early 2025)
   - BTC Price: £75,000 (representative for early 2025)
   - Mining Efficiency: 90%
   - Miner Model: S19J_PRO

## Instructions for Use
1. For a quick fix of just the missing periods (47-48):
   ```
   npx tsx fix_march_22_last_periods_new.ts
   ```

2. For reingesting specific period ranges:
   ```
   # Edit START_PERIOD and END_PERIOD in staged_reingest_march_22.ts
   npx tsx staged_reingest_march_22.ts
   ```

3. For complete reingestion of the entire day:
   ```
   npx tsx complete_reingest_march_22.ts
   ```

4. To update summaries after reingestion:
   ```
   npx tsx update_march_22_summaries.ts
   ```

## Validation
After reingestion, verify completeness with:
```sql
SELECT 
  COUNT(*) as record_count,
  COUNT(DISTINCT settlement_period) as period_count,
  SUM(volume) as total_volume,
  SUM(payment) as total_payment
FROM curtailment_records
WHERE settlement_date = '2025-03-22'
```

To check for missing periods:
```sql
SELECT 
  settlement_period, 
  COUNT(*) as record_count
FROM curtailment_records
WHERE settlement_date = '2025-03-22'
GROUP BY settlement_period
ORDER BY settlement_period
```