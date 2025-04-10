# April 1, 2025 Bitcoin Calculation Fix

## Issue Summary
On April 10, 2025, we identified a significant discrepancy in the Bitcoin mining potential calculations for April 1, 2025. The system had only processed 9-10 out of 544 curtailment records for the S19J_PRO miner model, resulting in vastly underreported Bitcoin mining potential.

## Problem Details
- **Date Affected**: April 1, 2025
- **Primary Issue**: Only 9-10 curtailment records (out of 544) were processed for Bitcoin calculations
- **Impact**: The daily Bitcoin total for S19J_PRO was reported as 0.32 BTC instead of 22.45 BTC
- **Root Cause**: Duplicate curtailment records with the same settlement_period and farm_id combinations were causing unique constraint violations during calculation

## Pre-Fix State
- **S19J_PRO Daily Total**: 0.32 BTC (9-10 records processed)
- **Monthly Total for April**: 7.21 BTC (including other days in April)
- **Total Energy Curtailed**: 14,871.88 MWh (not fully utilized for calculations)

## Fix Implementation
1. Created an aggregation-based approach to handle duplicate curtailment records
2. First aggregated energy by settlement_period and farm_id to avoid constraint violations
3. Recalculated Bitcoin for all 292 unique period-farm combinations
4. Used the correct difficulty value (113757508810853) for calculations
5. Updated daily and monthly summaries with corrected values

## Scripts Created
1. `fix_april1_2025_all_records.ts` - Initial fix attempt (encountered constraint issues)
2. `fix_april1_2025_direct_sql.ts` - SQL-based fix attempt (encountered constraint issues)
3. `fix_april1_2025_s19j_pro.ts` - Focused fix for S19J_PRO model
4. `fix_april1_2025_aggregated.ts` - **Successful fix** using energy aggregation
5. `update_april_2025_monthly_summary.ts` - Helper script to update monthly summaries

## Post-Fix State
- **S19J_PRO Daily Total**: 22.45 BTC (292 unique records processed)
- **Monthly Total for April**: 22.45 BTC for April 1 (S19J_PRO)
- **Monthly Total for April 2025**: Updated to show correct aggregated values

## Verification Process
1. Checked record counts and Bitcoin totals in `historical_bitcoin_calculations`
2. Verified daily summary was updated with correct totals
3. Confirmed monthly summary was updated with correct values
4. Validated settlement period-farm level calculations for sample periods

## Lessons Learned
1. When processing curtailment data, we must account for potential duplicate records with the same period-farm combinations
2. Aggregating energy values by period-farm before Bitcoin calculation prevents constraint violations
3. Running dedicated verification queries helps ensure data integrity after fixes
4. Implementing logging in fix scripts creates an audit trail of changes

## Future Improvements
1. Add pre-aggregation step to regular Bitcoin calculation process
2. Enhance validation checks to detect partial processing issues
3. Create automated alerts when the number of Bitcoin calculations differs significantly from curtailment record count
4. Implement process-level logging for better visibility into calculation pipeline