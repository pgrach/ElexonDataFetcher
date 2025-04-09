# Bitcoin Calculation Fix Documentation

## Background

During March 2025, we identified inconsistencies in Bitcoin calculations for the following dates:
- March 04, 2025
- March 20, 2025
- March 28, 2025
- March 31, 2025

The primary issue was incorrect difficulty values in the `historical_bitcoin_calculations` table, which led to incorrect Bitcoin mining estimates.

## Corrected Difficulty Values

The correct difficulty values for March 2025 are:
- March 01-09: 110568428300952
- March 10-22: 112149504190349
- March 23-31: 113757508810853

## March 31 Calculation Anomaly

A specific issue was identified with March 31, 2025 data where similar energy inputs were producing vastly different Bitcoin outputs compared to earlier dates:
- March 13: 6.3 GWh produced 4.8 BTC (with difficulty 112149504190349)
- March 31: 6.1 GWh initially produced only 0.02 BTC (with difficulty 113757508810853)

Investigation revealed that while the difficulty values had been updated by earlier scripts, the Bitcoin amounts had not been recalculated. This was fixed by processing:
1. Each individual miner model (S19J_PRO, M20S, S9)
2. Re-running the Bitcoin calculations with the correct difficulty values
3. Updating daily, monthly, and yearly summaries

## Fix Scripts Created

The following scripts were created to fix these issues:

1. `fix-march-31-S19J_PRO.ts` - Fixes Bitcoin calculations for S19J_PRO miners on March 31
2. `fix-march-31-M20S.ts` - Fixes Bitcoin calculations for M20S miners on March 31
3. `fix-march-31-S9.ts` - Fixes Bitcoin calculations for S9 miners on March 31
4. `fix-bitcoin-calculations.ts` - Generic script for fixing Bitcoin calculations for any date and miner model
5. `fix-march-completeness.ts` - Comprehensive script that checks and fixes all Bitcoin calculations for a date range

## Validation Results

After applying fixes, the Bitcoin values for March 31, 2025 were updated to:
- M20S: 1.96797893 BTC (previously 0.014783491)
- S19J_PRO: 3.82346558 BTC (previously 3.74782362)
- S9: 1.00804733 BTC (previously 0.007521674)

These values represent a significant improvement in accuracy and are consistent with our calculation formula at the hourly level.

## Recommendations

To prevent similar issues in the future, we recommend:

1. **Regular Validation**: Implement daily validation checks comparing calculated and expected Bitcoin values.
2. **Atomic Updates**: Ensure that difficulty updates always trigger recalculation of Bitcoin values.
3. **Reconciliation Process**: Add a regular reconciliation process for calculations spanning difficulty adjustment periods.
4. **Comprehensive Fix Script**: Keep the `fix-bitcoin-calculations.ts` script as a reusable tool for any future calculations that need correction.

## Technical Implementation Details

The Bitcoin calculation follows these steps:
1. Convert MWh to kWh
2. Calculate total hashes achievable with this energy based on miner efficiency
3. Calculate expected Bitcoin based on network difficulty

The miner model efficiencies used are:
- S19J_PRO: 100 TH/s, 3050 Watts
- S9: 13.5 TH/s, 1323 Watts
- M20S: 68 TH/s, 3360 Watts

For daily summaries, a Bitcoin price of $65,000 was used for value_at_mining calculations.