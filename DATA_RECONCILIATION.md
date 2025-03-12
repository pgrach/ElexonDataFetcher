# Data Reconciliation and Verification Tools

This document outlines the tools and processes for ensuring data completeness and accuracy in the cryptocurrency mining analytics platform.

## Overview

The platform relies on Elexon API data for wind farm curtailment records and performs Bitcoin mining calculations. To ensure data integrity, we've created several verification and reconciliation tools.

## Issue Resolution for 2025-03-11

We successfully fixed data completeness issues for 2025-03-11, specifically:

1. Missing/incorrect data in specific settlement periods (3, 4, 7, 8) corresponding to 1:00 and 3:00 hours
2. Ensuring all 48 settlement periods were present
3. Validating Bitcoin calculations across all periods for 3 miner models (S19J_PRO, S9, M20S)

### Critical Period Data

| Period | Records | Volume (MWh) | Payment (£) |
|--------|---------|--------------|-------------|
| 3      | 15      | 453.07       | -194.88     |
| 4      | 18      | 389.10       | -167.31     |
| 7      | 10      | 339.13       | -145.82     |
| 8      | 9       | 336.97       | -144.90     |

### Verified Totals
- Total records: 1,446
- Total volume: 44,140.93 MWh
- Total payment: £-554,999.84
- Bitcoin mined (S19J_PRO): 33.80154850 BTC
- Bitcoin mined (S9): 10.51987545 BTC
- Bitcoin mined (M20S): 20.86440881 BTC

## Verification Tools

### verification_report_2025_03_11.ts
Comprehensive verification script that checks:
- Settlement period coverage (all 48 periods)
- Critical period data accuracy
- Daily summary consistency
- Bitcoin calculation completeness

Run with:
```
npx tsx verification_report_2025_03_11.ts
```

### quick_data_check.ts
Simplified verification tool that can be used for any date:
- Checks period coverage
- Verifies daily summary matches
- Confirms Bitcoin calculations for all miner models

Run with:
```
npx tsx quick_data_check.ts YYYY-MM-DD
```

Example:
```
npx tsx quick_data_check.ts 2025-03-11
```

## Data Fixing Tools

### fix_time_periods_2025_03_11.ts
Script focused on fixing specific problematic periods (3, 4, 7, 8):
- Fetches correct data from Elexon API
- Updates curtailment records
- Updates daily summaries
- Recalculates Bitcoin mining potential

### fix_missing_periods_2025_03_11.ts
Script to address periods with completely missing data:
- Identifies missing periods
- Fetches data from Elexon API
- Inserts correct records
- Updates related calculations

## Best Practices for Data Reconciliation

1. **Daily Verification**: Run `quick_data_check.ts` daily to ensure data completeness
2. **Investigation Process**:
   - If issues are found, examine specific periods
   - Check API response data against database records
   - Fix specific periods rather than reprocessing entire days when possible
3. **Verification After Fixes**: Always run verification reports after making data fixes
4. **Documentation**: Document all data issues and fixes for future reference

## Related Services

Key services involved in the data pipeline:
- `server/services/elexon.ts` - Handles API requests to Elexon
- `server/services/curtailment.ts` - Processes curtailment records
- `server/services/bitcoinService.ts` - Calculates Bitcoin mining potential
- `server/services/historicalReconciliation.ts` - Manages data reconciliation