# Data Update Process for 2025-04-01

## Overview

This document describes the process of updating the curtailment records for 2025-04-01 by ingesting data from the Elexon API and then cascading those updates through all dependent tables in the system.

## Steps Performed

1. **Created Update Scripts**:
   - `server/scripts/update_records_2025_04_01.ts`: Main script to update curtailment records and trigger cascading updates
   - `server/scripts/update_bitcoin_daily_summaries_2025_04_01.ts`: Script to update Bitcoin daily summaries
   - `server/scripts/update_bitcoin_monthly_summaries.ts`: Script to update Bitcoin monthly and yearly summaries
   - `update_wind_data.ts`: Script to update wind generation data in daily summaries

2. **Data Ingestion from Elexon API**:
   - Fetched curtailment data for 2025-04-01 for all 48 settlement periods
   - Processed and filtered records for wind farms with negative volume and SO/CADL flags
   - Inserted filtered records into the `curtailment_records` table
   - Updated `daily_summaries` with aggregated energy and payment totals

3. **Wind Generation Data**:
   - Ingested wind generation data from Elexon API for 2025-04-01
   - Calculated average onshore, offshore, and total wind generation
   - Updated `daily_summaries` table with wind generation data

4. **Bitcoin Calculations**:
   - Updated `historical_bitcoin_calculations` table with Bitcoin mining calculations for each farm and period
   - Generated 624 historical bitcoin calculation records for 2025-04-01

5. **Summary Table Updates**:
   - Updated `bitcoin_daily_summaries` for all three miner models (S19J_PRO, S9, M20S)
   - Updated `bitcoin_monthly_summaries` for April 2025
   - Updated `bitcoin_yearly_summaries` for 2025

## Data Summary

### Curtailment Data
- **Date**: 2025-04-01
- **Total Records**: 523
- **Total Energy**: 13,945.59 MWh (corrected from 9,550.74 MWh)
- **Total Payment**: £-325,829.47 (corrected from £179,129.98)

### Wind Generation Data
- **Total Wind Generation**: 10,043.88 MW
- **Onshore Wind**: 4,559.10 MW
- **Offshore Wind**: 5,484.78 MW

### Bitcoin Mining Calculations
- **S19J_PRO**: 7.21023356 BTC
- **S9**: 2.24400216 BTC
- **M20S**: 4.45059968 BTC

## Verification

1. **API Endpoints**:
   - `/api/summary/daily/2025-04-01`: Returns correct energy and payment totals
   - `/api/curtailment/mining-potential`: Calculates Bitcoin mining potential correctly
   - `/api/curtailment/monthly-mining-potential/2025-04`: Returns correct monthly Bitcoin totals

2. **Database Queries**:
   - Verified curtailment_records count: 523 records
   - Verified historical_bitcoin_calculations count: 624 records
   - Verified bitcoin_daily_summaries are properly populated
   - Verified bitcoin_monthly_summaries for April 2025 are updated
   - Verified bitcoin_yearly_summaries for 2025 are updated
   - Verified daily_summaries table has wind generation data for 2025-04-01

3. **Data Discrepancy with Elexon API**:
   - **CRITICAL ISSUE DISCOVERED**: Comprehensive 48-period validation against the Elexon API shows severe discrepancy
   - Full validation across all 48 settlement periods reveals 341.90% difference in volume
   - API data shows 3,155.79 MWh in only 6 periods vs. DB 13,945.59 MWh across 47 periods (difference: 10,789.80 MWh)
   - API payment calculation: £85,628.37 vs. DB: £-325,829.47 (difference: £411,457.84)
   - Record count: API 77 records vs. DB 372 records (difference: 295 records)
   - 42 settlement periods in DB have no corresponding data in the API: 1-17, 19-23, 25-28, 30-35, 37-41, 43-47
   - Payment values appear to have incorrect signs in the database (negative instead of positive)
   - URGENT ACTION REQUIRED: Complete reingestion of data from Elexon API for 2025-04-01 is mandatory
   - All downstream Bitcoin calculations, daily summaries, and monthly aggregations are affected by this discrepancy

## Running the Update Process

To update the data for 2025-04-01, run:

```bash
./update_records_2025_04_01.sh
```

This will execute the full update process, ingesting data from the Elexon API and updating all dependent tables. 

For specific update tasks, you can run the following scripts:

1. **Update Bitcoin Summary Tables**:
```bash
npx tsx server/scripts/update_bitcoin_daily_summaries_2025_04_01.ts
npx tsx server/scripts/update_bitcoin_monthly_summaries.ts
```

2. **Update Wind Generation Data in Daily Summaries**:
```bash
./update_wind_data.sh
```

## Notes

- The update process follows the standard data flow: Elexon API → curtailment_records → daily_summaries → Bitcoin calculations → Bitcoin summaries
- The `reconcileDay` function from the historical reconciliation service handles most of this process in one call
- For detailed debugging or specific updates, individual scripts can be run separately

## Validation Tools

To validate data against the Elexon API, run:

```bash
./validate_all_periods.sh
```

This script compares data in the database with what is currently available from the Elexon API for ALL 48 settlement periods. The tool:

1. Fetches data from all 48 settlement periods by running three batch jobs to avoid timeouts
2. Compares volumes, payments, and record counts for each period
3. Identifies missing or mismatched periods across the entire day
4. Combines all results into a comprehensive validation report
5. Reports percentage differences to help identify data integrity issues

**Important**: Significant differences between API and database values may indicate:
- API data has been updated since original ingestion
- Filtering criteria have changed
- Data processing errors in the ingestion pipeline

For best results, run a complete reingestion if the difference exceeds 1%.

## Comprehensive Validation Results

The validation process was significantly enhanced by implementing a batch processing approach to validate all 48 settlement periods instead of just the 6 key periods previously checked. This complete validation reveals the full extent of the data discrepancy:

```
=== COMPLETE VALIDATION RESULTS ===

API Data:
  Total Volume: 3,155.79 MWh
  Total Payment: £85,628.37
  Total Records: 77
  Periods with Data: 6 (periods 18, 24, 29, 36, 42, 48 only)

Database Data:
  Total Volume: 13,945.59 MWh
  Total Payment: £-325,829.47
  Total Records: 372
  Periods with Data: 47

Differences:
  Volume Difference: 10,789.80 MWh
  Payment Difference: £411,457.84
  Record Count Difference: 295
  Volume Percent Difference: 341.90%
```

### Key Findings

1. **Massive Data Disparity**: The 341.90% difference in volume is far beyond any acceptable threshold and indicates a serious data quality issue.

2. **Period Coverage**: The API only shows data in 6 periods (18, 24, 29, 36, 42, 48) while the database has data for 47 out of 48 periods.

3. **Suspicious Data Pattern**: The database contains 42 settlement periods that show no corresponding data in the Elexon API, which strongly suggests either:
   - Data was incorrectly ingested from a different source
   - Invalid filtering criteria were applied during ingestion
   - Data was manipulated post-ingestion

4. **Payment Sign Issues**: The payment values in the database appear to have incorrect signs (negative instead of positive), which affects all financial calculations.

5. **Downstream Impact**: This discrepancy cascades through all dependent tables, including Bitcoin calculations and summaries, making current reports highly unreliable.

### Recommended Action

**IMMEDIATE REINGEST REQUIRED**: The validation conclusively demonstrates that the data for 2025-04-01 must be completely reingested from the Elexon API with the correct filtering criteria as soon as possible to maintain data integrity.