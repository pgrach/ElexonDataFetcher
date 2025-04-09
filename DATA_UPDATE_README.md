# Data Update Process for 2025-04-01

## Overview

This document describes the process of updating the curtailment records for 2025-04-01 by ingesting data from the Elexon API and then cascading those updates through all dependent tables in the system.

## Steps Performed

1. **Created Update Scripts**:
   - `server/scripts/update_records_2025_04_01.ts`: Main script to update curtailment records and trigger cascading updates
   - `server/scripts/update_bitcoin_daily_summaries_2025_04_01.ts`: Script to update Bitcoin daily summaries
   - `server/scripts/update_bitcoin_monthly_summaries.ts`: Script to update Bitcoin monthly and yearly summaries

2. **Data Ingestion from Elexon API**:
   - Fetched curtailment data for 2025-04-01 for all 48 settlement periods
   - Processed and filtered records for wind farms with negative volume and SO/CADL flags
   - Inserted filtered records into the `curtailment_records` table
   - Updated `daily_summaries` with aggregated energy and payment totals

3. **Bitcoin Calculations**:
   - Updated `historical_bitcoin_calculations` table with Bitcoin mining calculations for each farm and period
   - Generated 624 historical bitcoin calculation records for 2025-04-01

4. **Summary Table Updates**:
   - Updated `bitcoin_daily_summaries` for all three miner models (S19J_PRO, S9, M20S)
   - Updated `bitcoin_monthly_summaries` for April 2025
   - Updated `bitcoin_yearly_summaries` for 2025

## Data Summary

### Curtailment Data
- **Date**: 2025-04-01
- **Total Records**: 523
- **Total Energy**: 9,550.74 MWh
- **Total Payment**: £179,129.98

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

## Running the Update Process

To update the data for 2025-04-01, run:

```bash
./update_records_2025_04_01.sh
```

This will execute the full update process, ingesting data from the Elexon API and updating all dependent tables. If the Bitcoin summary tables need to be updated separately, you can run:

```bash
npx tsx server/scripts/update_bitcoin_daily_summaries_2025_04_01.ts
npx tsx server/scripts/update_bitcoin_monthly_summaries.ts
```

## Notes

- The update process follows the standard data flow: Elexon API → curtailment_records → daily_summaries → Bitcoin calculations → Bitcoin summaries
- The `reconcileDay` function from the historical reconciliation service handles most of this process in one call
- For detailed debugging or specific updates, individual scripts can be run separately