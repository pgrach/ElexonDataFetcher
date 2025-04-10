# Bitcoin Mining Calculations

This document explains the process for generating Bitcoin mining calculations for different miner models.

## Overview

The system calculates potential Bitcoin mining based on curtailed energy volume data. Each miner model has specific characteristics (hashrate and power consumption) which affect mining efficiency.

## Supported Miner Models

- S19J_PRO: Hashrate 100 TH/s, Power 3050W
- S9: Hashrate 13.5 TH/s, Power 1323W
- M20S: Hashrate 68 TH/s, Power 3360W

## Data Flow

1. Curtailment records are stored in the `curtailment_records` table
2. Bitcoin calculations are generated for each curtailment record and stored in `historical_bitcoin_calculations`
3. Daily summaries are aggregated and stored in `bitcoin_daily_summaries`
4. Monthly and yearly summaries are derived from daily summaries

## Generating Bitcoin Calculations

To generate Bitcoin calculations for a specific date and miner model, use the script at:
```
/server/scripts/generate_m20s_sql_direct.ts
```

This script can be modified for any miner model by updating the constants:
- `TARGET_DATE`: The date to generate calculations for
- `MINER_MODEL`: The miner model to use (S19J_PRO, S9, or M20S)
- `MINER_EFFICIENCY`: The efficiency factor to use in calculations

## April 2, 2025 Mining Summary

For April 2, 2025, the Bitcoin mining calculations are:

| Miner Model | Bitcoin Mined | Records Processed |
|-------------|---------------|-------------------|
| S19J_PRO    | 12.65 BTC     | 432               |
| S9          | 1.72 BTC      | 143               |
| M20S        | 0.36 BTC      | 279               |

## Monthly Summary for April 2025

| Miner Model | Bitcoin Mined |
|-------------|---------------|
| S19J_PRO    | 35.11 BTC     |
| S9          | 8.71 BTC      |
| M20S        | 14.22 BTC     |

## Troubleshooting

### Duplicate Key Errors

When inserting Bitcoin calculations, you may encounter duplicate key errors due to the unique constraint on `(settlement_date, settlement_period, farm_id, miner_model)`. To fix this:

1. Delete existing records for the target date and miner model before inserting new ones
2. Use the `ON CONFLICT` clause in SQL inserts to update existing records

### Missing DynamoDB Difficulty Data

If difficulty data isn't available in DynamoDB, the system will use a default difficulty value defined in `server/types/bitcoin.ts`.