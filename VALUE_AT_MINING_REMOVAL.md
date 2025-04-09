# Removal of `value_at_mining` Column

This document summarizes the changes made to remove the `value_at_mining` column from all Bitcoin-related summary tables.

## Affected Tables

The following tables had the `value_at_mining` column removed:

1. `bitcoin_daily_summaries`
2. `bitcoin_monthly_summaries`
3. `bitcoin_yearly_summaries`

## Changes Made

### Database Schema Updates

1. The column definitions were removed from the Drizzle schema in `db/schema.ts` for:
   - `bitcoinMonthlySummaries`
   - `bitcoinYearlySummaries`

2. SQL migration scripts were created to:
   - Make the columns nullable first (to avoid constraint issues)
   - Drop the columns
   - Verify column removal

### Code Changes

1. The following files were updated to remove `value_at_mining` usage:

   - `server/scripts/update_bitcoin_daily_summaries.ts`:
     - Removed Bitcoin price calculation
     - Updated the INSERT statement to not include `value_at_mining`

   - `server/services/bitcoinService.ts`:
     - Updated the yearly summary insert to remove `value_at_mining` and `months_count` fields

### Migration Scripts

1. Created `server/scripts/remove_value_at_mining.ts` to remove the column from `bitcoin_daily_summaries`
2. Created `server/scripts/remove_value_at_mining_from_all_tables.ts` to remove the column from:
   - `bitcoin_monthly_summaries`
   - `bitcoin_yearly_summaries`

## Testing

The migration scripts were run successfully and verified that the columns were removed from all tables.

## Next Steps

1. Continue monitoring for any issues with the Bitcoin calculation pipeline
2. Update any documentation or reports that might be referencing the removed columns