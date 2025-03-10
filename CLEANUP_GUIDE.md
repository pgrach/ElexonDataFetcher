# Project Cleanup Guide

After successfully fixing the data processing issues, here's a guide for cleaning up the project:

## 1. Consolidated Script
I've created a new optimized script that combines all the improvements:

- `optimized_critical_date_processor.ts` - Use this for processing any problematic date

This script:
- Properly handles multiple records with the same farm_id in a period
- Processes data in efficient batches
- Automatically runs reconciliation to update Bitcoin calculations
- Has comprehensive logging
- Simplified command-line interface

## 2. Files That Can Be Removed

The following temporary or duplicate files can be safely removed:

```
/CRITICAL_DATE_FIX_SUMMARY.md  # (Keep this if you want the documentation)
/process_critical_date.log      # Log files from previous runs
/reingestion_20250306.log       # Old log file
/reingestion_20250309.log       # Old log file
```

## 3. Usage Guide

To process a problematic date with the new optimized script:

```bash
# Process all periods (1-48) for a specific date
npx tsx optimized_critical_date_processor.ts 2025-03-09

# Process specific periods for a date
npx tsx optimized_critical_date_processor.ts 2025-03-09 44 48
```

## 4. Future Improvements

Consider applying the same pattern for bulk processing to these scripts:

```
/complete_reingestion_process.ts
/batch_process_periods.ts
/reingest_single_batch.ts
```

The key improvement is changing from clearing records per farm to:
1. Collect all unique farm IDs
2. Clear all farms at once before insertion
3. Bulk insert all records

## 5. Verification

Always verify successful processing by checking:

```sql
-- Verify record counts by period
SELECT settlement_period, COUNT(*) 
FROM curtailment_records 
WHERE settlement_date = '2025-03-09' AND settlement_period BETWEEN 44 AND 48
GROUP BY settlement_period
ORDER BY settlement_period;

-- Verify Bitcoin calculations
SELECT miner_model, COUNT(*) 
FROM historical_bitcoin_calculations
WHERE settlement_date = '2025-03-09' AND settlement_period BETWEEN 44 AND 48
GROUP BY miner_model;

-- Verify processing of multiple records per farm
SELECT farm_id, COUNT(*) 
FROM curtailment_records 
WHERE settlement_date = '2025-03-09' AND settlement_period = 48
GROUP BY farm_id
ORDER BY COUNT(*) DESC;
```

## 6. Backup

It's recommended to make a backup of the fixed data:

```sql
-- Export fixed data to CSV
\COPY (SELECT * FROM curtailment_records WHERE settlement_date = '2025-03-09' AND settlement_period BETWEEN 44 AND 48 ORDER BY settlement_period, farm_id) TO '/tmp/fixed_records_2025-03-09.csv' WITH CSV HEADER;
```