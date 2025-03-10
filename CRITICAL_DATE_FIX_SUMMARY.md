# Critical Date Processing Fix Summary

## Problem Identified
When processing Elexon API data for March 9, 2025 (particularly period 48), the system was only saving 10 records out of 21 total records. This was due to the way we handled multiple records with the same farm_id in the same settlement period.

## Root Cause
The original implementation cleared existing records for each farm individually before inserting the new record:

```typescript
// Clear any existing records first to avoid conflicts
await db.delete(curtailmentRecords)
  .where(
    and(
      eq(curtailmentRecords.settlementDate, date),
      eq(curtailmentRecords.settlementPeriod, period),
      eq(curtailmentRecords.farmId, record.id)
    )
  );
```

This approach caused subsequent records with the same farm_id to delete previous insertions, resulting in only the final record for each farm being retained when multiple exist.

## Solution Implemented
The solution involved modifying the `processPeriodWithRetries` function in `process_critical_date.ts` to:

1. First collect all unique farm IDs for a given period
2. Clear all existing records for these farms once, before any insertions
3. Process all records in bulk using a single transaction
4. Track all records properly in the reporting

```typescript
// Get the unique farm IDs for this period
const uniqueFarmIds = [...new Set(validRecords.map(record => record.id))];

// First clear all existing records for these farms in this period
if (uniqueFarmIds.length > 0) {
  try {
    await db.delete(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          eq(curtailmentRecords.settlementPeriod, period),
          inArray(curtailmentRecords.farmId, uniqueFarmIds)
        )
      );
  } catch (error) {
    log(`Period ${period}: Error clearing existing records: ${error}`, "error");
  }
}

// Prepare all records for bulk insertion
const recordsToInsert = validRecords.map(record => {
  // Record preparation logic
});

// Insert all records in a single transaction
await db.insert(curtailmentRecords).values(recordsToInsert);
```

## Results
After implementing the fix:

| Period | Original Record Count | Fixed Record Count |
|--------|------------------------|-------------------|
| 44     | 1                      | 1                 |
| 45     | 7                      | 7                 |
| 46     | 23                     | 23                |
| 47     | 30                     | 30                |
| 48     | 10                     | 21                |
| **Total** | **71**                  | **82**            |

## Farm Distribution for Period 48
The most affected period (48) now correctly shows multiple records per farm:

| Farm ID    | Record Count |
|------------|--------------|
| T_GORDW-2  | 3            |
| T_VKNGW-2  | 3            |
| T_HALSW-1  | 3            |
| T_EDINW-1  | 3            |
| T_VKNGW-3  | 2            |
| T_VKNGW-4  | 2            |
| T_VKNGW-1  | 2            |
| Others     | 1 each       |
| **Total**  | **21**       |

## Bitcoin Calculations
All corresponding Bitcoin calculations have been updated successfully. The system now has:
- 82 calculations for each of the 3 miner models (S19J_PRO, S9, M20S)
- Updated monthly and yearly summaries reflecting the corrected data

## Recommendations
This fix should be applied to other data processing scripts if they handle multiple records with the same farm_id, particularly:
- complete_reingestion_process.ts
- batch_process_periods.ts
- reingest_single_batch.ts

The current implementation in these scripts should be reviewed to ensure they handle duplicate farm IDs correctly within the same period.