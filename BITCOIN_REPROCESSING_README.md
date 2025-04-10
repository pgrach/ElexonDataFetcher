# Bitcoin Calculation Reprocessing Tool

This tool provides a simple way to reprocess Bitcoin mining potential calculations for specific dates. It operates directly on the database, creating or updating historical Bitcoin calculations and all dependent summary tables.

## Available Scripts

### 1. Generic Reprocessing Script (For Any Date)

```bash
./process_bitcoin.sh YYYY-MM-DD [difficulty]
```

This is the recommended script for most use cases. It can process any date and accepts an optional difficulty parameter.

Examples:
```bash
# Process with default difficulty
./process_bitcoin.sh 2025-04-02

# Process with specific difficulty
./process_bitcoin.sh 2025-04-02 113757508810853
```

### 2. Standard Reprocessing for 2025-04-02 (Using DynamoDB for Difficulty)

```bash
./reprocess_bitcoin_2025_04_02.sh
```

This script attempts to reprocess Bitcoin calculations for 2025-04-02 by:
- Fetching the Bitcoin network difficulty from DynamoDB
- Clearing existing historical Bitcoin calculations for that date
- Generating new Bitcoin calculations for all curtailment records
- Updating daily, monthly, and yearly Bitcoin summaries

### 3. Direct Reprocessing for 2025-04-02 (Using Hardcoded Difficulty)

```bash
./process_bitcoin_2025_04_02_direct.sh
```

This script performs a more direct reprocessing using a hardcoded difficulty value, which avoids potential AWS credentials issues with DynamoDB. It:
- Uses a typical difficulty value from April 2025
- Clears existing Bitcoin calculations for 2025-04-02
- Processes all curtailment records to create new Bitcoin calculations
- Updates all summary tables (daily, monthly, yearly)
- Verifies the calculations were successfully created

## Key Features

- **Efficient Processing**: Handles 833 curtailment records for each miner model (S19J_PRO, S9, M20S) in just a few seconds
- **Complete Pipeline**: Updates all dependent summary tables to ensure data consistency
- **Constraint Handling**: Uses `onConflictDoUpdate` to properly handle unique constraints in the database
- **Verification**: Confirms data integrity with verification queries after processing
- **Command Line Parameters**: The generic script accepts date and optional difficulty parameters

## Implementation Details

### Generic Bitcoin Processing Script

The generic script (`server/scripts/process_bitcoin_calculations.ts`) is designed to:

1. Accept command line parameters for date and optional difficulty
2. Clear existing Bitcoin calculations for the specified date
3. Process all curtailment records to generate Bitcoin calculations for three miner models:
   - S19J_PRO
   - S9
   - M20S
4. Update daily, monthly, and yearly summary tables
5. Verify the calculations were successfully created

## Output Example

```
==== Processing Bitcoin calculations for 2025-04-02 ====

==== Processing S19J_PRO miner model ====
Successfully processed 833 Bitcoin calculations for 2025-04-02 and S19J_PRO
Total Bitcoin calculated: 24.15836053
Updated daily summary for 2025-04-02 and S19J_PRO: 24.15836053438701 BTC

==== Processing S9 miner model ====
Successfully processed 833 Bitcoin calculations for 2025-04-02 and S9
Total Bitcoin calculated: 7.51867343
Updated daily summary for 2025-04-02 and S9: 7.518673431620424 BTC

==== Processing M20S miner model ====
Successfully processed 833 Bitcoin calculations for 2025-04-02 and M20S
Total Bitcoin calculated: 14.91203564
Updated daily summary for 2025-04-02 and M20S: 14.912035639380528 BTC
```

## Technical Notes

- The direct processing script uses a hardcoded difficulty value of `113757508810853` for 2025-04-02
- Records are processed in batches with all inserts executed using `Promise.all` for better performance
- Script includes error handling to continue processing despite isolated failures