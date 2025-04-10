# Wind Farm Data and Bitcoin Calculation Reprocessing Prompt

## Task

I need to reprocess wind farm curtailment data and Bitcoin mining potential calculations for a specific date (YYYY-MM-DD). This involves:

1. [Optional] Fetching and processing curtailment data from the Elexon API
2. Recalculating daily, monthly, and yearly summaries 
3. Processing Bitcoin mining potential calculations for different miner models (S19J_PRO, S9, M20S)
4. Updating all related summary tables

## Parameters

- **Date**: [REPLACE_WITH_TARGET_DATE] (format: YYYY-MM-DD)
- **Skip Elexon**: [true/false] (Default: false) - Set to true to skip fetching new curtailment data from Elexon API
- **Bitcoin Difficulty**: [OPTIONAL_DIFFICULTY_VALUE] (Optional) - Specific Bitcoin network difficulty to use for calculations

## Available Tools

The reprocessing can be done using the following shell scripts:

1. **Complete Reprocessing** (recommended):
   ```bash
   ./reprocess_date.sh 2025-04-02 [--skipElexon] [--difficulty=113757508810853]
   ```

2. **Bitcoin-Only Reprocessing**:
   ```bash
   ./process_bitcoin.sh 2025-04-02 [difficulty]
   ```

## Expected Output

A successful reprocessing should:

1. Clear and recreate all curtailment records for the target date
2. Recalculate daily, monthly, and yearly summaries
3. Process Bitcoin calculations for all three miner models
4. Update Bitcoin summary tables
5. Verify the results match expected values

The script should output a detailed log (stored in the `logs` directory) showing:
- Number of records processed
- Energy and payment totals
- Bitcoin calculation totals for each miner model
- Verification results

## Important Considerations

- The reprocessing script handles database constraints and ensures data integrity
- Processing for a full day might take several minutes
- The script automatically clears existing data for the target date before reprocessing
- Bitcoin calculations should follow the expected pattern where higher hashrate miners (S19J_PRO) produce more Bitcoin than lower hashrate miners (M20S and S9)
- Logs for the reprocessing are stored in the `logs` directory for future reference
- If you encounter issues, check the detailed logs for error messages

## Example Usage

To reprocess data for April 2, 2025, including fetching new data from Elexon API:
```bash
./reprocess_date.sh 2025-04-02
```

To reprocess only Bitcoin calculations for April 2, 2025 using existing curtailment data:
```bash
./process_bitcoin.sh 2025-04-02
```

## Verification

After reprocessing, you can verify the results by:
1. Checking the database tables for the target date
2. Reviewing the log file generated during processing
3. Comparing the output values with expected ratios (e.g., Bitcoin calculations matching miner hashrates)