# Elexon Data Update Scripts

This directory contains scripts for checking and updating Elexon API data for wind farm curtailment records.

## Scripts

1. **`check_and_update_2025_03_27.ts`**
   - Main script to check and update curtailment data for 2025-03-27
   - Compares Elexon API data with database records
   - Updates the database with any missing or different records

2. **`update_elexon_data.sh`**
   - Shell script wrapper to run the TypeScript update script
   - Provides a summary of the database state after updating

3. **`check_progress.sh`**
   - Tool to check the progress of a running update job
   - Shows statistics on records processed and estimated completion time

## How to Use

### Running a Data Update

To update the 2025-03-27 data:

```bash
./update_elexon_data.sh
```

This script will:
- Run the TypeScript update script
- Display the final database statistics when complete

### Checking Progress

To check the progress of a running update:

```bash
./check_progress.sh
```

This will show:
- Current period being processed (out of 48)
- Number of records updated so far
- Missing, different, and identical record counts
- Estimated time remaining

### Understanding the Logs

The update process creates a log file named `check_update_2025_03_27_YYYY-MM-DD.log`, where:
- Each period's processing is logged
- Records found in the API are compared with database records
- Updates are tracked

## How It Works

1. The script loads BMU mappings from `bmuMapping.json` to identify wind farms
2. For each settlement period (1-48), it fetches both bid and offer data from Elexon
3. It compares this data with existing database records
4. Records that are missing or different are updated in the database
5. A summary is provided at the end of the process

## Troubleshooting

- If the script fails, check the log file for error messages
- Common issues include API rate limiting or network connectivity problems
- The script has built-in retry logic for temporary failures