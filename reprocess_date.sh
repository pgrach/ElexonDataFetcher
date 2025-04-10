#!/bin/bash

# Complete Date Reprocessing Script
# This script handles the reprocessing of all data for a specific date
# Usage: ./reprocess_date.sh YYYY-MM-DD [--skipElexon] [--difficulty=123456789]

# Check if at least one argument is provided
if [ -z "$1" ]; then
    echo "Error: Date parameter is required"
    echo "Usage: ./reprocess_date.sh YYYY-MM-DD [--skipElexon] [--difficulty=DIFFICULTY]"
    echo "Example: ./reprocess_date.sh 2025-04-02"
    echo "Example with options: ./reprocess_date.sh 2025-04-02 --skipElexon --difficulty=113757508810853"
    exit 1
fi

DATE=$1
shift  # Remove the first argument (date) from the arguments list

# Pass all remaining arguments to the script
OPTIONS="$@"

# Format the options string for display
DISPLAY_OPTIONS=""
if [[ "$OPTIONS" == *"--skipElexon"* ]]; then
    DISPLAY_OPTIONS="$DISPLAY_OPTIONS Skip Elexon API,"
fi

if [[ "$OPTIONS" == *"--difficulty="* ]]; then
    DIFFICULTY=$(echo "$OPTIONS" | grep -o 'difficulty=[^ ]*' | cut -d= -f2)
    DISPLAY_OPTIONS="$DISPLAY_OPTIONS Difficulty: $DIFFICULTY,"
fi

# Remove trailing comma
DISPLAY_OPTIONS=$(echo "$DISPLAY_OPTIONS" | sed 's/,$//')
if [ -z "$DISPLAY_OPTIONS" ]; then
    DISPLAY_OPTIONS="None"
fi

# Create logs directory if it doesn't exist
mkdir -p logs

echo "==== Starting Complete Data Reprocessing for $DATE ===="
echo "This script will:"
echo "1. Clear existing curtailment records and Bitcoin calculations for $DATE"
echo "2. Process new curtailment data from Elexon API (unless --skipElexon is used)"
echo "3. Update daily, monthly, and yearly summaries"
echo "4. Process Bitcoin calculations for S19J_PRO, S9, and M20S miners"
echo "5. Verify that all calculations were successful"
echo ""
echo "Options: $DISPLAY_OPTIONS"
echo ""
echo "Starting reprocessing..."
echo ""

# Run the TypeScript script using tsx with all provided arguments
npx tsx server/scripts/reprocess_date_complete.ts --date=$DATE $OPTIONS

# Check if the script executed successfully
if [ $? -eq 0 ]; then
    echo ""
    echo "==== Data Reprocessing completed successfully ===="
    echo "All data for $DATE has been updated."
else
    echo ""
    echo "==== Data Reprocessing FAILED ===="
    echo "Please check the error messages and log file above."
fi