#!/bin/bash

# Script to reprocess Bitcoin calculations for any date
# Usage: ./process_bitcoin.sh YYYY-MM-DD [difficulty]

if [ -z "$1" ]; then
    echo "Error: Date parameter is required"
    echo "Usage: ./process_bitcoin.sh YYYY-MM-DD [difficulty]"
    echo "Example: ./process_bitcoin.sh 2025-04-02"
    echo "Example with difficulty: ./process_bitcoin.sh 2025-04-02 113757508810853"
    exit 1
fi

DATE=$1
DIFFICULTY_ARG=""

# Check if difficulty is provided
if [ ! -z "$2" ]; then
    DIFFICULTY_ARG="--difficulty=$2"
fi

echo "==== Starting Bitcoin Calculation Reprocessing for $DATE ===="
echo "This script will:"
echo "1. Clear existing Bitcoin calculations for $DATE"
echo "2. Process new calculations for S19J_PRO, S9, and M20S miners"
echo "3. Update daily, monthly, and yearly Bitcoin summaries"
echo "4. Verify that all calculations were successful"
echo ""
echo "Starting reprocessing..."
echo ""

# Run the TypeScript script using tsx
npx tsx server/scripts/process_bitcoin_calculations.ts --date=$DATE $DIFFICULTY_ARG

# Check if the script executed successfully
if [ $? -eq 0 ]; then
    echo ""
    echo "==== Bitcoin Calculation Reprocessing completed successfully ===="
    echo "All Bitcoin data for $DATE has been updated."
else
    echo ""
    echo "==== Bitcoin Calculation Reprocessing FAILED ===="
    echo "Please check the error messages above."
fi