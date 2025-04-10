#!/bin/bash

# Script to update Bitcoin summaries for 2025-04-02

echo "==== Starting Bitcoin Summaries Update for 2025-04-02 ===="
echo "This script will update all Bitcoin summary tables based on existing historical_bitcoin_calculations data:"
echo "1. Bitcoin daily summaries"
echo "2. Bitcoin monthly summaries"
echo "3. Bitcoin yearly summaries"
echo ""

echo "Starting update..."
echo ""

# Run the TypeScript script using tsx
npx tsx server/scripts/update_bitcoin_summaries_2025_04_02.ts

# Check if the script executed successfully
if [ $? -eq 0 ]; then
    echo ""
    echo "==== Bitcoin summaries updated successfully ===="
    echo "All Bitcoin summaries for 2025-04-02 have been updated."
else
    echo ""
    echo "==== Bitcoin summaries update FAILED ===="
    echo "Please check the error messages above."
fi