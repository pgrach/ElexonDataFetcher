#!/bin/bash

# Script to reprocess Bitcoin calculations for 2025-04-02

echo "==== Starting Bitcoin Calculation Reprocessing for 2025-04-02 ===="
echo "This script will:"
echo "1. Clear existing Bitcoin calculations for April 2, 2025"
echo "2. Process new calculations for S19J_PRO, S9, and M20S miners"
echo "3. Update daily, monthly, and yearly Bitcoin summaries"
echo "4. Verify that all calculations were successful"
echo ""
echo "Starting reprocessing..."
echo ""

# Run the TypeScript script using tsx
npx tsx server/scripts/reprocess_bitcoin_2025_04_02.ts

# Check if the script executed successfully
if [ $? -eq 0 ]; then
    echo ""
    echo "==== Bitcoin Calculation Reprocessing completed successfully ===="
    echo "All Bitcoin data for 2025-04-02 has been updated."
else
    echo ""
    echo "==== Bitcoin Calculation Reprocessing FAILED ===="
    echo "Please check the error messages above."
fi