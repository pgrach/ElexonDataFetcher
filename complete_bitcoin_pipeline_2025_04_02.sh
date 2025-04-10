#!/bin/bash

# Script to complete the Bitcoin calculation pipeline for 2025-04-02

echo "==== Starting Bitcoin Pipeline Update for 2025-04-02 ===="
echo "This script will:"
echo "1. Process Bitcoin calculations for all miner models (S19J_PRO, S9, M20S)"
echo "2. Update Bitcoin daily summaries"
echo "3. Update Bitcoin monthly summaries for April 2025"
echo "4. Update Bitcoin yearly summaries for 2025"
echo ""

echo "Starting processing..."
echo ""

# Run the TypeScript script using tsx
npx tsx server/scripts/complete_bitcoin_calculations_2025_04_02.ts

# Check if the script executed successfully
if [ $? -eq 0 ]; then
    echo ""
    echo "==== Bitcoin pipeline completed successfully ===="
    echo "All Bitcoin calculations and summaries for 2025-04-02 have been updated."
else
    echo ""
    echo "==== Bitcoin pipeline update FAILED ===="
    echo "Please check the error messages above."
fi