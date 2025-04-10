#!/bin/bash

# Script to reprocess all data for 2025-04-03
# This script handles both curtailment data and Bitcoin calculations

echo "==== Starting Complete Data Reprocessing for 2025-04-03 ===="
echo "This script will:"
echo "1. Clear existing curtailment records and Bitcoin calculations for 2025-04-03"
echo "2. Process new curtailment data from Elexon API"
echo "3. Update daily, monthly, and yearly summaries"
echo "4. Process Bitcoin calculations for S19J_PRO, S9, and M20S miners"
echo "5. Verify that all calculations were successful"
echo ""
echo "Starting reprocessing..."
echo ""

# Run the TypeScript script using tsx
npx tsx scripts/reprocess_2025_04_03.ts

# Check if the script executed successfully
if [ $? -eq 0 ]; then
    echo ""
    echo "==== Data Reprocessing completed successfully ===="
    echo "All data for 2025-04-03 has been updated."
else
    echo ""
    echo "==== Data Reprocessing FAILED ===="
    echo "Please check the error messages above."
fi