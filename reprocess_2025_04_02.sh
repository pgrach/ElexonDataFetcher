#!/bin/bash

# Script to reprocess data for 2025-04-02

echo "==== Starting reprocessing for 2025-04-02 ===="
echo "This script will:"
echo "1. Clear existing data for April 2, 2025"
echo "2. Fetch new data from Elexon API for all 48 settlement periods"
echo "3. Rebuild all summary tables (daily, monthly, yearly)"
echo "4. Recalculate Bitcoin mining potential"
echo "5. Verify data integrity"
echo ""

echo "Starting reprocessing..."
echo ""

# Run the TypeScript script using tsx (which handles TypeScript execution)
npx tsx server/scripts/reprocess_2025_04_02.ts

# Check if the script executed successfully
if [ $? -eq 0 ]; then
    echo ""
    echo "==== Reprocessing completed successfully ===="
    echo "All data for 2025-04-02 has been updated."
else
    echo ""
    echo "==== Reprocessing FAILED ===="
    echo "Please check the error messages above."
fi