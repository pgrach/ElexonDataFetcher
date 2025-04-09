#!/bin/bash

# Set script to exit on error
set -e

echo "==========================================================="
echo "Update Records for 2025-04-01 from Elexon API"
echo "==========================================================="

# Make the script executable
chmod +x update_records_2025_04_01.sh

# Execute the TypeScript script using tsx
echo "Starting update process..."
npx tsx server/scripts/update_records_2025_04_01.ts

echo "Update process completed."
echo "==========================================================="