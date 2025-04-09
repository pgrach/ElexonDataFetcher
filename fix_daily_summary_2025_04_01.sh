#!/bin/bash

# Exit on error
set -e

echo "==========================================================="
echo "Fix Daily Summary for 2025-04-01"
echo "==========================================================="

# Make the script executable
chmod +x fix_daily_summary_2025_04_01.sh

# Execute the TypeScript script using tsx
echo "Starting fix process..."
npx tsx fix_daily_summary_2025_04_01.ts

echo "Fix process completed."
echo "==========================================================="