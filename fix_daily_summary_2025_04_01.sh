#!/bin/bash

# This script runs the fix_daily_summary_2025_04_01.ts script to correct daily summary values.

echo "===================================================
     DAILY SUMMARY FIX for 2025-04-01
==================================================="

echo "Starting fix process..."
npx tsx fix_daily_summary_2025_04_01.ts

# Check exit status
if [ $? -eq 0 ]; then
  echo -e "\nDaily summary fixed successfully!"
else
  echo -e "\nError fixing daily summary. Check log file for details."
  exit 1
fi