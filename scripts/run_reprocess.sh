#!/bin/bash

# Run Reprocess Script
# This script runs the data reprocessing script for a specific date

# Parse command line arguments
date=${1:-$(date +%Y-%m-%d)}  # Default to today's date
max_periods=${2:-48}  # Default to 48 settlement periods

# Display parameters
echo "=== Data Reprocessing Script ==="
echo "Date: $date"
echo "Max Settlement Periods: $max_periods"
echo "==============================="

# Execute the reprocessing script with environmental variables
DATE=$date MAX_PERIODS=$max_periods npx ts-node scripts/reprocess_date.ts

exit_code=$?
if [ $exit_code -ne 0 ]; then
  echo "Error: Reprocessing script failed with exit code $exit_code"
  exit $exit_code
fi