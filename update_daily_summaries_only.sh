#!/bin/bash

# This script runs the update_daily_summaries_only.ts script with the provided date.
# It takes an optional date parameter in the format YYYY-MM-DD. Default is 2025-04-01.

# Set default date if not provided
DATE=${1:-"2025-04-01"}

echo "===================================================
     BITCOIN DAILY SUMMARIES UPDATE for ${DATE}
==================================================="

echo "Starting update process..."
npx tsx update_daily_summaries_only.ts ${DATE}

# Check exit status
if [ $? -eq 0 ]; then
  echo -e "\nBitcoin daily summaries updated successfully!"
else
  echo -e "\nError updating bitcoin daily summaries. Check log file for details."
  exit 1
fi