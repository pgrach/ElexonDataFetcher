#!/bin/bash

# Update daily summaries with wind generation data
echo "Updating daily summaries with wind generation data..."
npx tsx update_daily_summary_wind_data.ts

# Check exit status
if [ $? -eq 0 ]; then
  echo "✓ Successfully updated daily summaries with wind generation data"
else
  echo "✗ Failed to update daily summaries with wind generation data"
  exit 1
fi

exit 0