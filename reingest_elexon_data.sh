#!/bin/bash
# This script reingests Elexon data for a specific date
# Usage: ./reingest_elexon_data.sh [date]
# Default date: 2025-04-01

DATE=${1:-"2025-04-01"}

echo "==================================================="
echo "       ELEXON DATA REINGESTION for $DATE"
echo "==================================================="
echo "Starting reingestion process..."

# Run the reingestion script
npx tsx reingest_elexon_data.ts $DATE

# Check if reingestion was successful
if [ $? -eq 0 ]; then
  echo ""
  echo "==================================================="
  echo "           REBUILDING DEPENDENT TABLES"
  echo "==================================================="
  
  # Rebuild Bitcoin calculations
  echo "Rebuilding Bitcoin calculations..."
  npx tsx server/scripts/update_bitcoin_daily_summaries_for_date.ts $DATE
  
  # Update monthly and yearly summaries
  echo "Updating monthly and yearly summaries..."
  npx tsx server/scripts/update_bitcoin_monthly_summaries.ts
  
  # Update wind generation data in daily summaries
  echo "Updating wind generation data..."
  npx tsx update_daily_summary_wind_data.ts $DATE
  
  echo ""
  echo "==================================================="
  echo "           REINGESTION COMPLETED"
  echo "==================================================="
  echo "Elexon data for $DATE has been completely reingested"
  echo "and all dependent tables have been updated."
  echo ""
  echo "Please verify the data using the API endpoints:"
  echo "- /api/summary/daily/$DATE"
  echo "- /api/curtailment/mining-potential?date=$DATE&minerModel=S19J_PRO"
  echo "==================================================="
else
  echo ""
  echo "==================================================="
  echo "           REINGESTION FAILED"
  echo "==================================================="
  echo "Please check the logs for more information."
  echo "==================================================="
  exit 1
fi