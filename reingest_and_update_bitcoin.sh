#!/bin/bash

# Define date to process
DATE=${1:-"2025-04-01"}

# Log file for combined process
LOG_FILE="logs/combined_reingestion_$DATE_$(date +%Y-%m-%dT%H-%M-%S).log"

# Ensure logs directory exists
mkdir -p logs

echo "==================================================="
echo "  COMPLETE REINGESTION AND UPDATE FOR $DATE"
echo "==================================================="
echo "Logging to $LOG_FILE"

echo "[$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")] Starting complete reingestion process for $DATE" | tee -a "$LOG_FILE"

# Step 1: Run key periods reingestion
echo "[$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")] STEP 1: Reingesting Elexon data for key periods" | tee -a "$LOG_FILE"
./reingest_elexon_key_periods.sh "$DATE"
REINGEST_STATUS=$?

if [ $REINGEST_STATUS -ne 0 ]; then
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")] ERROR: Elexon reingestion failed with status $REINGEST_STATUS" | tee -a "$LOG_FILE"
  exit 1
fi

# Step 2: Update Bitcoin calculations
echo "[$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")] STEP 2: Updating Bitcoin calculations" | tee -a "$LOG_FILE"
./update_bitcoin_daily_summaries_for_date.sh "$DATE"
BITCOIN_STATUS=$?

if [ $BITCOIN_STATUS -ne 0 ]; then
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")] ERROR: Bitcoin update failed with status $BITCOIN_STATUS" | tee -a "$LOG_FILE"
  exit 1
fi

# Print summary
echo "[$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")] Reingestion and update process completed successfully!" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "Summary for $DATE:" | tee -a "$LOG_FILE"
echo "1. Elexon data reingested for key periods" | tee -a "$LOG_FILE"
echo "2. Bitcoin calculations updated" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "See individual log files for detailed information."

exit 0