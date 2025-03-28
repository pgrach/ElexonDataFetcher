#!/bin/bash
# Script to update Elexon data in batches to handle potential timeouts
START_PERIOD=$1
END_PERIOD=$2

# Set defaults if not provided
[[ -z "$START_PERIOD" ]] && START_PERIOD=26
[[ -z "$END_PERIOD" ]] && END_PERIOD=48

echo "Starting Elexon data update from period $START_PERIOD to $END_PERIOD"

# Process in batches of 5 periods
current=$START_PERIOD
batch_size=5

while [ $current -le $END_PERIOD ]; do
  end_batch=$((current + batch_size - 1))
  
  # Make sure we don't go beyond END_PERIOD
  [[ $end_batch -gt $END_PERIOD ]] && end_batch=$END_PERIOD
  
  echo "Processing batch: periods $current to $end_batch"
  
  # Run the update script for this batch
  npx tsx check_and_update_2025_03_27.ts --start-period $current --end-period $end_batch
  
  # Capture exit status
  status=$?
  
  if [ $status -ne 0 ]; then
    echo "Error processing periods $current to $end_batch (exit code: $status)"
    echo "Trying to continue with next batch..."
  fi
  
  # Sleep between batches to avoid rate limiting
  echo "Sleeping for 3 seconds before next batch..."
  sleep 3
  
  # Move to next batch
  current=$((end_batch + 1))
done

echo "Elexon data update complete!"
