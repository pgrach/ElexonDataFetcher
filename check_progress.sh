#!/bin/bash
# Script to check progress of the Elexon data update

LOG_FILE="check_update_2025_03_27_$(date +%Y-%m-%d).log"
UPDATE_LOG="update_run.log"

# Check which log file to use
if [ -f "$UPDATE_LOG" ] && [ -s "$UPDATE_LOG" ] && [ $(grep -c "Processing period" "$UPDATE_LOG") -gt 0 ]; then
  ACTIVE_LOG="$UPDATE_LOG"
  echo "Using active update log file: $UPDATE_LOG"
elif [ -f "$LOG_FILE" ]; then
  ACTIVE_LOG="$LOG_FILE"
  echo "Using daily log file: $LOG_FILE"
else
  echo "No log file found. Tried $UPDATE_LOG and $LOG_FILE."
  exit 1
fi

echo "Progress summary for Elexon data update on 2025-03-27:"
echo "======================================================="

# Get the highest period processed
highest_period=$(grep "Processing period" "$ACTIVE_LOG" | tail -1 | grep -oE "period [0-9]+" | awk '{print $2}')
echo "Current progress: Processing period $highest_period of 48"

# Count updated records
updated_count=$(grep "Updated " "$ACTIVE_LOG" | grep -oE "[0-9]+ records" | awk '{sum += $1} END {print sum}')
echo "Updated records so far: $updated_count"

# Count records by category
missing=$(grep "missing" "$ACTIVE_LOG" | grep -oE "[0-9]+ missing" | awk '{sum += $1} END {print sum}')
different=$(grep "different" "$ACTIVE_LOG" | grep -oE "[0-9]+ different" | awk '{sum += $1} END {print sum}')
identical=$(grep "identical" "$ACTIVE_LOG" | grep -oE "[0-9]+ identical" | awk '{sum += $1} END {print sum}')

echo "Summary stats:"
echo "  - Missing records found: $missing"
echo "  - Different records found: $different"
echo "  - Identical records found: $identical"

# Check if complete
if grep -q "Script execution complete" "$ACTIVE_LOG"; then
  echo "Status: COMPLETED"
  
  # Extract final stats
  final_stats=$(grep "Updated DB state:" "$ACTIVE_LOG" | tail -1)
  echo "Final database state:"
  echo "$final_stats"
else
  echo "Status: RUNNING"
  
  # Estimate time remaining based on periods processed
  if [ ! -z "$highest_period" ]; then
    current_period=$highest_period
    remaining_periods=$((48 - current_period))
    
    # Calculate average time per period
    start_time=$(head -10 "$ACTIVE_LOG" | grep -oE "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}" | head -1)
    current_time=$(tail -10 "$ACTIVE_LOG" | grep -oE "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}" | tail -1)
    
    if [ ! -z "$start_time" ] && [ ! -z "$current_time" ]; then
      start_seconds=$(date -d "$start_time" +%s)
      current_seconds=$(date -d "$current_time" +%s)
      elapsed_seconds=$((current_seconds - start_seconds))
      
      if [ "$current_period" -gt 0 ]; then
        seconds_per_period=$((elapsed_seconds / current_period))
        remaining_seconds=$((remaining_periods * seconds_per_period))
        
        # Convert to minutes for display
        remaining_minutes=$((remaining_seconds / 60))
        echo "Estimated time remaining: ~$remaining_minutes minutes"
      fi
    fi
  fi
fi