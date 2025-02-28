#!/bin/bash
# Auto Reconciliation Script
# 
# This script runs daily reconciliation checks and automatically fixes missing calculations
# Can be used with cron or another scheduler for automatic daily reconciliation
#
# Usage: ./auto_reconcile.sh [options]
# Options:
#   --batch-size N  Set batch size for reconciliation (default: 5)
#   --date DATE     Process specific date (format: YYYY-MM-DD)
#   --silent        Minimize output (good for cron jobs)
#   --help          Show this help text

# Configuration
LOG_DIR="./logs"
LOG_FILE="${LOG_DIR}/auto_reconcile_$(date +%Y-%m-%d).log"
BATCH_SIZE=5
TIMEOUT=7200  # 2 hours maximum execution time
SPECIFIC_DATE=""
SILENT=false

# Parse arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --batch-size)
      BATCH_SIZE="$2"
      shift
      ;;
    --date)
      SPECIFIC_DATE="$2"
      shift
      ;;
    --silent)
      SILENT=true
      ;;
    --help)
      echo "Auto Reconciliation Script"
      echo "Usage: ./auto_reconcile.sh [options]"
      echo "Options:"
      echo "  --batch-size N  Set batch size for reconciliation (default: 5)"
      echo "  --date DATE     Process specific date (format: YYYY-MM-DD)"
      echo "  --silent        Minimize output (good for cron jobs)"
      echo "  --help          Show this help text"
      exit 0
      ;;
    *)
      echo "Unknown parameter: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
  shift
done

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Log function
log() {
  local level=$1
  local message=$2
  local timestamp=$(date +"%Y-%m-%d %H:%M:%S")
  
  # Always write to log file
  echo "[$timestamp] [$level] $message" >> "$LOG_FILE"
  
  # Output to console unless in silent mode
  if [ "$SILENT" = false ]; then
    case $level in
      "ERROR")
        echo -e "\e[31m[$timestamp] [$level] $message\e[0m"
        ;;
      "WARNING")
        echo -e "\e[33m[$timestamp] [$level] $message\e[0m"
        ;;
      "SUCCESS") 
        echo -e "\e[32m[$timestamp] [$level] $message\e[0m"
        ;;
      *)
        echo -e "\e[36m[$timestamp] [$level] $message\e[0m"
        ;;
    esac
  fi
}

# Function to handle timeout
handle_timeout() {
  log "ERROR" "Reconciliation process timed out after $TIMEOUT seconds"
  log "INFO" "Try running with a smaller batch size or use minimal_reconciliation.ts"
  exit 1
}

# Set timeout handler
trap handle_timeout SIGALRM

# Start logging
log "INFO" "Starting auto reconciliation"
log "INFO" "Batch size: $BATCH_SIZE"
if [ -n "$SPECIFIC_DATE" ]; then
  log "INFO" "Processing specific date: $SPECIFIC_DATE"
fi

# Check current reconciliation status
log "INFO" "Checking current reconciliation status"
status_output=$(npx tsx reconciliation_manager.ts status 2>&1)
echo "$status_output" >> "$LOG_FILE"

# Extract reconciliation percentage
percentage=$(echo "$status_output" | grep "Reconciliation status:" | sed -E 's/.*Reconciliation status: ([0-9.]+)%.*/\1/')

if [ -z "$percentage" ]; then
  log "ERROR" "Failed to get reconciliation status"
  exit 1
fi

log "INFO" "Current reconciliation: $percentage%"

# If already at 100%, we're done
if [ "$percentage" = "100" ]; then
  log "SUCCESS" "Already at 100% reconciliation. No action needed."
  exit 0
fi

# Process the reconciliation
if [ -n "$SPECIFIC_DATE" ]; then
  log "INFO" "Running reconciliation for date: $SPECIFIC_DATE"
  timeout $TIMEOUT npx tsx reconciliation_manager.ts date "$SPECIFIC_DATE" >> "$LOG_FILE" 2>&1
else
  log "INFO" "Running general reconciliation with batch size $BATCH_SIZE"
  timeout $TIMEOUT npx tsx reconciliation_manager.ts fix "$BATCH_SIZE" >> "$LOG_FILE" 2>&1
fi

# Check result
exit_code=$?
if [ $exit_code -eq 124 ]; then
  # Timeout occurred
  handle_timeout
elif [ $exit_code -ne 0 ]; then
  log "ERROR" "Reconciliation process failed with exit code $exit_code"
  log "INFO" "Check $LOG_FILE for details"
  
  # Try with minimal reconciliation for the most critical date
  log "INFO" "Attempting to fix most critical date with minimal reconciliation"
  npx tsx minimal_reconciliation.ts most-critical >> "$LOG_FILE" 2>&1
else
  log "SUCCESS" "Reconciliation process completed successfully"
fi

# Check final status
log "INFO" "Checking final reconciliation status"
final_status_output=$(npx tsx reconciliation_manager.ts status 2>&1)
echo "$final_status_output" >> "$LOG_FILE"

# Extract final reconciliation percentage
final_percentage=$(echo "$final_status_output" | grep "Reconciliation status:" | sed -E 's/.*Reconciliation status: ([0-9.]+)%.*/\1/')

if [ -z "$final_percentage" ]; then
  log "ERROR" "Failed to get final reconciliation status"
  exit 1
fi

log "INFO" "Final reconciliation: $final_percentage%"

# Compare percentages
if (( $(echo "$final_percentage > $percentage" | bc -l) )); then
  improvement=$(echo "$final_percentage - $percentage" | bc -l)
  log "SUCCESS" "Improved reconciliation by $improvement%"
  exit 0
else
  log "WARNING" "No improvement in reconciliation percentage"
  log "INFO" "Try running with a different batch size or use minimal_reconciliation.ts"
  exit 1
fi