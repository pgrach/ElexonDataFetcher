#!/bin/bash

# Process Critical Date Script
# Focused script to process a single critical date with minimal reconciliation tool

# Usage: ./process_critical_date.sh <date>
# Example: ./process_critical_date.sh 2022-10-06

# Log setup
LOG_FILE="./logs/critical_date_$(date +%Y-%m-%d).log"

# Create logs directory if it doesn't exist
mkdir -p ./logs

log() {
  local message="$1"
  local timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  echo "[$timestamp] $message" | tee -a "$LOG_FILE"
}

log_section() {
  local section="$1"
  log "============================================================"
  log "=== $section"
  log "============================================================"
}

# Get the date to process
DATE="${1:-2022-10-06}"  # Default to 2022-10-06 if no date provided

# Process the critical date with minimal tool
log_section "Starting Critical Date Processing for $DATE"

log "Using minimal_reconciliation.ts with sequence mode for $DATE"
log "This processes records one-by-one with careful error handling"

# Execute with a 30-minute timeout
timeout 1800 npx tsx minimal_reconciliation.ts sequence "$DATE" 1

result=$?
if [ $result -eq 124 ]; then
  log "⚠️ Process timed out after 30 minutes but may have made progress"
elif [ $result -eq 0 ]; then
  log "✅ Process completed successfully"
else
  log "❌ Process failed with exit code $result"
fi

# Check current status
log_section "Checking Current Status"
npx tsx reconciliation_progress_check.ts | tee -a "$LOG_FILE"

log_section "Processing Complete"
log "Date processed: $DATE"
log "Check the logs and reconciliation progress for details"