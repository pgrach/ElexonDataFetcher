#!/bin/bash

# Process Critical Batch Script
# Processes a batch of the most critical dates in sequence for reconciliation

# Log setup
LOG_FILE="./logs/critical_batch_$(date +%Y-%m-%d).log"

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

# Set of most critical dates to process
CRITICAL_DATES=(
  "2022-10-06"
  "2022-06-11"
  "2022-11-10"
  "2022-06-12"
  "2022-10-09"
)

# Maximum processing time for each date (in seconds)
MAX_TIME=1800  # 30 minutes

process_date() {
  local date="$1"
  log_section "Processing Critical Date: $date"
  
  # First try to fetch some minimal data to avoid overloading
  log "Getting a small batch from $date using efficient_reconciliation..."
  timeout 300 npx tsx efficient_reconciliation.ts date "$date" 5 || log "⚠️ Efficient reconciliation timed out for $date"
  
  # Then process using the minimal tool with very small batch size
  log "Processing $date using minimal_reconciliation..."
  timeout $MAX_TIME npx tsx minimal_reconciliation.ts sequence "$date" 1 || log "⚠️ Minimal reconciliation timed out for $date after $MAX_TIME seconds"
  
  # Let's check how much progress we made
  log "Progress for $date:"
  npx tsx efficient_reconciliation.ts date-status "$date" 2>/dev/null || log "⚠️ Status check failed"
  
  # Add a break between dates to let connections settle
  log "Pausing for 30 seconds before next date..."
  sleep 30
}

# Main function
main() {
  log_section "Starting Critical Batch Processing"
  log "Date: $(date)"
  log "Processing ${#CRITICAL_DATES[@]} critical dates"
  
  # Process each date in sequence
  for date in "${CRITICAL_DATES[@]}"; do
    process_date "$date"
  done
  
  # Check overall status when done
  log_section "Final Reconciliation Status"
  npx tsx reconciliation_progress_check.ts | tee -a "$LOG_FILE"
  
  log_section "Critical Batch Processing Complete"
  log "Check the logs for details on each date's processing"
}

# Run the main function
main