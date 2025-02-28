#!/bin/bash

# Process Critical Date Script
# Focuses on reconciling a single critical date with careful error handling

# Default date is 2022-10-06 (most critical) if none provided
CRITICAL_DATE=${1:-"2022-10-06"}

# Log setup
LOG_FILE="./logs/critical_date_$CRITICAL_DATE.log"

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

check_status() {
  log "Current status for $CRITICAL_DATE:"
  npx tsx minimal_reconciliation.ts date-status "$CRITICAL_DATE" 2>/dev/null || log "⚠️ Status check failed"
}

# Main function
main() {
  log_section "Starting Critical Date Processing for $CRITICAL_DATE"
  log "Date: $(date)"
  
  # Check initial status
  log_section "Initial Status"
  check_status
  
  # First try to get missing combinations
  log_section "Analyzing Missing Combinations"
  npx tsx minimal_reconciliation.ts analyze "$CRITICAL_DATE" | tee -a "$LOG_FILE"
  
  # Process the date with extreme caution (one record at a time)
  log_section "Processing with Critical Safeguards"
  log "Using critical-date mode with minimal reconnection..."
  npx tsx minimal_reconciliation.ts critical-date "$CRITICAL_DATE" | tee -a "$LOG_FILE"
  
  # Check progress after first pass
  log_section "Status After First Pass"
  check_status
  
  # Try to process any remaining records with sequence mode
  log_section "Processing Remaining Records"
  log "Using sequence mode with batch size of 1..."
  timeout 1800 npx tsx minimal_reconciliation.ts sequence "$CRITICAL_DATE" 1 | tee -a "$LOG_FILE" || log "⚠️ Sequence processing timed out after 30 minutes"
  
  # Final status check
  log_section "Final Status"
  check_status
  
  log_section "Processing Complete for $CRITICAL_DATE"
  log "Check $LOG_FILE for complete details"
}

# Run the main function
main