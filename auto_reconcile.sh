#!/bin/bash

# Auto Reconciliation Script
# Automates the reconciliation process with proper error handling and logging

# Default batch size of 10 if none provided
BATCH_SIZE=${1:-"10"}

# Log setup
LOG_FILE="./logs/auto_reconciliation_$(date +%Y-%m-%d).log"

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

# Main reconciliation function
run_reconciliation() {
  log_section "Starting Auto Reconciliation"
  log "Date: $(date)"
  log "Batch Size: $BATCH_SIZE"
  
  # First check overall status
  log_section "Checking Reconciliation Status"
  npx tsx reconciliation_progress_check.ts | tee -a "$LOG_FILE"
  
  # Get top 5 dates with missing calculations
  log_section "Top 5 Dates with Missing Calculations"
  npx tsx efficient_reconciliation.ts analyze 5 | tee -a "$LOG_FILE"
  
  # Process each date with appropriate tool based on size
  log_section "Processing Dates"
  
  # First try with efficient_reconciliation 
  log "Running efficient_reconciliation with batch size $BATCH_SIZE..."
  timeout 1800 npx tsx efficient_reconciliation.ts reconcile "$BATCH_SIZE" | tee -a "$LOG_FILE" || log "⚠️ Efficient reconciliation timed out"
  
  # Get updated status
  log_section "Updated Reconciliation Status"
  npx tsx reconciliation_progress_check.ts | tee -a "$LOG_FILE"
  
  # Run connection analysis if needed
  log_section "Database Connection Analysis"
  npx tsx connection_timeout_analyzer.ts analyze | tee -a "$LOG_FILE"
  
  log_section "Reconciliation Complete"
  log "Check $LOG_FILE for complete reconciliation details"
}

# Run reconciliation with error handling
(run_reconciliation) || {
  log_section "ERROR: Reconciliation Failed"
  log "Checking system resources..."
  free -m | tee -a "$LOG_FILE"
  ps aux | grep -i postgres | tee -a "$LOG_FILE"
  log "See above for potential resource issues"
}

# Final message
log "Auto reconciliation process finished at $(date)"