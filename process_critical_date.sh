#!/bin/bash

# Process Critical Date Script
# Focuses on reconciling a single critical date with careful error handling and protection
# against connection and pipe errors.

# Default date is 2022-10-06 (most critical) if none provided
CRITICAL_DATE=${1:-"2022-10-06"}

# Log setup
LOG_DIR="./logs"
PROCESS_LOG="$LOG_DIR/process_critical_date_$CRITICAL_DATE.log"
CRITICAL_LOG="$LOG_DIR/critical_date_$CRITICAL_DATE.log"
MINIMAL_LOG="./minimal_reconciliation.log"

# Ensure log directories exist
mkdir -p "$LOG_DIR"

# Clean up any existing empty log files
[ -f "$PROCESS_LOG" ] && [ ! -s "$PROCESS_LOG" ] && rm "$PROCESS_LOG"
[ -f "$CRITICAL_LOG" ] && [ ! -s "$CRITICAL_LOG" ] && rm "$CRITICAL_LOG"

# Backup functions to ensure we don't lose earlier logs
backup_logs() {
  local timestamp=$(date -u +"%Y%m%d%H%M%S")
  
  # Backup minimal log if it exists and has content
  if [ -f "$MINIMAL_LOG" ] && [ -s "$MINIMAL_LOG" ]; then
    cp "$MINIMAL_LOG" "$MINIMAL_LOG.$timestamp.bak"
  fi
  
  # Truncate minimal log to prevent it from growing too large
  echo "=== New Run $(date -u) ===" > "$MINIMAL_LOG"
}

# Log function with error handling
log() {
  local message="$1"
  local timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  local formatted="[$timestamp] $message"
  
  # Write to console with error handling
  echo "$formatted" || true
  
  # Write to log file with error handling
  echo "$formatted" >> "$PROCESS_LOG" 2>/dev/null || true
}

log_section() {
  local section="$1"
  log "============================================================"
  log "=== $section"
  log "============================================================"
}

# Run with retries and proper error handling
run_with_retry() {
  local command="$1"
  local retry_count=3
  local attempt=1
  local result=0
  
  while [ $attempt -le $retry_count ]; do
    log "Running command (attempt $attempt/$retry_count): $command"
    
    # Run command and capture its output while still showing it on screen
    # Redirect stderr to stdout to capture both
    eval "$command" 2>&1 | tee -a "$PROCESS_LOG" || result=$?
    
    if [ $result -eq 0 ]; then
      return 0
    else
      log "⚠️ Command failed with exit code $result"
      
      # If we've hit the retry limit, give up
      if [ $attempt -eq $retry_count ]; then
        log "❌ All retry attempts failed"
        return $result
      fi
      
      # Wait longer between retries
      sleep_time=$((5 * attempt))
      log "Waiting $sleep_time seconds before retry..."
      sleep $sleep_time
      
      # Increment attempt counter
      attempt=$((attempt + 1))
    fi
  done
}

# Check reconciliation status for the date
check_status() {
  log "Checking reconciliation status for $CRITICAL_DATE..."
  
  # Get status from the database directly to avoid potential script issues
  pg_status=$(psql "$DATABASE_URL" -t -c "
    WITH curtailment_summary AS (
      SELECT 
        COUNT(DISTINCT (settlement_period || '-' || farm_id)) * 3 as expected_count
      FROM curtailment_records
      WHERE settlement_date = '$CRITICAL_DATE'
    ),
    bitcoin_summary AS (
      SELECT 
        COUNT(*) as actual_count
      FROM historical_bitcoin_calculations
      WHERE settlement_date = '$CRITICAL_DATE'
    )
    SELECT 
      COALESCE(cs.expected_count, 0) as expected,
      COALESCE(bs.actual_count, 0) as actual,
      CASE 
        WHEN COALESCE(cs.expected_count, 0) = 0 THEN 100
        ELSE ROUND((COALESCE(bs.actual_count, 0) * 100.0) / cs.expected_count, 2)
      END as percentage
    FROM curtailment_summary cs
    CROSS JOIN bitcoin_summary bs;
  " 2>/dev/null)
  
  if [ $? -eq 0 ] && [ -n "$pg_status" ]; then
    log "Current status: $pg_status"
  else
    log "Failed to get status from database directly, trying minimal_reconciliation.ts..."
    run_with_retry "npx tsx minimal_reconciliation.ts sequence \"$CRITICAL_DATE\" 0" || log "⚠️ Status check failed"
  fi
}

# Clean up function to ensure we close connections properly
cleanup() {
  log "Cleaning up and ensuring connections are closed..."
  
  # Attempt to close any lingering DB connections
  psql "$DATABASE_URL" -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE 
    application_name = 'minimal_reconciliation' OR 
    (query LIKE '%curtailment_records%' AND query LIKE '%historical_bitcoin_calculations%');" 2>/dev/null || true
    
  log "Cleanup complete."
}

# Main function with enhanced error handling
main() {
  # Backup existing logs
  backup_logs
  
  log_section "Starting Critical Date Processing for $CRITICAL_DATE"
  log "Date: $(date)"
  
  # Set up trap to clean up on exit
  trap cleanup EXIT INT TERM
  
  # Check initial status
  log_section "Initial Status"
  check_status
  
  # First try to get missing combinations
  log_section "Analyzing Missing Combinations"
  
  # This is a dry run to just identify missing records
  run_with_retry "npx tsx minimal_reconciliation.ts sequence \"$CRITICAL_DATE\" 0"
  
  # Process the date with extreme caution (one record at a time)
  log_section "Processing with Critical Safeguards"
  log "Using critical-date mode with minimal reconnection..."
  
  # Run with a 45-minute timeout which should be enough for most critical dates
  timeout -k 10 2700 run_with_retry "npx tsx minimal_reconciliation.ts critical-date \"$CRITICAL_DATE\""
  
  # Check progress after first pass
  log_section "Status After First Pass"
  check_status
  
  # Rest a bit to let connections close
  log "Pausing for 30 seconds to allow connections to reset..."
  sleep 30
  
  # Only attempt sequence mode if we don't have 100% completion
  if ! psql "$DATABASE_URL" -t -c "
    WITH curtailment_summary AS (
      SELECT COUNT(DISTINCT (settlement_period || '-' || farm_id)) * 3 as expected_count
      FROM curtailment_records WHERE settlement_date = '$CRITICAL_DATE'
    ),
    bitcoin_summary AS (
      SELECT COUNT(*) as actual_count
      FROM historical_bitcoin_calculations WHERE settlement_date = '$CRITICAL_DATE'
    )
    SELECT CASE WHEN bs.actual_count = cs.expected_count THEN 1 ELSE 0 END as is_complete
    FROM curtailment_summary cs
    CROSS JOIN bitcoin_summary bs;" 2>/dev/null | grep -q '1'; then
    
    # Try to process any remaining records with sequence mode
    log_section "Processing Remaining Records"
    log "Using sequence mode with batch size of 1..."
    
    # Run with a 30-minute timeout
    timeout -k 10 1800 run_with_retry "npx tsx minimal_reconciliation.ts sequence \"$CRITICAL_DATE\" 1" || 
      log "⚠️ Sequence processing timed out after 30 minutes"
  else
    log_section "Processing Complete"
    log "✅ All records for $CRITICAL_DATE have been successfully processed"
  fi
  
  # Final status check
  log_section "Final Status"
  check_status
  
  log_section "Processing Complete for $CRITICAL_DATE"
  log "Check $PROCESS_LOG and $MINIMAL_LOG for complete details"
  
  # Copy any output from minimal_reconciliation.log to our process log for completeness
  if [ -f "$MINIMAL_LOG" ]; then
    log "Appending minimal reconciliation log to process log..."
    echo "\n=== MINIMAL RECONCILIATION LOG ===" >> "$PROCESS_LOG"
    cat "$MINIMAL_LOG" >> "$PROCESS_LOG" 2>/dev/null || true
  fi
  
  # Final success message
  log "✅ Critical date processing complete at $(date -u)"
}

# Run the main function with overall error handling
{
  main
} || {
  log "❌ Critical error occurred during processing. Check logs for details."
  cleanup
  exit 1
}