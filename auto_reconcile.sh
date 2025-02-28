#!/bin/bash

# Auto Reconciliation Script
# Automates the reconciliation process with robust error handling, connection management,
# and adaptive processing based on database load

# Configuration
BATCH_SIZE=${1:-"10"}      # Default batch size
MAX_ATTEMPTS=3             # Number of retry attempts for each operation
TIMEOUT_RECONCILE=2700     # 45 minutes timeout for reconciliation
TIMEOUT_CRITICAL=3600      # 60 minutes timeout for critical date processing
TIMEOUT_ANALYZE=300        # 5 minutes timeout for analysis

# File paths
LOG_DIR="./logs"
LOG_FILE="$LOG_DIR/auto_reconciliation_$(date +%Y-%m-%d).log"
CHECKPOINT_FILE="./auto_reconciliation_checkpoint.json"

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Log function with error handling
log() {
  local message="$1"
  local timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  local formatted="[$timestamp] $message"
  
  # Write to console with error handling
  echo "$formatted" || true
  
  # Write to log file with error handling
  echo "$formatted" >> "$LOG_FILE" 2>/dev/null || true
}

log_section() {
  local section="$1"
  log "============================================================"
  log "=== $section"
  log "============================================================"
}

# Run command with retries and error handling
run_with_retry() {
  local command="$1"
  local description="$2"
  local max_attempts="${3:-$MAX_ATTEMPTS}"
  local timeout_seconds="${4:-300}"
  local attempt=1
  local result=0
  
  log "Starting operation: $description"
  
  while [ $attempt -le $max_attempts ]; do
    log "Running command (attempt $attempt/$max_attempts)..."
    
    # Run with timeout and capture result
    timeout -k 10 $timeout_seconds bash -c "$command" 2>&1 | tee -a "$LOG_FILE" || result=$?
    
    if [ $result -eq 0 ]; then
      log "✅ Operation successful: $description"
      return 0
    elif [ $result -eq 124 ] || [ $result -eq 137 ]; then
      log "⚠️ Operation timed out after $timeout_seconds seconds"
    else
      log "⚠️ Operation failed with exit code $result"
    fi
    
    # If we've reached max attempts, report failure
    if [ $attempt -eq $max_attempts ]; then
      log "❌ All retry attempts failed for: $description"
      return $result
    fi
    
    # Exponential backoff
    sleep_time=$((10 * 2**(attempt-1)))
    log "Waiting $sleep_time seconds before retry..."
    sleep $sleep_time
    
    # Increment attempt counter
    attempt=$((attempt + 1))
  done
  
  return 1
}

# Cleanup database connections
cleanup_db_connections() {
  log "Cleaning up database connections..."
  
  # Terminate connections that might be blocking other operations
  psql "$DATABASE_URL" -c "
    SELECT pg_terminate_backend(pid) 
    FROM pg_stat_activity 
    WHERE 
      (application_name LIKE '%reconciliation%' OR
      query LIKE '%curtailment_records%' OR 
      query LIKE '%historical_bitcoin_calculations%') AND
      pid <> pg_backend_pid();" 2>/dev/null || true
      
  log "Database connections cleaned up"
}

# Save checkpoint to allow resuming
save_checkpoint() {
  local status="$1"
  local last_date="$2"
  local remaining_dates="$3"
  
  # Create JSON checkpoint
  cat > "$CHECKPOINT_FILE" << EOF
{
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "status": "$status",
  "lastProcessedDate": "$last_date",
  "remainingDates": $remaining_dates,
  "batchSize": $BATCH_SIZE
}
EOF

  log "Checkpoint saved: $status"
}

# Load checkpoint if it exists
load_checkpoint() {
  if [ -f "$CHECKPOINT_FILE" ]; then
    log "Found checkpoint file, resuming previous reconciliation"
    return 0
  else
    log "No checkpoint found, starting new reconciliation"
    return 1
  fi
}

# Process a specific critical date
process_critical_date() {
  local critical_date="$1"
  
  if [ -z "$critical_date" ]; then
    log "⚠️ No critical date provided for processing"
    return 1
  fi
  
  log "Starting critical date processing for $critical_date"
  
  # Use the specialized critical date script
  run_with_retry "./process_critical_date.sh $critical_date" "Process critical date $critical_date" 2 $TIMEOUT_CRITICAL
  
  # Check result
  local result=$?
  if [ $result -eq 0 ]; then
    log "✅ Successfully processed critical date: $critical_date"
  else
    log "❌ Failed to process critical date: $critical_date (Error: $result)"
  fi
  
  return $result
}

# Try to adapt the batch size based on database performance
adapt_batch_size() {
  log "Analyzing database performance to adapt batch size..."
  
  # Run connection analyzer in analyze mode
  run_with_retry "npx tsx connection_timeout_analyzer.ts analyze" "Database performance analysis" 1 120
  
  # Check for timeout patterns in recent logs
  timeout_count=$(grep -c "timeout|EPIPE|connection" "$LOG_FILE" | tail -100)
  
  if [ $timeout_count -gt 5 ]; then
    # Reduce batch size if we're seeing connection issues
    new_batch_size=$((BATCH_SIZE / 2))
    if [ $new_batch_size -lt 1 ]; then
      new_batch_size=1
    fi
    
    log "⚠️ Detected connection issues, reducing batch size from $BATCH_SIZE to $new_batch_size"
    BATCH_SIZE=$new_batch_size
  elif [ $timeout_count -eq 0 ] && [ $BATCH_SIZE -lt 10 ]; then
    # Cautiously increase batch size if no issues
    new_batch_size=$((BATCH_SIZE + 1))
    log "✅ No connection issues detected, increasing batch size from $BATCH_SIZE to $new_batch_size"
    BATCH_SIZE=$new_batch_size
  else
    log "Keeping current batch size of $BATCH_SIZE"
  fi
}

# Main reconciliation process
run_reconciliation() {
  local start_time=$(date +%s)
  
  log_section "Starting Auto Reconciliation"
  log "Date: $(date)"
  log "Initial Batch Size: $BATCH_SIZE"
  
  # Check if we should resume from checkpoint
  if load_checkpoint; then
    log "Resuming from checkpoint"
    # TODO: Extract data from checkpoint if needed
  fi
  
  # First check overall status
  log_section "Checking Reconciliation Status"
  run_with_retry "npx tsx reconciliation_progress_check.ts" "Check reconciliation status" 2 60
  
  # Get top dates with missing calculations
  log_section "Analyzing Missing Calculations"
  run_with_retry "npx tsx efficient_reconciliation.ts analyze 5" "Analyze missing calculations" 2 $TIMEOUT_ANALYZE
  
  # Find most critical date (date with most missing records)
  log_section "Identifying Critical Dates"
  most_critical=$(run_with_retry "npx tsx minimal_reconciliation.ts most-critical" "Find most critical date" 2 60)
  critical_date=$(echo "$most_critical" | grep "Most critical date:" | awk '{print $4}')
  
  if [ -n "$critical_date" ]; then
    log "Found critical date with many missing records: $critical_date"
    log_section "Processing Critical Date First"
    process_critical_date "$critical_date"
    
    # Cleanup connections after critical date processing
    cleanup_db_connections
    sleep 10
  fi
  
  # Process regular missing records with adaptive batch size
  log_section "Processing Missing Records"
  adapt_batch_size
  log "Running efficient reconciliation with batch size $BATCH_SIZE..."
  
  run_with_retry "npx tsx efficient_reconciliation.ts reconcile $BATCH_SIZE" "Process missing records" 2 $TIMEOUT_RECONCILE
  
  # Get updated status
  log_section "Updated Reconciliation Status"
  run_with_retry "npx tsx reconciliation_progress_check.ts" "Check updated status" 2 60
  
  # Final database connection analysis
  log_section "Database Connection Analysis"
  run_with_retry "npx tsx connection_timeout_analyzer.ts analyze" "Final connection analysis" 1 120
  
  # Calculate duration
  local end_time=$(date +%s)
  local duration=$((end_time - start_time))
  local duration_minutes=$((duration / 60))
  local duration_seconds=$((duration % 60))
  
  log_section "Reconciliation Complete"
  log "Total runtime: $duration_minutes minutes, $duration_seconds seconds"
  log "Final batch size: $BATCH_SIZE"
  log "Check $LOG_FILE for complete reconciliation details"
  
  # Clean up checkpoint file on successful completion
  if [ -f "$CHECKPOINT_FILE" ]; then
    rm "$CHECKPOINT_FILE"
    log "Checkpoint file removed after successful completion"
  fi
}

# Set up trap to clean up database connections on exit
trap cleanup_db_connections EXIT INT TERM

# Run reconciliation with comprehensive error handling
{
  run_reconciliation
  exit_code=$?
  
  if [ $exit_code -eq 0 ]; then
    log_section "✅ AUTO RECONCILIATION COMPLETED SUCCESSFULLY"
  else
    log_section "❌ AUTO RECONCILIATION FAILED WITH ERROR CODE $exit_code"
    log "Checking system resources..."
    free -m | tee -a "$LOG_FILE" || true
    ps aux | grep -i postgres | tee -a "$LOG_FILE" || true
    log "See above for potential resource issues"
    
    # Save checkpoint for later resumption
    save_checkpoint "failed" "" "[]"
  fi
} || {
  log_section "❌ FATAL ERROR IN AUTO RECONCILIATION"
  log "Unexpected error occurred in the main script"
  
  # Save checkpoint for later resumption
  save_checkpoint "crashed" "" "[]"
}

# Final message
log "Auto reconciliation process finished at $(date)"