#!/bin/bash

# Auto Reconciliation Script
# This script automates the reconciliation process between curtailment_records and historical_bitcoin_calculations tables.
# It uses various strategies to handle different data volumes and potential timeout issues.

# Log setup
LOG_FILE="./logs/auto_reconciliation_$(date +%Y-%m-%d).log"
CHECKPOINT_FILE="./reconciliation_checkpoint.json"

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

# Monitor execution time and restart if needed
execute_with_timeout() {
  local command="$1"
  local timeout_seconds=${2:-300}  # Default timeout: 5 minutes
  local description=${3:-"Command"}
  
  log "Executing: $command (timeout: ${timeout_seconds}s)"
  
  timeout $timeout_seconds bash -c "$command" &
  local pid=$!
  
  local start_time=$(date +%s)
  while ps -p $pid > /dev/null; do
    local current_time=$(date +%s)
    local elapsed=$((current_time - start_time))
    
    if [ $elapsed -ge $timeout_seconds ]; then
      log "⚠️ $description timed out after ${elapsed}s. Killing process..."
      kill -9 $pid 2>/dev/null
      wait $pid 2>/dev/null
      return 1
    fi
    
    sleep 2
  done
  
  wait $pid
  local exit_code=$?
  
  if [ $exit_code -eq 0 ]; then
    log "✅ $description completed successfully"
    return 0
  else
    log "❌ $description failed with exit code $exit_code"
    return $exit_code
  fi
}

# Run a database analysis to check for potential issues
analyze_database() {
  log_section "Database Analysis"
  execute_with_timeout "npx tsx connection_timeout_analyzer.ts analyze" 60 "Database analysis"
}

# Process most critical dates first (ones with most missing calculations)
process_critical_dates() {
  log_section "Processing Critical Dates"
  local dates_to_process=${1:-5}  # Default: process top 5 critical dates
  
  # Get most critical dates
  log "Identifying the $dates_to_process most critical dates..."
  
  # Skip this step if minimal_reconciliation produces no output
  if ! execute_with_timeout "npx tsx minimal_reconciliation.ts most-critical" 120 "Finding most critical date"; then
    log "⚠️ Failed to find critical dates through minimal_reconciliation, using alternative method."
    execute_with_timeout "npx tsx efficient_reconciliation.ts analyze" 120 "Analyzing reconciliation status"
    return 1
  fi
  
  # Process was successful
  log "✅ Critical date processing completed"
  return 0
}

# Process a specific date range
process_date_range() {
  local start_date=$1
  local end_date=$2
  local batch_size=${3:-20}  # Default batch size: 20
  
  log_section "Processing Date Range: $start_date to $end_date (batch size: $batch_size)"
  
  execute_with_timeout "npx tsx efficient_reconciliation.ts range $start_date $end_date $batch_size" 900 "Date range reconciliation"
  
  # Check if it failed due to timeout, try smaller batches
  if [ $? -ne 0 ]; then
    log "⚠️ Date range processing failed, trying with smaller batch size"
    execute_with_timeout "npx tsx efficient_reconciliation.ts range $start_date $end_date 5" 900 "Date range reconciliation (small batch)"
  fi
}

# Process highest-value month (where most calculations are missing)
process_high_value_month() {
  log_section "Processing High Value Month"
  
  # Get current date components
  local current_year=$(date +%Y)
  local current_month=$(date +%m)
  
  # Process older months with missing data (targeting October 2022)
  log "Processing data for October 2022 (high-value month)"
  process_date_range "2022-10-01" "2022-10-31" 10
  
  # Also process June 2022 which has several days in the top missing dates
  log "Processing data for June 2022 (high-value month)"
  process_date_range "2022-06-10" "2022-06-15" 5
}

# Process most recent month to ensure up-to-date data
process_recent_month() {
  log_section "Processing Recent Month"
  
  # Get date for start of previous month
  local prev_month_start=$(date -d "$(date +%Y-%m-01) -1 month" +%Y-%m-01)
  # Get date for end of previous month
  local prev_month_end=$(date -d "$prev_month_start +1 month -1 day" +%Y-%m-%d)
  
  log "Processing previous month: $prev_month_start to $prev_month_end"
  process_date_range "$prev_month_start" "$prev_month_end" 50
  
  # Process current month data
  local current_month_start=$(date +%Y-%m-01)
  local today=$(date +%Y-%m-%d)
  
  log "Processing current month to date: $current_month_start to $today"
  process_date_range "$current_month_start" "$today" 50
}

# Generate a progress report
generate_report() {
  log_section "Generating Reconciliation Report"
  
  execute_with_timeout "npx tsx reconciliation_progress_report.ts" 60 "Progress report generation"
  
  log "Current reconciliation status:"
  npx tsx reconciliation_progress_check.ts | tee -a "$LOG_FILE"
}

# Check if we're already reconciled to an acceptable level
check_reconciliation_level() {
  log "Checking current reconciliation percentage..."
  
  # Run the progress check and capture the output
  local progress_output=$(npx tsx reconciliation_progress_check.ts)
  
  # Extract the completion percentage (this regex looks for a percentage value)
  local percentage=$(echo "$progress_output" | grep -o "Completion percentage: [0-9.]\+%" | grep -o "[0-9.]\+")
  
  if [ -z "$percentage" ]; then
    log "⚠️ Unable to determine current reconciliation percentage"
    return 1
  fi
  
  log "Current reconciliation percentage: $percentage%"
  
  # Check if we've reached the target (75% or higher)
  if (( $(echo "$percentage >= 75" | bc -l) )); then
    log "✅ Reconciliation target met or exceeded: $percentage% (target: 75%)"
    return 0
  else
    log "⚠️ Reconciliation target not yet met: $percentage% (target: 75%)"
    return 1
  fi
}

# Main execution
main() {
  log_section "Starting Auto Reconciliation"
  log "Date: $(date)"
  
  # Check if we already have a good reconciliation level
  if check_reconciliation_level; then
    log "Reconciliation already at acceptable level. Running only recent data updates."
    process_recent_month
    generate_report
    return 0
  fi
  
  # Step 1: Analyze database connection for potential issues
  analyze_database
  
  # Step 2: Process critical dates first (most problematic)
  process_critical_dates 3
  
  # Step 3: Process high-value months
  process_high_value_month
  
  # Step 4: Process recent data to ensure up-to-date records
  process_recent_month
  
  # Step 5: Generate final report
  generate_report
  
  log_section "Auto Reconciliation Complete"
  log "Reconciliation processing completed. Check the report for details."
}

# Run the main function
main