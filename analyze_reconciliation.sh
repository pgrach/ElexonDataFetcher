#!/bin/bash

# Reconciliation Analysis Script
# Provides a detailed analysis of the reconciliation status

# Log setup
LOG_FILE="./logs/analysis_$(date +%Y-%m-%d).log"

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

# Main analysis function
run_analysis() {
  log_section "Starting Reconciliation Analysis"
  log "Date: $(date)"
  
  # Check overall status
  log_section "Overall Reconciliation Status"
  npx tsx reconciliation_progress_check.ts | tee -a "$LOG_FILE"
  
  # Generate database connection analysis
  log_section "Database Connection Analysis"
  timeout 60 npx tsx connection_timeout_analyzer.ts test | tee -a "$LOG_FILE" || log "⚠️ Connection analysis timed out"
  
  # Check critical dates progress
  log_section "Critical Dates Status"
  log "2022-10-06:"
  npx tsx efficient_reconciliation.ts date-status 2022-10-06 2>/dev/null | tee -a "$LOG_FILE" || log "⚠️ Status check failed"
  
  log "2022-06-11:"
  npx tsx efficient_reconciliation.ts date-status 2022-06-11 2>/dev/null | tee -a "$LOG_FILE" || log "⚠️ Status check failed"
  
  log "2022-11-10:"
  npx tsx efficient_reconciliation.ts date-status 2022-11-10 2>/dev/null | tee -a "$LOG_FILE" || log "⚠️ Status check failed"
  
  # Check current month status
  current_month=$(date +%Y-%m)
  log_section "Current Month Status ($current_month)"
  
  # Execute SQL query to get current month status
  log "Executing SQL query for current month..."
  psql "$DATABASE_URL" -c "
  WITH curtailment_by_date AS (
    SELECT 
      settlement_date,
      COUNT(DISTINCT (settlement_period || '-' || farm_id)) * 3 as expected_count
    FROM curtailment_records
    WHERE volume::numeric != 0
      AND TO_CHAR(settlement_date::date, 'YYYY-MM') = '$current_month'
    GROUP BY settlement_date
  ),
  bitcoin_by_date AS (
    SELECT 
      settlement_date,
      COUNT(*) as actual_count
    FROM historical_bitcoin_calculations
    WHERE TO_CHAR(settlement_date::date, 'YYYY-MM') = '$current_month'
    GROUP BY settlement_date
  )
  SELECT 
    cd.settlement_date as date,
    cd.expected_count,
    COALESCE(bd.actual_count, 0) as actual_count,
    CASE 
      WHEN cd.expected_count = 0 THEN 100
      ELSE ROUND((COALESCE(bd.actual_count, 0) * 100.0) / cd.expected_count, 2)
    END as completion_percentage
  FROM curtailment_by_date cd
  LEFT JOIN bitcoin_by_date bd ON cd.settlement_date = bd.settlement_date
  ORDER BY cd.settlement_date;
  " | tee -a "$LOG_FILE" || log "⚠️ SQL query failed"
  
  log_section "Analysis Complete"
  log "Results saved to $LOG_FILE"
}

# Execute the analysis
run_analysis