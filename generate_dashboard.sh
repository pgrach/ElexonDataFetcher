#!/bin/bash

# Generate Reconciliation Dashboard
# This script provides a user-friendly interface for checking reconciliation status

# Set up colors for output
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Timestamp for logging
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
LOG_FILE="dashboard_$(date -u +"%Y%m%d").log"

# Function to log messages
log() {
  local message="$1"
  local level="${2:-INFO}"
  local color="${NC}"
  
  case "$level" in
    "INFO") color="${BLUE}" ;;
    "SUCCESS") color="${GREEN}" ;;
    "WARNING") color="${YELLOW}" ;;
    "ERROR") color="${RED}" ;;
    *) color="${CYAN}" ;;
  esac
  
  echo -e "${color}[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [${level}] ${message}${NC}"
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] [${level}] ${message}" >> "$LOG_FILE"
}

# Function to print section header
print_header() {
  local title="$1"
  local length=${#title}
  local line=$(printf '=%.0s' $(seq 1 $((length + 4))))
  
  echo ""
  log "${PURPLE}${line}${NC}" "HEADER"
  log "${PURPLE}| ${title} |${NC}" "HEADER"
  log "${PURPLE}${line}${NC}" "HEADER"
  echo ""
}

# Function to handle errors
handle_error() {
  log "An error occurred. Exiting..." "ERROR"
  exit 1
}

# Set error handling
trap handle_error ERR

# Print welcome message
print_header "Bitcoin Mining Reconciliation Dashboard"
log "Starting dashboard generation at ${TIMESTAMP}" "INFO"

# Run the dashboard TypeScript file
log "Generating comprehensive reconciliation report..." "INFO"
npx tsx reconciliation_dashboard.ts

# Print additional operations menu
print_header "Reconciliation Operations"
echo -e "${CYAN}Choose an operation:${NC}"
echo -e "  ${GREEN}1)${NC} Fix critical date (2022-10-06)"
echo -e "  ${GREEN}2)${NC} Fix a specific date"
echo -e "  ${GREEN}3)${NC} Run auto reconciliation"
echo -e "  ${GREEN}4)${NC} Check daily reconciliation"
echo -e "  ${GREEN}q)${NC} Quit"

read -p "Enter your choice: " choice

case "$choice" in
  1)
    log "Running critical date fix for 2022-10-06..." "INFO"
    ./process_critical_date.sh 2022-10-06
    ;;
  2)
    read -p "Enter date (YYYY-MM-DD): " date_input
    if [[ "$date_input" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
      log "Processing specific date: ${date_input}" "INFO"
      ./process_critical_date.sh "$date_input"
    else
      log "Invalid date format. Please use YYYY-MM-DD." "ERROR"
    fi
    ;;
  3)
    read -p "Enter batch size (default: 5): " batch_size
    batch_size=${batch_size:-5}
    log "Running auto reconciliation with batch size ${batch_size}..." "INFO"
    ./auto_reconcile.sh "$batch_size"
    ;;
  4)
    read -p "Enter days to check (default: 2): " days
    days=${days:-2}
    log "Running daily reconciliation check for last ${days} days..." "INFO"
    npx tsx daily_reconciliation_check.ts "$days" false
    ;;
  q|Q)
    log "Exiting dashboard." "INFO"
    ;;
  *)
    log "Invalid option. Exiting." "WARNING"
    ;;
esac

log "Dashboard operations completed at $(date -u +"%Y-%m-%dT%H:%M:%SZ")" "SUCCESS"