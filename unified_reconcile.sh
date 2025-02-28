#!/bin/bash

# Unified Reconciliation Shell Script
# A wrapper for the unified_reconciliation.ts module providing easy command-line access to
# all reconciliation functions

# Text formatting
BOLD="\033[1m"
RED="\033[31m"
GREEN="\033[32m"
YELLOW="\033[33m"
BLUE="\033[34m"
RESET="\033[0m"

# Banner
function show_banner() {
  echo -e "${BOLD}${BLUE}===== Unified Reconciliation System =====${RESET}"
  echo -e "A comprehensive data reconciliation tool for Bitcoin mining calculations"
  echo ""
}

# Show help
function show_help() {
  show_banner
  echo -e "${BOLD}Usage:${RESET}"
  echo -e "  $0 ${BOLD}command${RESET} [options]"
  echo ""
  echo -e "${BOLD}Commands:${RESET}"
  echo -e "  ${BOLD}status${RESET}                 Show current reconciliation status"
  echo -e "  ${BOLD}analyze${RESET}                Analyze missing calculations and detect issues"
  echo -e "  ${BOLD}reconcile${RESET} [batchSize]  Process all missing calculations with specified batch size"
  echo -e "  ${BOLD}date${RESET} YYYY-MM-DD        Process a specific date"
  echo -e "  ${BOLD}range${RESET} START END [size] Process a date range with optional batch size"
  echo -e "  ${BOLD}critical${RESET} DATE          Process a problematic date with extra safeguards"
  echo -e "  ${BOLD}spot-fix${RESET} DATE PERIOD FARM Fix a specific date-period-farm combination"
  echo -e "  ${BOLD}help${RESET}                   Show this help message"
  echo ""
  echo -e "${BOLD}Examples:${RESET}"
  echo -e "  $0 status                    # Show current status"
  echo -e "  $0 date 2025-02-28           # Process February 28, 2025"
  echo -e "  $0 range 2025-02-01 2025-02-28 10  # Process February 2025 with batch size 10"
  echo -e "  $0 reconcile 20              # Process all missing calculations with batch size 20"
  echo ""
  echo -e "${BOLD}Tips:${RESET}"
  echo -e "  - Use ${BOLD}date${RESET} for processing a single day"
  echo -e "  - Use ${BOLD}critical${RESET} for dates that consistently timeout"
  echo -e "  - Use ${BOLD}spot-fix${RESET} for targeted repairs of specific records"
  echo -e "  - Monitor ${BOLD}reconciliation.log${RESET} for detailed output"
  echo ""
}

# Functions to handle different commands
function run_status() {
  echo -e "${BOLD}Running reconciliation status check...${RESET}"
  npx tsx unified_reconciliation.ts status
}

function run_analyze() {
  echo -e "${BOLD}Analyzing reconciliation status...${RESET}"
  npx tsx unified_reconciliation.ts analyze
}

function run_reconcile() {
  local batch_size=${1:-10}  # Default batch size is 10
  echo -e "${BOLD}Processing all missing calculations with batch size ${batch_size}...${RESET}"
  npx tsx unified_reconciliation.ts reconcile $batch_size
}

function run_date() {
  local date=$1
  if [[ ! $date =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo -e "${RED}Error: Invalid date format. Please use YYYY-MM-DD.${RESET}"
    exit 1
  fi
  echo -e "${BOLD}Processing date ${date}...${RESET}"
  npx tsx unified_reconciliation.ts date $date
}

function run_range() {
  local start_date=$1
  local end_date=$2
  local batch_size=${3:-10}  # Default batch size is 10
  
  if [[ ! $start_date =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || [[ ! $end_date =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo -e "${RED}Error: Invalid date format. Please use YYYY-MM-DD.${RESET}"
    exit 1
  fi
  
  echo -e "${BOLD}Processing date range from ${start_date} to ${end_date} with batch size ${batch_size}...${RESET}"
  npx tsx unified_reconciliation.ts range $start_date $end_date $batch_size
}

function run_critical() {
  local date=$1
  if [[ ! $date =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo -e "${RED}Error: Invalid date format. Please use YYYY-MM-DD.${RESET}"
    exit 1
  fi
  echo -e "${BOLD}Processing critical date ${date} with extra safeguards...${RESET}"
  npx tsx unified_reconciliation.ts critical $date
}

function run_spot_fix() {
  local date=$1
  local period=$2
  local farm_id=$3
  
  if [[ ! $date =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo -e "${RED}Error: Invalid date format. Please use YYYY-MM-DD.${RESET}"
    exit 1
  fi
  
  if [[ ! $period =~ ^[0-9]+$ ]]; then
    echo -e "${RED}Error: Period must be a number.${RESET}"
    exit 1
  fi
  
  echo -e "${BOLD}Spot-fixing date=${date}, period=${period}, farm=${farm_id}...${RESET}"
  npx tsx unified_reconciliation.ts spot-fix $date $period $farm_id
}

# Main function to parse arguments and execute commands
function main() {
  if [ $# -eq 0 ]; then
    show_help
    exit 0
  fi

  command=$1
  shift  # Remove the command from arguments

  case $command in
    status)
      run_status
      ;;
    analyze)
      run_analyze
      ;;
    reconcile)
      run_reconcile $1
      ;;
    date)
      if [ -z "$1" ]; then
        echo -e "${RED}Error: Date argument is required.${RESET}"
        exit 1
      fi
      run_date $1
      ;;
    range)
      if [ -z "$1" ] || [ -z "$2" ]; then
        echo -e "${RED}Error: Start and end date arguments are required.${RESET}"
        exit 1
      fi
      run_range $1 $2 $3
      ;;
    critical)
      if [ -z "$1" ]; then
        echo -e "${RED}Error: Date argument is required.${RESET}"
        exit 1
      fi
      run_critical $1
      ;;
    spot-fix)
      if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ]; then
        echo -e "${RED}Error: Date, period, and farm ID arguments are required.${RESET}"
        exit 1
      fi
      run_spot_fix $1 $2 $3
      ;;
    help)
      show_help
      ;;
    *)
      echo -e "${RED}Error: Unknown command '${command}'.${RESET}"
      show_help
      exit 1
      ;;
  esac
}

# Execute the main function with all the arguments
main "$@"