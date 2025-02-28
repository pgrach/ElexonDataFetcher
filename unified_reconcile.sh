#!/bin/bash

# Unified Reconciliation System Shell Script
# This script simplifies running the unified_reconciliation.ts module

# Display help message
function show_help {
  echo "Usage: ./unified_reconcile.sh [command] [options]"
  echo ""
  echo "Commands:"
  echo "  status                 - Show current reconciliation status"
  echo "  analyze                - Analyze missing calculations and detect issues"
  echo "  reconcile [batchSize]  - Process all missing calculations with specified batch size"
  echo "  date YYYY-MM-DD        - Process a specific date"
  echo "  range YYYY-MM-DD YYYY-MM-DD [batchSize] - Process a date range"
  echo "  critical DATE          - Process a problematic date with extra safeguards"
  echo "  spot-fix DATE PERIOD FARM - Fix a specific date-period-farm combination"
  echo "  help                   - Show this help message"
  echo ""
  echo "Examples:"
  echo "  ./unified_reconcile.sh status"
  echo "  ./unified_reconcile.sh date 2025-02-25"
  echo "  ./unified_reconcile.sh reconcile 10"
  echo "  ./unified_reconcile.sh range 2025-02-01 2025-02-28 5"
}

# Check if npx is available
if ! command -v npx &> /dev/null; then
  echo "Error: npx is required but not found. Please ensure Node.js is installed correctly."
  exit 1
fi

# Check if tsx is available via npx
if ! npx tsx --version &> /dev/null; then
  echo "Error: tsx is required but not found. It may not be installed."
  echo "You can install it with: npm install -g tsx"
  exit 1
fi

# Parse command line arguments
if [ $# -eq 0 ] || [ "$1" == "help" ]; then
  show_help
  exit 0
fi

command="$1"
shift

# Execute the command
case "$command" in
  status)
    echo "Fetching reconciliation status..."
    npx tsx unified_reconciliation.ts status
    ;;
    
  analyze)
    echo "Analyzing reconciliation status..."
    npx tsx unified_reconciliation.ts analyze
    ;;
    
  reconcile)
    batch_size=${1:-5}
    echo "Running reconciliation with batch size $batch_size..."
    npx tsx unified_reconciliation.ts reconcile "$batch_size"
    ;;
    
  date)
    if [ -z "$1" ]; then
      echo "Error: Date is required"
      show_help
      exit 1
    fi
    
    echo "Processing date $1..."
    npx tsx unified_reconciliation.ts date "$1"
    ;;
    
  range)
    if [ -z "$1" ] || [ -z "$2" ]; then
      echo "Error: Start and end dates are required"
      show_help
      exit 1
    fi
    
    start_date="$1"
    end_date="$2"
    batch_size=${3:-5}
    
    echo "Processing date range from $start_date to $end_date with batch size $batch_size..."
    npx tsx unified_reconciliation.ts range "$start_date" "$end_date" "$batch_size"
    ;;
    
  critical)
    if [ -z "$1" ]; then
      echo "Error: Date is required"
      show_help
      exit 1
    fi
    
    echo "Processing critical date $1 with extra safeguards..."
    npx tsx unified_reconciliation.ts critical "$1"
    ;;
    
  spot-fix)
    if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ]; then
      echo "Error: Date, period, and farm ID are required"
      show_help
      exit 1
    fi
    
    date="$1"
    period="$2"
    farm_id="$3"
    
    echo "Fixing specific date-period-farm combination: $date P$period $farm_id..."
    npx tsx unified_reconciliation.ts spot-fix "$date" "$period" "$farm_id"
    ;;
    
  *)
    echo "Error: Unknown command '$command'"
    show_help
    exit 1
    ;;
esac

exit 0