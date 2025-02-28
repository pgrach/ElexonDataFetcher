#!/bin/bash

# Unified Reconciliation Runner
# A simple wrapper around the unified_reconciliation.ts script

set -e  # Exit on error

# Terminal colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
  echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] $1${NC}"
}

# Error logging function
error() {
  echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}" >&2
}

# Success logging function
success() {
  echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] SUCCESS: $1${NC}"
}

# Warning logging function
warning() {
  echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

# Check if npx and tsx are available
if ! command -v npx &> /dev/null; then
  error "npx not found. Please make sure Node.js is installed properly."
  exit 1
fi

# Check that unified_reconciliation.ts exists
if [ ! -f "unified_reconciliation.ts" ]; then
  error "unified_reconciliation.ts not found. Please run this script from the project root directory."
  exit 1
fi

# Print header
echo -e "${BLUE}=========================================${NC}"
echo -e "${BLUE}       Unified Reconciliation Tool      ${NC}"
echo -e "${BLUE}=========================================${NC}"
echo

# Command handling
case "$1" in
  "status")
    log "Checking reconciliation status..."
    npx tsx unified_reconciliation.ts status
    ;;
    
  "analyze")
    log "Analyzing reconciliation data..."
    npx tsx unified_reconciliation.ts analyze
    ;;
    
  "reconcile")
    BATCH_SIZE="${2:-10}"
    log "Starting reconciliation with batch size $BATCH_SIZE..."
    npx tsx unified_reconciliation.ts reconcile "$BATCH_SIZE"
    ;;
    
  "date")
    if [ -z "$2" ]; then
      error "Date is required (format: YYYY-MM-DD)"
      echo "Usage: $0 date YYYY-MM-DD"
      exit 1
    fi
    
    log "Processing date $2..."
    npx tsx unified_reconciliation.ts date "$2"
    ;;
    
  "range")
    if [ -z "$2" ] || [ -z "$3" ]; then
      error "Start and end dates are required (format: YYYY-MM-DD)"
      echo "Usage: $0 range YYYY-MM-DD YYYY-MM-DD [batchSize]"
      exit 1
    fi
    
    BATCH_SIZE="${4:-10}"
    log "Processing date range from $2 to $3 with batch size $BATCH_SIZE..."
    npx tsx unified_reconciliation.ts range "$2" "$3" "$BATCH_SIZE"
    ;;
    
  "critical")
    if [ -z "$2" ]; then
      error "Date is required (format: YYYY-MM-DD)"
      echo "Usage: $0 critical YYYY-MM-DD"
      exit 1
    fi
    
    log "Processing critical date $2..."
    npx tsx unified_reconciliation.ts critical "$2"
    ;;
    
  "spot-fix")
    if [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ]; then
      error "Date, period, and farm ID are required"
      echo "Usage: $0 spot-fix YYYY-MM-DD PERIOD FARM_ID"
      exit 1
    fi
    
    log "Spot fixing $2 period $3 farm $4..."
    npx tsx unified_reconciliation.ts spot-fix "$2" "$3" "$4"
    ;;
    
  "help"|*)
    echo "Usage: $0 [command] [options]"
    echo
    echo "Commands:"
    echo "  status                - Show current reconciliation status"
    echo "  analyze               - Analyze missing calculations and detect issues"
    echo "  reconcile [batchSize] - Process all missing calculations (default batch size: 10)"
    echo "  date YYYY-MM-DD       - Process a specific date"
    echo "  range YYYY-MM-DD YYYY-MM-DD [batchSize] - Process a date range"
    echo "  critical DATE         - Process a problematic date with extra safeguards"
    echo "  spot-fix DATE PERIOD FARM - Fix a specific date-period-farm combination"
    echo "  help                  - Show this help message"
    ;;
esac

echo
success "Operation completed"