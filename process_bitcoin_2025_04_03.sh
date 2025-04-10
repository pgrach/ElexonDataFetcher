#!/bin/bash

# Script to process Bitcoin calculations for 2025-04-03

echo "==== Starting Bitcoin processing for 2025-04-03 ===="
echo "This script will:"
echo "1. Clear existing Bitcoin calculations for April 3, 2025"
echo "2. Recalculate Bitcoin mining for all three miner models (S19J_PRO, S9, M20S)"
echo "3. Update all Bitcoin summary tables"
echo "4. Verify the results match expected patterns"
echo ""

echo "Starting processing..."
echo ""

# Log file setup
LOG_DIR="./logs"
LOG_FILE="${LOG_DIR}/process_bitcoin_2025_04_03_$(date +%Y-%m-%dT%H-%M-%S).log"

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Run the TypeScript script using tsx with logging
npx tsx scripts/process_bitcoin_2025_04_03.ts | tee "$LOG_FILE"

# Check if the script executed successfully
if [ $? -eq 0 ]; then
    echo ""
    echo "==== Bitcoin processing completed successfully ===="
    echo "All Bitcoin calculations for 2025-04-03 have been updated."
    echo "Log file: $LOG_FILE"
else
    echo ""
    echo "==== Bitcoin processing FAILED ===="
    echo "Please check the error messages in log file: $LOG_FILE"
fi