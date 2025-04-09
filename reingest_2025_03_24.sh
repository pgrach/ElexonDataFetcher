#!/bin/bash

# Script to reingest data for 2025-03-24
# This script provides a simple way to run the reingestion process

echo "Select which reingestion process to run:"
echo "1) Basic reingestion (curtailment_records table only)"
echo "2) Complete reingestion with summary table updates"
echo "3) Simple JS version (most reliable)"
echo "4) Update summary tables only (after running option 1 or 3)"

read -p "Enter option (1-4): " option

case $option in
  1)
    echo "Running basic reingestion..."
    npx tsx server/scripts/run_reingest_2025_03_24.ts
    ;;
  2)
    echo "Running complete reingestion with summary updates..."
    npx tsx server/scripts/update_2025_03_24_complete.ts
    ;;
  3)
    echo "Running simple JS version..."
    node server/scripts/reingest_2025_03_24_simple.js
    ;;
  4)
    echo "Updating summary tables only..."
    node server/scripts/update_summaries_2025_03_24.js
    ;;
  *)
    echo "Invalid option. Exiting."
    exit 1
    ;;
esac