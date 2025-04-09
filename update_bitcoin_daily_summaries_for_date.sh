#!/bin/bash

# Define date to process
DATE=${1:-"2025-04-01"}

echo "==================================================="
echo "     BITCOIN CALCULATIONS UPDATE for $DATE"
echo "==================================================="

echo "Starting update process..."
npx tsx update_bitcoin_daily_summaries_for_date.ts $DATE