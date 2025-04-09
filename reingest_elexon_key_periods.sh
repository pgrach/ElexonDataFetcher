#!/bin/bash

# Define date to process
DATE=${1:-"2025-04-01"}

echo "==================================================="
echo "   ELEXON KEY PERIODS REINGESTION for $DATE"
echo "==================================================="

echo "Starting reingestion process..."
npx tsx reingest_elexon_key_periods.ts $DATE