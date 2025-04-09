#!/bin/bash
# This script runs validation for all 48 periods by splitting the work into batches
# to avoid timeout issues with the Elexon API.

echo "===========================================================
Complete Elexon API Validation for 2025-04-01 (All 48 Periods)
===========================================================
Starting validation process..."

# Run batch 1 (periods 1-16)
echo "
RUNNING BATCH 1 (Periods 1-16)..."
npx tsx validate_elexon_data_batch1.ts

# Wait a bit between batches to avoid rate limiting
echo "
Waiting 10 seconds before starting batch 2..."
sleep 10

# Run batch 2 (periods 17-32)
echo "
RUNNING BATCH 2 (Periods 17-32)..."
npx tsx validate_elexon_data_batch2.ts

# Wait a bit between batches to avoid rate limiting
echo "
Waiting 10 seconds before starting batch 3..."
sleep 10

# Run batch 3 (periods 33-48)
echo "
RUNNING BATCH 3 (Periods 33-48)..."
npx tsx validate_elexon_data_batch3.ts

# Wait a bit to ensure all files are written
echo "
Waiting 5 seconds before combining results..."
sleep 5

# Combine all results
echo "
COMBINING RESULTS FROM ALL BATCHES..."
npx tsx validate_elexon_combine_results.ts

echo "
===========================================================
Complete validation finished. Results available in:
- batch1_results.json
- batch2_results.json
- batch3_results.json
- complete_validation_results.json
==========================================================="