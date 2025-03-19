#!/bin/bash

# Run 2025-03-18 Processing
# This script processes the missing periods for 2025-03-18 in small batches
# to avoid API timeouts and rate limiting issues.

# Set error handling
set -e

# Define the date we're processing
TARGET_DATE="2025-03-18"
echo "Starting batch processing for $TARGET_DATE"

# First batch: Periods 1-5
echo -e "\n================================================"
echo "Processing periods 1-5..."
npx tsx process_2025_03_18_batch.ts 1 5
echo "Batch 1-5 complete"
sleep 5  # Wait between batches

# Second batch: Periods 6-10
echo -e "\n================================================"
echo "Processing periods 6-10..."
npx tsx process_2025_03_18_batch.ts 6 10
echo "Batch 6-10 complete"
sleep 5

# Third batch: Periods 11-15
echo -e "\n================================================"
echo "Processing periods 11-15..."
npx tsx process_2025_03_18_batch.ts 11 15
echo "Batch 11-15 complete"
sleep 5

# Fourth batch: Periods 16-20
echo -e "\n================================================"
echo "Processing periods 16-20..."
npx tsx process_2025_03_18_batch.ts 16 20
echo "Batch 16-20 complete"
sleep 5

# Fifth batch: Periods 21-25
echo -e "\n================================================"
echo "Processing periods 21-25..."
npx tsx process_2025_03_18_batch.ts 21 25
echo "Batch 21-25 complete"
sleep 5

# Sixth batch: Periods 26-29
echo -e "\n================================================"
echo "Processing periods 26-29..."
npx tsx process_2025_03_18_batch.ts 26 29
echo "Batch 26-29 complete"
sleep 5

# Seventh batch: Periods 32-37
echo -e "\n================================================"
echo "Processing periods 32-37..."
npx tsx process_2025_03_18_batch.ts 32 37
echo "Batch 32-37 complete"
sleep 5

# Update Bitcoin calculations
echo -e "\n================================================"
echo "Updating Bitcoin calculations..."
npx tsx unified_reconciliation.ts date 2025-03-18

# Verify the completion
echo -e "\n================================================"
echo "Running comprehensive verification..."
npx tsx verify_2025_03_18_data.ts

echo -e "\n================================================"
echo "Processing complete for $TARGET_DATE"
echo "Check the verification results above for any remaining gaps"