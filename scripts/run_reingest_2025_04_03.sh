#!/bin/bash

# Set maximum number of settlement periods to process
# Default is 10 for quick testing; set to 48 for full processing
MAX_PERIODS=${1:-5}

# Run script to reingest data for 2025-04-03
echo "Starting reingestion process for 2025-04-03 with MAX_PERIODS=$MAX_PERIODS..."
MAX_PERIODS=$MAX_PERIODS npx tsx scripts/reingest_2025_04_03.ts

echo "Reingestion process complete."