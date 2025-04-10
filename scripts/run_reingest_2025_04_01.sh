#!/bin/bash

# Set the maximum number of settlement periods to process
MAX_PERIODS=${1:-48}

# Start the reprocessing script
echo "Starting reingestion process for 2025-04-01 with MAX_PERIODS=$MAX_PERIODS..."
MAX_PERIODS=$MAX_PERIODS npx ts-node scripts/reingest_2025_04_01.ts