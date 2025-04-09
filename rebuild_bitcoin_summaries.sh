#!/bin/bash

# Set script to exit on error
set -e

echo "==============================================="
echo "Bitcoin Summary Tables Rebuild Script"
echo "==============================================="

# Make script executable first time
chmod +x rebuild_bitcoin_summaries.sh

# Run the rebuild script with tsx
echo "Starting rebuild process..."
npx tsx server/scripts/rebuild_bitcoin_summaries.ts

echo "Rebuild process completed."
echo "==============================================="