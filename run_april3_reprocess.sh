#!/bin/bash

# Script to reprocess data for April 3, 2025
echo "Starting direct reprocessing of data for April 3, 2025..."
echo "Using TSX to run the TypeScript script directly..."

# Run the TypeScript script directly using tsx
tsx scripts/reprocess_april3_direct.ts

# Check the exit code
if [ $? -eq 0 ]; then
  echo "Reprocessing completed successfully!"
else
  echo "Reprocessing failed. Check the logs for details."
fi