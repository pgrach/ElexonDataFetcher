#!/bin/bash

echo "Starting reprocessing for 2025-04-03..."
cd "$(dirname "$0")/.." # Navigate to project root
npx tsx scripts/reprocess-april3.ts

# Check the exit code
if [ $? -eq 0 ]; then
  echo "Reprocessing completed successfully!"
else
  echo "Reprocessing failed with an error. Check the logs above."
fi