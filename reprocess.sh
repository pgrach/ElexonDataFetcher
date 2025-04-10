#!/bin/bash

echo "Starting reprocessing script for 2025-04-03..."
npx tsx reprocess-april3.ts

# Check the exit code
if [ $? -eq 0 ]; then
  echo "Reprocessing completed successfully!"
else
  echo "Reprocessing failed with an error. Check the logs above."
fi