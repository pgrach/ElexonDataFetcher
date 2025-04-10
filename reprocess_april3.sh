#!/bin/bash

# Script to reprocess data for April 3, 2025
echo "Starting reprocessing of data for April 3, 2025..."
tsx scripts/reprocess_april3_2025.ts

# Check the exit code
if [ $? -eq 0 ]; then
  echo "Reprocessing completed successfully!"
else
  echo "Reprocessing failed. Check the logs for details."
fi