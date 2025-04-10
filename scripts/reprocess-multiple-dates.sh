#!/bin/bash

# Check if dates are provided
if [ $# -eq 0 ]; then
  echo "Error: No dates provided. Please provide at least one date in YYYY-MM-DD format."
  echo "Usage: $0 <date1> [date2] [date3] ..."
  echo "Example: $0 2025-04-03 2025-04-04"
  exit 1
fi

echo "Starting reprocessing for dates: $@"
cd "$(dirname "$0")/.." # Navigate to project root
npx tsx scripts/reprocess-multiple-dates.ts "$@"

# Check the exit code
if [ $? -eq 0 ]; then
  echo "Reprocessing completed successfully!"
else
  echo "Reprocessing encountered errors. Check the logs above."
fi