#!/bin/bash
# Shell script to reprocess data for any date

# Check if date argument is provided
if [ $# -eq 0 ]; then
  echo "Error: No date specified"
  echo "Usage: ./reprocess-any-date.sh YYYY-MM-DD"
  exit 1
fi

DATE=$1

# Validate date format
if ! [[ $DATE =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "Error: Invalid date format"
  echo "Usage: ./reprocess-any-date.sh YYYY-MM-DD"
  exit 1
fi

echo "Starting reprocessing of data for $DATE..."
echo "This will delete existing data and fetch fresh data from Elexon API"
echo "Please wait, this process may take several minutes..."

# Execute the TypeScript script with the date parameter
npx tsx scripts/reprocessAnyDate.ts $DATE

echo "Script execution complete."