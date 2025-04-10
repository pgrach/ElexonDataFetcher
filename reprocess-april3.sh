#!/bin/bash
# Shell script to reprocess data for April 3, 2025

echo "Starting reprocessing of data for 2025-04-03..."
echo "This will delete existing data and fetch fresh data from Elexon API"
echo "Please wait, this process may take a few minutes..."

# Execute the TypeScript script
npx tsx reprocessApril3.ts

echo "Script execution complete."