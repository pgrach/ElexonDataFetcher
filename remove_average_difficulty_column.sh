#!/bin/bash

# Set script to exit on error
set -e

echo "==============================================="
echo "Remove Average Difficulty Column Script"
echo "==============================================="

# Make the script executable
chmod +x remove_average_difficulty_column.sh

# Run the column removal script with tsx
echo "Starting column removal process..."
npx tsx server/scripts/remove_average_difficulty_column.ts

echo "Column removal process completed."
echo "==============================================="