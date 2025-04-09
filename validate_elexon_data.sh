#!/bin/bash

# Exit on error
set -e

echo "==========================================================="
echo "Validate Elexon API Data for 2025-04-01"
echo "==========================================================="

# Make the script executable
chmod +x validate_elexon_data.sh

# Execute the TypeScript script using tsx
echo "Starting validation process..."
npx tsx validate_elexon_data.ts

echo "Validation process completed."
echo "==========================================================="