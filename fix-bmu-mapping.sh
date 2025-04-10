#!/bin/bash
# Script to fix BMU mapping inconsistency and sync files

echo "==== BMU Mapping Fix Script ===="
echo "This script will synchronize the BMU mapping files to ensure consistent farm data"
echo ""

# Run the TypeScript fix script
npx tsx scripts/fixBmuMapping.ts

# Check if the script was successful
if [ $? -eq 0 ]; then
  echo ""
  echo "✅ BMU mapping fixed successfully!"
  echo ""
  echo "You can now run ./reprocess-april3.sh to reprocess April 3 data with the fixed mappings"
  echo "Or run ./reprocess-any-date.sh YYYY-MM-DD to reprocess any other date"
else
  echo ""
  echo "❌ BMU mapping fix failed. Please check the logs above for errors."
  exit 1
fi