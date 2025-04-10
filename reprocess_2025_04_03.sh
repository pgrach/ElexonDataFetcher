#!/bin/bash

# Script to reprocess data for 2025-04-03
# This will run the full reprocessing pipeline including:
# - Curtailment records
# - Summary tables
# - Bitcoin calculations

echo "====== Data Reprocessing for 2025-04-03 ======"
echo "Starting reprocessing at $(date)"
echo

# Run the TypeScript file using tsx
npx tsx reprocess_2025_04_03.ts

# Check if the script was successful
if [ $? -eq 0 ]; then
  echo
  echo "✅ Reprocessing completed successfully at $(date)"
  echo
  echo "You can verify the data in the database using the following endpoints:"
  echo "- /api/curtailment/daily/2025-04-03            (curtailment data)"
  echo "- /api/summary/daily/2025-04-03                (daily summary)"
  echo "- /api/curtailment/mining-potential/2025-04-03 (Bitcoin mining potential)"
  echo
else
  echo
  echo "❌ Reprocessing failed at $(date)"
  echo "Check the logs above for more details"
  echo
fi