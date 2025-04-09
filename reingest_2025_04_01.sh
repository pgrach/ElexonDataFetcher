#!/bin/bash

# Script to reingest data for 2025-04-01
# This script provides a simple way to run the reingestion process

echo "========================================================"
echo "Elexon Data Reingestion Utility for 2025-04-01"
echo "========================================================"
echo ""

# If no option is provided as an argument, use option 1 by default
if [ $# -eq 0 ]; then
  option=1
else
  option=$1
fi

echo "Running option $option:"
echo "1) Full reingestion with all updates (recommended)"
echo "2) Only reingest curtailment records"
echo "3) Only update summary tables (after curtailment reingestion)"
echo "4) Only update Bitcoin calculations (after curtailment reingestion)"

case $option in
  1)
    echo "Running full reingestion with all updates..."
    npx tsx server/scripts/update_2025_04_01_complete.ts
    ;;
  2)
    echo "Running curtailment records reingestion only..."
    npx tsx -e "import { reingestCurtailmentRecords } from './server/scripts/update_2025_04_01_complete.ts'; reingestCurtailmentRecords();"
    ;;
  3)
    echo "Updating summary tables only..."
    npx tsx -e "import { updateSummaryTables } from './server/scripts/update_2025_04_01_complete.ts'; updateSummaryTables();"
    ;;
  4)
    echo "Updating Bitcoin calculations only..."
    npx tsx -e "import { updateBitcoinCalculations } from './server/scripts/update_2025_04_01_complete.ts'; updateBitcoinCalculations();"
    ;;
  *)
    echo "Invalid option. Exiting."
    exit 1
    ;;
esac

echo ""
echo "Process completed. Check the logs above for details."