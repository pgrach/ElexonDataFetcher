#!/bin/bash

# Script to generate M20S Bitcoin calculations for 2025-04-02 using direct SQL

echo "==== Starting M20S Bitcoin Calculations Generation for 2025-04-02 (Direct SQL) ===="
echo "This script will generate historical Bitcoin calculations for the M20S miner model"
echo "for 2025-04-02 and update the daily Bitcoin summary, using direct SQL queries."
echo ""

echo "Starting calculation generation..."
echo ""

# Run the TypeScript script using tsx
npx tsx server/scripts/generate_m20s_sql_direct.ts

# Check if the script executed successfully
if [ $? -eq 0 ]; then
    echo ""
    echo "==== M20S Bitcoin calculations generated successfully ===="
    echo "You can verify the results using the following SQL query:"
    echo "  SELECT * FROM bitcoin_daily_summaries WHERE summary_date = '2025-04-02' AND miner_model = 'M20S';"
else
    echo ""
    echo "==== M20S Bitcoin calculations generation FAILED ===="
    echo "Please check the error messages above."
fi