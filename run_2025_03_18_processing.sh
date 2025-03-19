#!/bin/bash

# Script to process all periods for 2025-03-18 in batches
# This script helps avoid API timeouts by processing smaller batches

echo "=== Starting 2025-03-18 Data Processing ==="
echo "Processing date: 2025-03-18"
echo ""

# Collect current state
echo "Current data state:"
npx tsx -e "import { db } from './db'; import { eq, sql } from 'drizzle-orm'; import { curtailmentRecords } from './db/schema'; async function check() { const stats = await db.select({ recordCount: sql\`COUNT(*)\`, periodCount: sql\`COUNT(DISTINCT settlement_period)\`, totalVolume: sql\`ROUND(SUM(ABS(volume::numeric))::numeric, 2)\`, totalPayment: sql\`ROUND(SUM(payment::numeric)::numeric, 2)\` }).from(curtailmentRecords).where(eq(curtailmentRecords.settlementDate, '2025-03-18')); console.log(stats[0] ? \`Records: \${stats[0].recordCount}, Periods: \${stats[0].periodCount}, Volume: \${stats[0].totalVolume} MWh, Payment: £\${stats[0].totalPayment}\` : 'No data found'); } check();"
echo ""

# Process in batches - these batches cover all 48 periods
# Each batch processes 5 periods
echo "Processing batches..."

echo "Batch 1: Periods 1-5"
npx tsx process_2025_03_18_batch.ts 1 5
echo ""

echo "Batch 2: Periods 6-10"
npx tsx process_2025_03_18_batch.ts 6 10
echo ""

echo "Batch 3: Periods 11-15"
npx tsx process_2025_03_18_batch.ts 11 15
echo ""

echo "Batch 4: Periods 16-20"
npx tsx process_2025_03_18_batch.ts 16 20
echo ""

echo "Batch 5: Periods 21-25"
npx tsx process_2025_03_18_batch.ts 21 25
echo ""

echo "Batch 6: Periods 26-29"
npx tsx process_2025_03_18_batch.ts 26 29
echo ""

# Periods 30-37 are partially processed - let's re-process them to be thorough
echo "Batch 7: Periods 30-34"
npx tsx process_2025_03_18_batch.ts 30 34
echo ""

echo "Batch 8: Periods 35-39"
npx tsx process_2025_03_18_batch.ts 35 39
echo ""

echo "Batch 9: Periods 40-44"
npx tsx process_2025_03_18_batch.ts 40 44
echo ""

echo "Batch 10: Periods 45-48"
npx tsx process_2025_03_18_batch.ts 45 48
echo ""

# Update Bitcoin calculations
echo "Updating Bitcoin calculations for all miner models..."
for model in "S19J_PRO" "S9" "M20S"; do
  echo "Processing $model..."
  npx tsx -e "import { processSingleDay } from './server/services/bitcoinService'; async function run() { await processSingleDay('2025-03-18', '$model'); console.log('Completed $model calculations'); } run();"
done

# Final verification
echo ""
echo "Final data state:"
npx tsx -e "import { db } from './db'; import { eq, sql } from 'drizzle-orm'; import { curtailmentRecords } from './db/schema'; async function check() { const stats = await db.select({ recordCount: sql\`COUNT(*)\`, periodCount: sql\`COUNT(DISTINCT settlement_period)\`, totalVolume: sql\`ROUND(SUM(ABS(volume::numeric))::numeric, 2)\`, totalPayment: sql\`ROUND(SUM(payment::numeric)::numeric, 2)\` }).from(curtailmentRecords).where(eq(curtailmentRecords.settlementDate, '2025-03-18')); console.log(stats[0] ? \`Records: \${stats[0].recordCount}, Periods: \${stats[0].periodCount}, Volume: \${stats[0].totalVolume} MWh, Payment: £\${stats[0].totalPayment}\` : 'No data found'); } check();"

echo "Bitcoin calculation verification:"
npx tsx -e "import { db } from './db'; import { sql } from 'drizzle-orm'; async function check() { const stats = await db.execute(sql\`SELECT miner_model, COUNT(*) as record_count, COUNT(DISTINCT settlement_period) as period_count, ROUND(SUM(bitcoin_mined)::numeric, 8) as total_bitcoin FROM historical_bitcoin_calculations WHERE settlement_date = '2025-03-18' GROUP BY miner_model ORDER BY miner_model\`); stats.forEach(row => console.log(\`\${row.miner_model}: \${row.record_count} records, \${row.period_count} periods, \${row.total_bitcoin} BTC\`)); } check();"

echo ""
echo "=== Processing Complete ==="