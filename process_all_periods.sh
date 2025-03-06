#!/bin/bash

# Process 2025-03-04 data in batches of 6 periods
# This avoids timeouts by running smaller batches

echo "Starting batch processing for 2025-03-04"
echo "-----------------------------------------"

# Process in batches of 6 periods (8 batches total)
npx tsx batch_process_periods.ts 1 6
echo "Batch 1 complete. Continuing..."

npx tsx batch_process_periods.ts 7 12
echo "Batch 2 complete. Continuing..."

npx tsx batch_process_periods.ts 13 18
echo "Batch 3 complete. Continuing..."

npx tsx batch_process_periods.ts 19 24
echo "Batch 4 complete. Continuing..."

npx tsx batch_process_periods.ts 25 30
echo "Batch 5 complete. Continuing..."

npx tsx batch_process_periods.ts 31 36
echo "Batch 6 complete. Continuing..."

npx tsx batch_process_periods.ts 37 42
echo "Batch 7 complete. Continuing..."

npx tsx batch_process_periods.ts 43 48
echo "Batch 8 complete."

echo "-----------------------------------------"
echo "All batches completed. Verifying results:"

# Check final results
npx tsx -e "
const { db } = require('./db');
const { curtailmentRecords, historicalBitcoinCalculations } = require('./db/schema');
const { count, eq, sql } = require('drizzle-orm');

async function checkResults() {
  // Check curtailment records
  const curtailmentStats = await db
    .select({
      recordCount: count(),
      uniquePeriods: sql\`COUNT(DISTINCT settlement_period)\`,
      totalVolume: sql\`SUM(ABS(volume::numeric))\`,
      totalPayment: sql\`SUM(payment::numeric)\`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, '2025-03-04'));
    
  console.log('Curtailment records summary:');
  console.log(\`- Total records: \${curtailmentStats[0].recordCount}\`);
  console.log(\`- Unique periods: \${curtailmentStats[0].uniquePeriods} of 48\`);
  console.log(\`- Total volume: \${Number(curtailmentStats[0].totalVolume).toFixed(2)} MWh\`);
  console.log(\`- Total payment: £\${Number(curtailmentStats[0].totalPayment).toFixed(2)}\`);
  
  // Check Bitcoin calculations
  const bitcoinStats = await db
    .select({
      minerModel: historicalBitcoinCalculations.minerModel,
      recordCount: count(),
      totalBitcoin: sql\`SUM(bitcoin_mined::numeric)\`
    })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, '2025-03-04'))
    .groupBy(historicalBitcoinCalculations.minerModel);
    
  console.log('\\nBitcoin calculations summary:');
  bitcoinStats.forEach(stat => {
    console.log(\`- \${stat.minerModel}: \${stat.recordCount} records, \${Number(stat.totalBitcoin).toFixed(8)} BTC\`);
  });
  
  process.exit(0);
}

checkResults().catch(err => {
  console.error('Error checking results:', err);
  process.exit(1);
});
"

echo "-----------------------------------------"
echo "Processing completed for 2025-03-04"