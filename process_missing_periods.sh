#!/bin/bash

# Process the remaining missing periods for 2025-03-04
echo "Processing missing periods for 2025-03-04"
echo "-----------------------------------------"

# Process period 38
npx tsx batch_process_periods.ts 38 38
echo "Period 38 complete. Continuing..."

# Process periods 41-42
npx tsx batch_process_periods.ts 41 42
echo "Periods 41-42 complete. Continuing..."

# Process periods 43-45
npx tsx batch_process_periods.ts 43 45
echo "Periods 43-45 complete."

echo "-----------------------------------------"
echo "Verifying results:"

# Check final results
npx tsx -e "
const { db } = require('./db');
const { curtailmentRecords } = require('./db/schema');
const { count, eq, sql } = require('drizzle-orm');

async function checkResults() {
  // Get count of periods
  const periodCount = await db
    .select({ count: sql\`COUNT(DISTINCT settlement_period)\` })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, '2025-03-04'));
    
  console.log(\`Periods processed: \${periodCount[0].count} of 48\`);
  
  // Find any missing periods
  const missingPeriods = await db.execute(sql\`
    WITH all_periods AS (
        SELECT generate_series(1, 48) AS period
    )
    SELECT 
        a.period
    FROM 
        all_periods a
    LEFT JOIN (
        SELECT DISTINCT settlement_period 
        FROM curtailment_records 
        WHERE settlement_date = '2025-03-04'
    ) c ON a.period = c.settlement_period
    WHERE 
        c.settlement_period IS NULL
    ORDER BY 
        a.period
  \`);
  
  if (missingPeriods.length > 0) {
    console.log('Missing periods:');
    missingPeriods.forEach(row => console.log(\`- Period \${row.period}\`));
  } else {
    console.log('All 48 periods successfully processed!');
  }
  
  // Check total curtailment volume
  const totalVolume = await db
    .select({ volume: sql\`SUM(ABS(volume::numeric))\` })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, '2025-03-04'));
    
  console.log(\`Total curtailed volume: \${Number(totalVolume[0].volume).toFixed(2)} MWh\`);
  
  process.exit(0);
}

checkResults().catch(err => {
  console.error('Error checking results:', err);
  process.exit(1);
});
"

echo "-----------------------------------------"
echo "Processing completed for 2025-03-04"