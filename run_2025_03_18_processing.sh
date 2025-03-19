#!/bin/bash

# Process 2025-03-18 in manageable chunks to avoid timeouts
# We'll process periods that are still missing (1-29, 32-37)

# Display the current status
echo "=== Checking current status for 2025-03-18 ==="
npx tsx -e "import { db } from './db'; import { eq, sql } from 'drizzle-orm'; import { curtailmentRecords } from './db/schema'; 
async function check() {
  const result = await db.select({
    period_count: sql\`COUNT(DISTINCT settlement_period)\`,
    record_count: sql\`COUNT(*)\`,
    total_volume: sql\`ROUND(SUM(ABS(volume::numeric))::numeric, 2)\`,
    total_payment: sql\`ROUND(SUM(payment::numeric)::numeric, 2)\`,
    periods: sql\`ARRAY_AGG(DISTINCT settlement_period ORDER BY settlement_period)\`
  }).from(curtailmentRecords).where(eq(curtailmentRecords.settlementDate, '2025-03-18'));
  
  if (result.length > 0) {
    console.log(\`Current state: \${result[0].record_count} records across \${result[0].period_count} periods\`);
    console.log(\`Volume: \${result[0].total_volume} MWh, Payment: £\${result[0].total_payment}\`);
    console.log(\`Existing periods: \${result[0].periods.join(', ')}\`);
    
    // Calculate missing periods
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    const existingPeriods = new Set(result[0].periods);
    const missingPeriods = allPeriods.filter(p => !existingPeriods.has(p));
    
    console.log(\`Missing periods: \${missingPeriods.join(', ')}\`);
    
    // Create batches of missing periods
    const batchSize = 5;
    const batches = [];
    for (let i = 0; i < missingPeriods.length; i += batchSize) {
      const batch = missingPeriods.slice(i, i + batchSize);
      batches.push(batch);
    }
    
    console.log(\`Created \${batches.length} batches to process\`);
    
    // Print batch details
    batches.forEach((batch, index) => {
      console.log(\`Batch \${index + 1}: Periods \${batch[0]}-\${batch[batch.length - 1]}\`);
    });
  } else {
    console.log('No data found for 2025-03-18');
  }
}
check().then(() => process.exit(0)).catch(e => { console.error(e); process.exit(1); });"

echo ""
echo "=== Processing missing periods in batches ==="

# Process batch 1: periods 1-5
echo ""
echo "Processing Batch 1: Periods 1-5..."
npx tsx process_2025_03_18_batch.ts 1 5

# Process batch 2: periods 6-10
echo ""
echo "Processing Batch 2: Periods 6-10..."
npx tsx process_2025_03_18_batch.ts 6 10

# Process batch 3: periods 11-15
echo ""
echo "Processing Batch 3: Periods 11-15..."
npx tsx process_2025_03_18_batch.ts 11 15

# Process batch 4: periods 16-20
echo ""
echo "Processing Batch 4: Periods 16-20..."
npx tsx process_2025_03_18_batch.ts 16 20

# Process batch 5: periods 21-25
echo ""
echo "Processing Batch 5: Periods 21-25..."
npx tsx process_2025_03_18_batch.ts 21 25

# Process batch 6: periods 26-29, 32-33
echo ""
echo "Processing Batch 6: Periods 26-29..."
npx tsx process_2025_03_18_batch.ts 26 29

# Process batch 7: periods 32-37
echo ""
echo "Processing Batch 7: Periods 32-37..."
npx tsx process_2025_03_18_batch.ts 32 37

# Final check of the data
echo ""
echo "=== Final data check for 2025-03-18 ==="
npx tsx -e "import { db } from './db'; import { eq, sql } from 'drizzle-orm'; import { curtailmentRecords } from './db/schema'; 
async function check() {
  const result = await db.select({
    period_count: sql\`COUNT(DISTINCT settlement_period)\`,
    record_count: sql\`COUNT(*)\`,
    total_volume: sql\`ROUND(SUM(ABS(volume::numeric))::numeric, 2)\`,
    total_payment: sql\`ROUND(SUM(payment::numeric)::numeric, 2)\`,
  }).from(curtailmentRecords).where(eq(curtailmentRecords.settlementDate, '2025-03-18'));
  
  if (result.length > 0) {
    console.log(\`Final state: \${result[0].record_count} records across \${result[0].period_count} periods\`);
    console.log(\`Volume: \${result[0].total_volume} MWh, Payment: £\${result[0].total_payment}\`);
    
    // Check if all 48 periods are present
    const periodsResult = await db.select({
      periods: sql\`ARRAY_AGG(DISTINCT settlement_period ORDER BY settlement_period)\`
    }).from(curtailmentRecords).where(eq(curtailmentRecords.settlementDate, '2025-03-18'));
    
    if (periodsResult[0].periods.length === 48) {
      console.log('SUCCESS: All 48 periods are present!');
    } else {
      const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
      const existingPeriods = new Set(periodsResult[0].periods);
      const missingPeriods = allPeriods.filter(p => !existingPeriods.has(p));
      console.log(\`WARN: Still missing periods: \${missingPeriods.join(', ')}\`);
    }
  } else {
    console.log('No data found for 2025-03-18');
  }
}
check().then(() => process.exit(0)).catch(e => { console.error(e); process.exit(1); });"

echo ""
echo "=== Running reconciliation to update Bitcoin calculations ==="
npx tsx unified_reconciliation.ts date 2025-03-18

echo ""
echo "=== Process complete ==="