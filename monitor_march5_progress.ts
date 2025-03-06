/**
 * Monitor Progress for March 5, 2025 Data Restoration
 * 
 * This script checks the current state of curtailment records for March 5, 2025
 * and reports on progress toward the target of 105,247.85 MWh.
 */

import { db } from './db';
import { curtailmentRecords } from './db/schema';
import { sql } from 'drizzle-orm';

const TARGET_DATE = '2025-03-05';
const TARGET_VOLUME = 105247.85; // MWh
const TARGET_PAYMENT = 3390364.09; // GBP

async function main() {
  console.log(`🔍 Monitoring progress for ${TARGET_DATE}`);
  
  // Get current database stats
  const stats = await db.select({
    recordCount: sql<number>`count(*)`,
    periodCount: sql<number>`count(distinct settlement_period)`,
    farmCount: sql<number>`count(distinct farm_id)`,
    totalVolume: sql<number>`sum(abs(volume::numeric))`,
    totalPayment: sql<number>`abs(sum(payment::numeric))`,
  })
  .from(curtailmentRecords)
  .where(sql`${curtailmentRecords.settlementDate}::text = ${TARGET_DATE}`);
  
  // Convert to numbers and calculate progress percentages
  const volume = Number(stats[0].totalVolume || 0);
  const payment = Number(stats[0].totalPayment || 0);
  const volumeProgress = (volume / TARGET_VOLUME) * 100;
  const paymentProgress = (payment / TARGET_PAYMENT) * 100;
  const periodProgress = (Number(stats[0].periodCount || 0) / 48) * 100;
  
  console.log(`\n===== Current Progress =====`);
  console.log(`Total records: ${stats[0].recordCount}`);
  console.log(`Total unique farms: ${stats[0].farmCount}`);
  console.log(`Periods processed: ${stats[0].periodCount} / 48 (${periodProgress.toFixed(2)}%)`);
  console.log(`Volume processed: ${volume.toFixed(2)} / ${TARGET_VOLUME} MWh (${volumeProgress.toFixed(2)}%)`);
  console.log(`Payment processed: £${payment.toFixed(2)} / £${TARGET_PAYMENT} (${paymentProgress.toFixed(2)}%)`);
  
  // Get period-specific stats
  const periodStats = await db.select({
    period: curtailmentRecords.settlementPeriod,
    recordCount: sql<number>`count(*)`,
    totalVolume: sql<number>`sum(abs(volume::numeric))`,
    totalPayment: sql<number>`abs(sum(payment::numeric))`,
  })
  .from(curtailmentRecords)
  .where(sql`${curtailmentRecords.settlementDate}::text = ${TARGET_DATE}`)
  .groupBy(curtailmentRecords.settlementPeriod)
  .orderBy(curtailmentRecords.settlementPeriod);
  
  console.log(`\n===== Periods Processed =====`);
  console.log(`Period | Records | Volume (MWh) | Payment (£)`);
  console.log(`-------|---------|-------------|------------`);
  for (const period of periodStats) {
    const periodVolume = Number(period.totalVolume || 0).toFixed(2);
    const periodPayment = Number(period.totalPayment || 0).toFixed(2);
    console.log(`${period.period.toString().padStart(6, ' ')} | ${period.recordCount.toString().padStart(7, ' ')} | ${periodVolume.padStart(11, ' ')} | ${periodPayment.padStart(10, ' ')}`);
  }
  
  // Find missing periods
  const processedPeriods = new Set(periodStats.map(p => p.period));
  const missingPeriods: number[] = [];
  for (let i = 1; i <= 48; i++) {
    if (!processedPeriods.has(i)) {
      missingPeriods.push(i);
    }
  }
  
  console.log(`\n===== Missing Periods =====`);
  console.log(missingPeriods.length > 0 ? missingPeriods.join(', ') : 'None! All periods processed.');
  
  // Provide recommendations
  console.log(`\n===== Recommendations =====`);
  if (volumeProgress < 90) {
    console.log(`⚠️ Only ${volumeProgress.toFixed(2)}% of target volume processed. Continue running scripts to process more periods.`);
    console.log(`   Next periods to process: ${missingPeriods.slice(0, 5).join(', ')}...`);
  } else if (volumeProgress < 100) {
    console.log(`✅ ${volumeProgress.toFixed(2)}% of target volume processed. This is sufficient to run the Bitcoin reconciliation.`);
    console.log(`   Run: npx tsx reconcile_march5_2025.ts`);
  } else {
    console.log(`🎉 100% of target volume processed! Run the reconciliation script.`);
    console.log(`   Run: npx tsx reconcile_march5_2025.ts`);
  }
}

// Run the main function
main()
  .then(() => {
    console.log("\nMonitoring complete");
    process.exit(0);
  })
  .catch((err) => {
    console.error("Error during monitoring:", err);
    process.exit(1);
  });