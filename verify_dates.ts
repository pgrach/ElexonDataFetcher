/**
 * Data Verification Script
 * 
 * This script provides a comprehensive verification of the data integrity
 * for specific dates across all relevant tables.
 */

import { db } from './db';
import { eq, between, sql } from 'drizzle-orm';
import { curtailmentRecords, historicalBitcoinCalculations, bitcoinMonthlySummaries, bitcoinYearlySummaries } from './db/schema';

// Configuration
const DATES_TO_VERIFY = ['2025-03-11', '2025-03-12'];
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

async function verifyCurtailmentRecords(date: string) {
  console.log(`\n=== Curtailment Records for ${date} ===`);
  
  // Get overall statistics
  const stats = await db
    .select({
      recordCount: sql<number>`COUNT(*)`,
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  // Check for missing periods
  const existingPeriods = await db
    .select({ period: curtailmentRecords.settlementPeriod })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date))
    .groupBy(curtailmentRecords.settlementPeriod);
  
  const existingPeriodNumbers = existingPeriods.map(p => p.period);
  const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
  const missingPeriods = allPeriods.filter(p => !existingPeriodNumbers.includes(p));
  
  // Print summary
  console.log(`Records: ${stats[0]?.recordCount || 0}`);
  console.log(`Periods: ${stats[0]?.periodCount || 0} of 48`);
  console.log(`Farms: ${stats[0]?.farmCount || 0}`);
  console.log(`Total Volume: ${Number(stats[0]?.totalVolume || 0).toFixed(2)} MWh`);
  console.log(`Total Payment: £${Number(stats[0]?.totalPayment || 0).toFixed(2)}`);
  
  if (missingPeriods.length > 0) {
    console.log(`Missing Periods: ${missingPeriods.join(', ')}`);
  } else {
    console.log(`All 48 periods are present.`);
  }
  
  return stats[0];
}

async function verifyBitcoinCalculations(date: string) {
  console.log(`\n=== Bitcoin Calculations for ${date} ===`);
  
  for (const minerModel of MINER_MODELS) {
    const stats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, date))
      .where(eq(historicalBitcoinCalculations.minerModel, minerModel));
    
    console.log(`\n${minerModel}:`);
    console.log(`Records: ${stats[0]?.recordCount || 0}`);
    console.log(`Periods: ${stats[0]?.periodCount || 0} of 48`);
    console.log(`Farms: ${stats[0]?.farmCount || 0}`);
    console.log(`Total Bitcoin: ${Number(stats[0]?.totalBitcoin || 0).toFixed(8)} BTC`);
  }
}

async function verifySummaries() {
  // Get month and year from the first date
  const month = DATES_TO_VERIFY[0].substring(0, 7); // YYYY-MM
  const year = DATES_TO_VERIFY[0].substring(0, 4);  // YYYY
  
  console.log(`\n=== Monthly Summary for ${month} ===`);
  
  const monthlySummaries = await db
    .select({
      minerModel: bitcoinMonthlySummaries.minerModel,
      bitcoinMined: bitcoinMonthlySummaries.bitcoinMined
    })
    .from(bitcoinMonthlySummaries)
    .where(eq(bitcoinMonthlySummaries.yearMonth, month));
  
  for (const summary of monthlySummaries) {
    console.log(`${summary.minerModel}: ${Number(summary.bitcoinMined).toFixed(8)} BTC`);
  }
  
  console.log(`\n=== Yearly Summary for ${year} ===`);
  
  const yearlySummaries = await db
    .select({
      minerModel: bitcoinYearlySummaries.minerModel,
      bitcoinMined: bitcoinYearlySummaries.bitcoinMined
    })
    .from(bitcoinYearlySummaries)
    .where(eq(bitcoinYearlySummaries.year, year));
  
  for (const summary of yearlySummaries) {
    console.log(`${summary.minerModel}: ${Number(summary.bitcoinMined).toFixed(8)} BTC`);
  }
}

async function verifyData() {
  console.log("=== Starting Data Verification ===");
  
  const curtailmentStats = {};
  
  for (const date of DATES_TO_VERIFY) {
    curtailmentStats[date] = await verifyCurtailmentRecords(date);
    await verifyBitcoinCalculations(date);
  }
  
  await verifySummaries();
  
  console.log("\n=== Verification Complete ===");
  console.log("All data has been verified across curtailment records, bitcoin calculations, and summaries.");
  console.log(`Total energy verified: ${Number(curtailmentStats[DATES_TO_VERIFY[0]]?.totalVolume || 0) + Number(curtailmentStats[DATES_TO_VERIFY[1]]?.totalVolume || 0)} MWh`);
}

// Run the verification
verifyData().then(() => {
  console.log("\nVerification completed successfully");
  process.exit(0);
}).catch((error) => {
  console.error("Verification failed:", error);
  process.exit(1);
});