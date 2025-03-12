#!/usr/bin/env tsx
/**
 * Data Verification Tool for 2025-03-11
 * 
 * This script checks the current state of data in the database for 2025-03-11
 * and identifies any missing periods that need to be reprocessed.
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { eq, sql, count } from "drizzle-orm";

const DATE = "2025-03-11";
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

async function main() {
  console.log(`Checking data for ${DATE}...\n`);

  // Check curtailment records
  const curtailmentStats = await db
    .select({
      recordCount: count(curtailmentRecords.id),
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE));

  console.log("=== Curtailment Records ===");
  console.log(`Records: ${curtailmentStats[0]?.recordCount || 0}`);
  console.log(`Periods: ${curtailmentStats[0]?.periodCount || 0} / 48`);
  console.log(`Farms: ${curtailmentStats[0]?.farmCount || 0}`);
  console.log(`Volume: ${Number(curtailmentStats[0]?.totalVolume || 0).toFixed(2)} MWh`);
  console.log(`Payment: £${Number(curtailmentStats[0]?.totalPayment || 0).toFixed(2)}`);

  // Get period-level data
  const periodStats = await db
    .select({
      period: curtailmentRecords.settlementPeriod,
      recordCount: count(curtailmentRecords.id),
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE))
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);

  console.log("\n=== Period Details ===");
  console.log("Period | Records | Volume (MWh) | Payment (£)");
  console.log("-------|---------|-------------|------------");
  
  periodStats.forEach(stat => {
    console.log(
      `${stat.period.toString().padStart(6)} | ` +
      `${stat.recordCount.toString().padStart(7)} | ` +
      `${Number(stat.totalVolume).toFixed(2).padStart(11)} | ` +
      `${Number(stat.totalPayment).toFixed(2).padStart(10)}`
    );
  });

  // Check for missing periods
  const existingPeriods = periodStats.map(p => p.period);
  const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
  const missingPeriods = allPeriods.filter(p => !existingPeriods.includes(p));

  if (missingPeriods.length > 0) {
    console.log(`\n⚠️ Missing ${missingPeriods.length} periods: ${missingPeriods.join(', ')}`);
  } else {
    console.log("\n✅ All 48 periods have data");
  }

  // Check Bitcoin calculations
  console.log("\n=== Bitcoin Calculations ===");
  
  for (const minerModel of MINER_MODELS) {
    const bitcoinStats = await db
      .select({
        recordCount: count(),
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`
      })
      .from(historicalBitcoinCalculations)
      .where(
        eq(historicalBitcoinCalculations.settlementDate, DATE) && 
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      );
    
    console.log(`${minerModel}: ${bitcoinStats[0]?.recordCount || 0} records, ` +
      `${bitcoinStats[0]?.periodCount || 0} periods, ` +
      `${Number(bitcoinStats[0]?.totalBitcoin || 0).toFixed(8)} BTC`);
  }

  // Suggest next steps
  console.log("\n=== Recommendation ===");
  
  if (missingPeriods.length > 0) {
    console.log(`Re-ingest data for ${DATE} focusing on missing periods: ${missingPeriods.join(', ')}`);
    console.log("Run: npx tsx reingest-data.ts " + DATE);
  } else if ((curtailmentStats[0]?.periodCount || 0) < 48) {
    console.log(`Re-ingest data for ${DATE} to ensure all periods are properly processed`);
    console.log("Run: npx tsx reingest-data.ts " + DATE);
  } else {
    console.log(`Data for ${DATE} appears complete with all 48 periods. No action needed.`);
  }
}

main().catch(error => {
  console.error("Error:", error);
  process.exit(1);
});