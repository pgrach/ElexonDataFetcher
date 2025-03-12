#!/usr/bin/env tsx
/**
 * Verification Report for 2025-03-11 Data Reingestion
 * 
 * This script generates a comprehensive report showing the completeness
 * and consistency of data for 2025-03-11 after reingestion.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, historicalBitcoinCalculations } from "./db/schema";
import { eq, sql, count } from "drizzle-orm";

const DATE = "2025-03-11";
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// Helper function to format numbers
function formatNumber(value: number | string | null, decimals: number = 2): string {
  if (value === null) return "0";
  
  const num = typeof value === 'string' ? parseFloat(value) : value;
  return num.toLocaleString('en-US', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals
  });
}

// Helper function to format percentages
function formatPercentage(value: number): string {
  return value.toLocaleString('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
    style: 'percent'
  });
}

async function main() {
  console.log(`\n============================================`);
  console.log(`   VERIFICATION REPORT FOR ${DATE}`);
  console.log(`============================================\n`);
  
  // 1. Check if we have all 48 periods
  const periodCheck = await db
    .select({
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE));
  
  const allPeriodsPresent = (periodCheck[0]?.periodCount || 0) === 48;
  
  console.log(`1. PERIOD COMPLETENESS CHECK`);
  console.log(`---------------------------`);
  console.log(`Total Periods Found: ${periodCheck[0]?.periodCount || 0} / 48`);
  console.log(`Status: ${allPeriodsPresent ? '✅ COMPLETE' : '❌ INCOMPLETE'}\n`);
  
  // 2. Check curtailment record statistics
  const curtailmentStats = await db
    .select({
      recordCount: count(curtailmentRecords.id),
      farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE));
  
  console.log(`2. CURTAILMENT RECORD STATISTICS`);
  console.log(`--------------------------------`);
  console.log(`Total Records: ${curtailmentStats[0]?.recordCount || 0}`);
  console.log(`Unique Farms: ${curtailmentStats[0]?.farmCount || 0}`);
  console.log(`Total Volume: ${formatNumber(curtailmentStats[0]?.totalVolume || 0)} MWh`);
  console.log(`Total Payment: £${formatNumber(curtailmentStats[0]?.totalPayment || 0)}\n`);
  
  // 3. Check daily summary
  const dailySummary = await db.query.dailySummaries.findFirst({
    where: eq(dailySummaries.summaryDate, DATE)
  });
  
  // Calculate if daily summary matches curtailment records
  const volumeMatch = 
    parseFloat(dailySummary?.totalCurtailedEnergy || "0").toFixed(2) === 
    parseFloat(curtailmentStats[0]?.totalVolume || "0").toFixed(2);
  
  const paymentMatch = 
    parseFloat(dailySummary?.totalPayment || "0").toFixed(2) === 
    parseFloat(curtailmentStats[0]?.totalPayment || "0").toFixed(2);
  
  console.log(`3. DAILY SUMMARY CONSISTENCY CHECK`);
  console.log(`----------------------------------`);
  console.log(`Daily Summary Volume: ${formatNumber(dailySummary?.totalCurtailedEnergy || 0)} MWh`);
  console.log(`Curtailment Records Volume: ${formatNumber(curtailmentStats[0]?.totalVolume || 0)} MWh`);
  console.log(`Volume Match: ${volumeMatch ? '✅ YES' : '❌ NO'}`);
  console.log();
  console.log(`Daily Summary Payment: £${formatNumber(dailySummary?.totalPayment || 0)}`);
  console.log(`Curtailment Records Payment: £${formatNumber(curtailmentStats[0]?.totalPayment || 0)}`);
  console.log(`Payment Match: ${paymentMatch ? '✅ YES' : '❌ NO'}\n`);
  
  // 4. Check Bitcoin calculations
  console.log(`4. BITCOIN CALCULATION COMPLETENESS`);
  console.log(`-----------------------------------`);
  
  for (const minerModel of MINER_MODELS) {
    const bitcoinStats = await db
      .select({
        recordCount: count(),
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`,
        farms: sql<number>`COUNT(DISTINCT farm_id)`
      })
      .from(historicalBitcoinCalculations)
      .where(
        eq(historicalBitcoinCalculations.settlementDate, DATE) && 
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      );
      
    const btcPeriodsComplete = (bitcoinStats[0]?.periodCount || 0) === 48;
    
    console.log(`${minerModel}:`);
    console.log(`  Records: ${bitcoinStats[0]?.recordCount || 0}`);
    console.log(`  Periods: ${bitcoinStats[0]?.periodCount || 0} / 48 ${btcPeriodsComplete ? '✅' : '❌'}`);
    console.log(`  Unique Farms: ${bitcoinStats[0]?.farms || 0}`);
    console.log(`  Total Bitcoin: ${formatNumber(bitcoinStats[0]?.totalBitcoin || 0, 8)} BTC`);
    console.log();
  }
  
  // 5. Check for specific periods 11 and 12 that were previously missing
  const periods11And12 = await db
    .select({
      period: curtailmentRecords.settlementPeriod,
      recordCount: count(curtailmentRecords.id),
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(
      eq(curtailmentRecords.settlementDate, DATE) && 
      sql`settlement_period IN (11, 12)`
    )
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
  
  console.log(`5. PREVIOUSLY MISSING PERIODS CHECK`);
  console.log(`----------------------------------`);
  
  if (periods11And12.length === 2) {
    periods11And12.forEach(period => {
      console.log(`Period ${period.period}:`);
      console.log(`  Records: ${period.recordCount}`);
      console.log(`  Volume: ${formatNumber(period.totalVolume || 0)} MWh`);
      console.log(`  Payment: £${formatNumber(period.totalPayment || 0)}`);
      console.log();
    });
    console.log(`Status: ✅ FIXED - Both periods 11 and 12 are now present\n`);
  } else {
    console.log(`Status: ❌ ERROR - Expected 2 periods, found ${periods11And12.length}\n`);
  }
  
  // 6. Overall verification summary
  console.log(`6. OVERALL VERIFICATION SUMMARY`);
  console.log(`-------------------------------`);
  
  const isDataComplete = allPeriodsPresent && volumeMatch && paymentMatch;
  
  console.log(`Data Completeness: ${isDataComplete ? '✅ COMPLETE' : '❌ INCOMPLETE'}`);
  console.log(`All 48 Periods Present: ${allPeriodsPresent ? '✅ YES' : '❌ NO'}`);
  console.log(`Daily Summary Consistent: ${(volumeMatch && paymentMatch) ? '✅ YES' : '❌ NO'}`);
  console.log(`Previously Missing Periods Fixed: ${periods11And12.length === 2 ? '✅ YES' : '❌ NO'}`);
  
  console.log(`\n============================================`);
  console.log(`   VERIFICATION REPORT COMPLETE`);
  console.log(`============================================\n`);
}

main().catch(error => {
  console.error(`Error generating verification report: ${error}`);
  process.exit(1);
});