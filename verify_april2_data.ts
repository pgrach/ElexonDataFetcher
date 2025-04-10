/**
 * Verification Script for April 2, 2025 Data
 * 
 * This script checks the data completeness for April 2, 2025 in all relevant tables:
 * - curtailment_records
 * - historical_bitcoin_calculations
 * - daily_summaries
 * - bitcoin_daily_summaries
 * - monthly summaries
 * - yearly summaries
 * 
 * Usage: npx tsx verify_april2_data.ts
 */

import { db } from "./db";
import { 
  curtailmentRecords, 
  dailySummaries, 
  monthlySummaries, 
  yearlySummaries,
  historicalBitcoinCalculations,
  bitcoinDailySummaries,
  bitcoinMonthlySummaries, 
  bitcoinYearlySummaries
} from "./db/schema";
import { eq, sql, and } from "drizzle-orm";

const TARGET_DATE = "2025-04-02";
const YEAR_MONTH = "2025-04";
const YEAR = "2025";
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

async function verifyData() {
  console.log(`\n===== VERIFICATION FOR ${TARGET_DATE} =====\n`);
  
  // 1. Check curtailment records
  const curtailmentStats = await db
    .select({
      recordCount: sql<number>`COUNT(*)::int`,
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)::int`,
      farmCount: sql<number>`COUNT(DISTINCT farm_id)::int`,
      totalVolume: sql<string>`SUM(volume::numeric)::text`,
      totalPayment: sql<string>`SUM(payment::numeric)::text`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  console.log(`Curtailment Records:
- Total Records: ${curtailmentStats[0]?.recordCount || 0}
- Unique Settlement Periods: ${curtailmentStats[0]?.periodCount || 0}/48
- Unique Farms: ${curtailmentStats[0]?.farmCount || 0}
- Total Volume: ${Number(curtailmentStats[0]?.totalVolume || 0).toFixed(2)} MWh
- Total Payment: £${Number(curtailmentStats[0]?.totalPayment || 0).toFixed(2)}`);
  
  // 2. Check daily summary
  const dailySummary = await db
    .select({
      totalCurtailedEnergy: dailySummaries.totalCurtailedEnergy,
      totalPayment: dailySummaries.totalPayment,
    })
    .from(dailySummaries)
    .where(eq(dailySummaries.summaryDate, TARGET_DATE));
  
  console.log(`\nDaily Summary:
- Entry Exists: ${dailySummary.length > 0 ? 'Yes' : 'No'}
- Total Curtailed Energy: ${Number(dailySummary[0]?.totalCurtailedEnergy || 0).toFixed(2)} MWh
- Total Payment: £${Number(dailySummary[0]?.totalPayment || 0).toFixed(2)}`);
  
  // 3. Check Bitcoin calculations
  console.log("\nBitcoin Calculations:");
  for (const minerModel of MINER_MODELS) {
    const bitcoinStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)::int`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)::int`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)::int`,
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );
    
    console.log(`${minerModel}:
- Records: ${bitcoinStats[0]?.recordCount || 0}
- Periods: ${bitcoinStats[0]?.periodCount || 0}
- Farms: ${bitcoinStats[0]?.farmCount || 0}
- Bitcoin: ${Number(bitcoinStats[0]?.totalBitcoin || 0).toFixed(8)} BTC`);
  }
  
  // 4. Check Bitcoin daily summaries
  console.log("\nBitcoin Daily Summaries:");
  for (const minerModel of MINER_MODELS) {
    const dailyBitcoin = await db
      .select({
        bitcoinMined: bitcoinDailySummaries.bitcoinMined
      })
      .from(bitcoinDailySummaries)
      .where(
        and(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        )
      );
    
    console.log(`${minerModel}: ${Number(dailyBitcoin[0]?.bitcoinMined || 0).toFixed(8)} BTC`);
  }
  
  // 5. Check monthly summary
  const monthlySummary = await db
    .select({
      totalCurtailedEnergy: monthlySummaries.totalCurtailedEnergy,
      totalPayment: monthlySummaries.totalPayment
    })
    .from(monthlySummaries)
    .where(eq(monthlySummaries.yearMonth, YEAR_MONTH));
  
  console.log(`\nMonthly Summary (${YEAR_MONTH}):
- Entry Exists: ${monthlySummary.length > 0 ? 'Yes' : 'No'}
- Total Curtailed Energy: ${Number(monthlySummary[0]?.totalCurtailedEnergy || 0).toFixed(2)} MWh
- Total Payment: £${Number(monthlySummary[0]?.totalPayment || 0).toFixed(2)}`);
  
  // 6. Check Bitcoin monthly summaries
  console.log("\nBitcoin Monthly Summaries:");
  for (const minerModel of MINER_MODELS) {
    const monthlyBitcoin = await db
      .select({
        bitcoinMined: bitcoinMonthlySummaries.bitcoinMined
      })
      .from(bitcoinMonthlySummaries)
      .where(
        and(
          eq(bitcoinMonthlySummaries.yearMonth, YEAR_MONTH),
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        )
      );
    
    console.log(`${minerModel}: ${Number(monthlyBitcoin[0]?.bitcoinMined || 0).toFixed(8)} BTC`);
  }
  
  // 7. Check yearly summary
  const yearlySummary = await db
    .select({
      totalCurtailedEnergy: yearlySummaries.totalCurtailedEnergy,
      totalPayment: yearlySummaries.totalPayment
    })
    .from(yearlySummaries)
    .where(eq(yearlySummaries.year, YEAR));
  
  console.log(`\nYearly Summary (${YEAR}):
- Entry Exists: ${yearlySummary.length > 0 ? 'Yes' : 'No'}
- Total Curtailed Energy: ${Number(yearlySummary[0]?.totalCurtailedEnergy || 0).toFixed(2)} MWh
- Total Payment: £${Number(yearlySummary[0]?.totalPayment || 0).toFixed(2)}`);
  
  // 8. Check Bitcoin yearly summaries
  console.log("\nBitcoin Yearly Summaries:");
  for (const minerModel of MINER_MODELS) {
    const yearlyBitcoin = await db
      .select({
        bitcoinMined: bitcoinYearlySummaries.bitcoinMined
      })
      .from(bitcoinYearlySummaries)
      .where(
        and(
          eq(bitcoinYearlySummaries.year, YEAR),
          eq(bitcoinYearlySummaries.minerModel, minerModel)
        )
      );
    
    console.log(`${minerModel}: ${Number(yearlyBitcoin[0]?.bitcoinMined || 0).toFixed(8)} BTC`);
  }
  
  console.log("\n===== VERIFICATION COMPLETE =====");
}

// Run the verification
verifyData().catch(console.error);