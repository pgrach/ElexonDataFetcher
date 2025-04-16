/**
 * Verify and Update Bitcoin Calculations for April 14, 2025
 * 
 * This script performs verification and updates Bitcoin calculations for 2025-04-14
 * after data has been imported from Elexon.
 * 
 * Run with: npx tsx verify-bitcoin-april14.ts
 */

import { db } from './db';
import { 
  curtailmentRecords, 
  dailySummaries, 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries,
  monthlySummaries
} from './db/schema';
import { eq, sql, and } from 'drizzle-orm';
import { format } from 'date-fns';
import { processSingleDay } from './server/services/bitcoinService';

const TARGET_DATE = '2025-04-14';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S']; // Standard miner models

async function verifyAndUpdateBitcoin() {
  console.log(`\n=== Verifying and Updating Bitcoin Calculations for ${TARGET_DATE} ===`);
  
  try {
    // First check if we have curtailment records
    const curtailmentCheck = await db
      .select({
        recordCount: sql<string>`COUNT(*)`,
        periodCount: sql<string>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const recordCount = Number(curtailmentCheck[0]?.recordCount || 0);
    const periodCount = Number(curtailmentCheck[0]?.periodCount || 0);
    const totalVolume = Number(curtailmentCheck[0]?.totalVolume || 0);
    const totalPayment = Number(curtailmentCheck[0]?.totalPayment || 0);
    
    console.log(`Curtailment Records in Database:`);
    console.log(`- Total Records: ${recordCount}`);
    console.log(`- Settlement Periods: ${periodCount}`);
    console.log(`- Total Volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`- Total Payment: £${totalPayment.toFixed(2)}`);
    
    if (recordCount === 0) {
      console.log('No curtailment records found. Cannot update Bitcoin calculations.');
      return;
    }
    
    // Check if daily summary exists, if not create it
    console.log(`\nChecking daily summary for ${TARGET_DATE}...`);
    const existingSummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    if (existingSummary.length === 0) {
      console.log(`Creating daily summary for ${TARGET_DATE}...`);
      await db.insert(dailySummaries).values({
        summaryDate: TARGET_DATE,
        totalCurtailedEnergy: totalVolume.toString(),
        totalPayment: totalPayment.toString(),
        lastUpdated: new Date()
      });
    } else {
      console.log(`Updating daily summary for ${TARGET_DATE}...`);
      await db.update(dailySummaries)
        .set({
          totalCurtailedEnergy: totalVolume.toString(),
          totalPayment: totalPayment.toString(),
          lastUpdated: new Date()
        })
        .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    }
    
    // Update monthly summary
    const yearMonth = TARGET_DATE.substring(0, 7);
    console.log(`\nUpdating monthly summary for ${yearMonth}...`);
    
    // Calculate total from all daily summaries in this month
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${TARGET_DATE}::date)`);

    if (monthlyTotals[0].totalCurtailedEnergy && monthlyTotals[0].totalPayment) {
      await db.insert(monthlySummaries).values({
        yearMonth,
        totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
        totalPayment: monthlyTotals[0].totalPayment,
        updatedAt: new Date()
      }).onConflictDoUpdate({
        target: [monthlySummaries.yearMonth],
        set: {
          totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
          totalPayment: monthlyTotals[0].totalPayment,
          updatedAt: new Date()
        }
      });
    }
    
    // Check if Bitcoin calculations exist, if not create them
    console.log(`\nChecking Bitcoin calculations for ${TARGET_DATE}...`);
    
    for (const minerModel of MINER_MODELS) {
      const bitcoinCheck = await db
        .select({ count: sql<string>`COUNT(*)` })
        .from(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
        
      const bitcoinCount = Number(bitcoinCheck[0]?.count || 0);
      
      if (bitcoinCount === 0) {
        console.log(`Creating Bitcoin calculations for ${minerModel}...`);
        try {
          // Process Bitcoin calculations for this date and model
          await processSingleDay(TARGET_DATE, minerModel);
          console.log(`Successfully processed ${minerModel}`);
        } catch (error) {
          console.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
        }
      } else {
        console.log(`Bitcoin calculations already exist for ${minerModel}: ${bitcoinCount} records`);
      }
    }
    
    // Final verification
    console.log(`\nPerforming final verification...`);
    
    // Verify daily summary
    const updatedSummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    console.log(`Daily Summary verification:`);
    if (updatedSummary.length > 0) {
      console.log(`- Date: ${updatedSummary[0].summaryDate}`);
      console.log(`- Total Curtailed Energy: ${updatedSummary[0].totalCurtailedEnergy} MWh`);
      console.log(`- Total Payment: £${updatedSummary[0].totalPayment}`);
    } else {
      console.log(`Error: No daily summary found for ${TARGET_DATE} after updating.`);
    }
    
    // Verify Bitcoin calculations
    const bitcoinStats = await db
      .select({
        minerModel: historicalBitcoinCalculations.minerModel,
        count: sql<number>`COUNT(*)`,
        totalBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`,
        totalEnergy: sql<string>`SUM(${historicalBitcoinCalculations.energyConsumption}::numeric)`
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE))
      .groupBy(historicalBitcoinCalculations.minerModel);
    
    console.log(`\nBitcoin Calculations summary:`);
    if (bitcoinStats.length > 0) {
      for (const stat of bitcoinStats) {
        console.log(`- ${stat.minerModel}: ${stat.count} records, ${Number(stat.totalBitcoin).toFixed(8)} BTC, ${Number(stat.totalEnergy).toFixed(2)} MWh`);
      }
    } else {
      console.log(`Error: No Bitcoin calculations found for ${TARGET_DATE} after processing.`);
    }
    
    console.log(`\n=== Verification and Update Complete ===`);
    console.log(`Completed at: ${format(new Date(), "yyyy-MM-dd HH:mm:ss")}`);
    
  } catch (error) {
    console.error("Error during verification and update:", error);
    process.exit(1);
  }
}

// Run the verification and update script
verifyAndUpdateBitcoin().catch(error => {
  console.error("Script execution error:", error);
  process.exit(1);
});