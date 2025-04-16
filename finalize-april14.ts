/**
 * Finalize Data Processing for 2025-04-14
 * 
 * This script finalizes the reprocessing by updating daily and monthly summaries
 * and generating Bitcoin calculations.
 * 
 * Run with: npx tsx finalize-april14.ts
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
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

async function finalize() {
  console.log(`\n=== Finalizing Data Processing for ${TARGET_DATE} ===`);
  console.log(`Start Time: ${format(new Date(), "yyyy-MM-dd HH:mm:ss")}`);
  
  try {
    // Step 1: Check current data
    const currentData = await db
      .select({
        recordCount: sql<string>`COUNT(*)`,
        periodCount: sql<string>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nCurrent Data for ${TARGET_DATE}:`);
    console.log(`Total Records: ${currentData[0]?.recordCount || 0}`);
    console.log(`Settlement Periods: ${currentData[0]?.periodCount || 0}`);
    console.log(`Total Volume: ${Number(currentData[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Total Payment: £${Number(currentData[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Get total volume from the database
    const dbEnergy = Number(currentData[0]?.totalVolume || 0);
    const dbPayment = Number(currentData[0]?.totalPayment || 0);
    
    if (dbEnergy === 0) {
      console.error("No curtailment data found for this date. Aborting.");
      return;
    }
    
    // Step 2: Update daily summary
    console.log(`\nUpdating daily summary for ${TARGET_DATE}...`);
    
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: dbEnergy.toString(),
      totalPayment: dbPayment.toString(),
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: dbEnergy.toString(),
        totalPayment: dbPayment.toString(),
        lastUpdated: new Date()
      }
    });
    
    console.log(`Daily summary updated with ${dbEnergy.toFixed(2)} MWh and £${dbPayment.toFixed(2)}`);
    
    // Step 3: Update monthly summary for April 2025
    const yearMonth = TARGET_DATE.substring(0, 7); // 2025-04
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
      
      console.log(`Monthly summary for ${yearMonth} updated with ${Number(monthlyTotals[0].totalCurtailedEnergy).toFixed(2)} MWh and £${Number(monthlyTotals[0].totalPayment).toFixed(2)}`);
    }
    
    // Step 4: Process Bitcoin calculations for each miner model
    console.log(`\nProcessing Bitcoin calculations for ${TARGET_DATE}...`);
    
    // Remove existing Bitcoin calculations first
    for (const minerModel of MINER_MODELS) {
      await db.delete(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
    }
    
    // Also remove bitcoin daily summaries if they exist
    await db.delete(bitcoinDailySummaries)
      .where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE));
    
    // Process each miner model
    for (const minerModel of MINER_MODELS) {
      try {
        console.log(`Processing ${minerModel}...`);
        await processSingleDay(TARGET_DATE, minerModel);
        console.log(`Successfully processed ${minerModel}`);
      } catch (error) {
        console.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
      }
    }
    
    // Step 5: Final verification
    console.log(`\nFinal Verification:`);
    
    // Check settlement periods processed
    const periodCounts = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        recordCount: sql<number>`COUNT(*)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
    
    console.log(`Settlement periods with data: ${periodCounts.length}`);
    if (periodCounts.length > 0) {
      console.log(`Period breakdown:`);
      for (const pc of periodCounts) {
        console.log(`  Period ${pc.period}: ${pc.recordCount} records`);
      }
    }
    
    // Check if Bitcoin calculations were created for each miner model
    for (const minerModel of MINER_MODELS) {
      const bitcoinCount = await db
        .select({ count: sql<number>`COUNT(*)` })
        .from(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      console.log(`Bitcoin calculations for ${minerModel}: ${bitcoinCount[0]?.count || 0} records`);
    }
    
    // Check daily summary exists
    const summary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    console.log(`\nDaily Summary After Processing:`);
    if (summary.length > 0) {
      console.log(`Date: ${summary[0].summaryDate}`);
      console.log(`Total Curtailed Energy: ${summary[0].totalCurtailedEnergy} MWh`);
      console.log(`Total Payment: £${summary[0].totalPayment}`);
      if (summary[0].totalWindGeneration) {
        console.log(`Total Wind Generation: ${summary[0].totalWindGeneration} MWh`);
      }
    }
    
    console.log(`\n=== Finalization Complete ===`);
    console.log(`End Time: ${format(new Date(), "yyyy-MM-dd HH:mm:ss")}`);
    
  } catch (error) {
    console.error("Fatal error during finalization:", error);
    process.exit(1);
  }
}

// Run the finalization script
finalize().catch(error => {
  console.error("Script execution error:", error);
  process.exit(1);
});