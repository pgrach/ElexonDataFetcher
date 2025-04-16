/**
 * Complete the April 14 Summary and Bitcoin Calculations
 * 
 * This script completes the reprocessing for 2025-04-14 by creating the daily summary
 * and processing Bitcoin calculations using the curtailment records that were already fetched.
 * 
 * Run with: npx tsx complete-april14-summary.ts
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

async function completeProcessing() {
  console.log(`\n=== Completing Processing for ${TARGET_DATE} ===`);
  
  try {
    // Step 1: First, let's check what we have so far
    const dbVerification = await db.select({
      recordCount: sql<string>`COUNT(*)`,
      periodCount: sql<string>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nCurrent Database State for ${TARGET_DATE}:`);
    console.log(`Total Records: ${dbVerification[0]?.recordCount || '0'}`);
    console.log(`Settlement Periods: ${dbVerification[0]?.periodCount || '0'}`);
    console.log(`Total Volume: ${Number(dbVerification[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Total Payment: £${Number(dbVerification[0]?.totalPayment || 0).toFixed(2)}`);
    
    if (Number(dbVerification[0]?.recordCount || 0) === 0) {
      console.log('No curtailment records found. Cannot complete processing.');
      return;
    }
    
    // Step 2: Delete existing Bitcoin calculations if any exist
    console.log(`\nRemoving any existing Bitcoin calculations for ${TARGET_DATE}...`);
    for (const minerModel of MINER_MODELS) {
      await db.delete(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
    }
    
    // Remove bitcoin daily summaries if they exist
    await db.delete(bitcoinDailySummaries)
      .where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE));
    
    // Step 3: Create or update daily summary
    console.log(`\nCreating daily summary for ${TARGET_DATE}...`);
    
    const dbTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const dbEnergy = Number(dbTotals[0]?.totalCurtailedEnergy || 0);
    const dbPayment = Number(dbTotals[0]?.totalPayment || 0);
    
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
    
    // Step 4: Update monthly summary
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
    
    // Step 5: Process Bitcoin calculations for each miner model
    console.log(`\nProcessing Bitcoin calculations for ${TARGET_DATE}...`);
    
    for (const minerModel of MINER_MODELS) {
      try {
        console.log(`Processing ${minerModel}...`);
        await processSingleDay(TARGET_DATE, minerModel);
        console.log(`Successfully processed ${minerModel}`);
      } catch (error) {
        console.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
      }
    }
    
    // Step 6: Final verification
    console.log(`\nVerifying daily summary was created...`);
    const summary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    if (summary.length > 0) {
      console.log(`Daily Summary Created:`);
      console.log(`Date: ${summary[0].summaryDate}`);
      console.log(`Total Curtailed Energy: ${summary[0].totalCurtailedEnergy} MWh`);
      console.log(`Total Payment: £${summary[0].totalPayment}`);
    } else {
      console.log(`Error: Daily summary not found after processing.`);
    }
    
    // Step 7: Verify Bitcoin calculations
    console.log(`\nVerifying Bitcoin calculations...`);
    const bitcoinCalcs = await db
      .select({
        minerModel: historicalBitcoinCalculations.minerModel,
        count: sql<number>`COUNT(*)`
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE))
      .groupBy(historicalBitcoinCalculations.minerModel);
    
    if (bitcoinCalcs.length > 0) {
      console.log(`Bitcoin Calculations Created:`);
      for (const calc of bitcoinCalcs) {
        console.log(`${calc.minerModel}: ${calc.count} records`);
      }
    } else {
      console.log(`Error: No Bitcoin calculations found after processing.`);
    }
    
    console.log(`\n=== Processing Complete ===`);
    console.log(`Completed at: ${format(new Date(), "yyyy-MM-dd HH:mm:ss")}`);
    
  } catch (error) {
    console.error("Error during processing:", error);
    process.exit(1);
  }
}

// Run the processing script
completeProcessing().catch(error => {
  console.error("Script execution error:", error);
  process.exit(1);
});