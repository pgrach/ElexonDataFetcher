/**
 * Direct Data Reprocessing Script for 2025-04-14
 * 
 * This script directly reprocesses data for 2025-04-14 by calling the Elexon API
 * with specific periods known to have data.
 * 
 * Run with: npx tsx direct-reprocess-april14.ts
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
import { fetchBidsOffers } from './server/services/elexon';
import { processSingleDay } from './server/services/bitcoinService';
import { processSingleDate } from './server/services/windGenerationService';

const TARGET_DATE = '2025-04-14';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S']; // Standard miner models

// These are the settlement periods where we found data in previous runs
// We'll focus on these to make processing faster
const TARGET_PERIODS = [
  1, 4, 5, 6, 7, 8, 9, 10, 
  11, 12, 13, 14, 15, 16, 17, 18, 
  19, 20, 21, 22, 23, 24, 25, 26, 
  27, 28, 29, 30, 31, 32
];

async function directReprocess() {
  console.log(`\n=== Starting Direct Reprocessing for ${TARGET_DATE} ===`);
  const startTime = Date.now();
  
  try {
    // Step 1: Delete existing data for the target date to ensure a clean slate
    console.log(`\nRemoving existing curtailment records for ${TARGET_DATE}...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
    // Step 2: Remove all Bitcoin-related data for this date
    console.log(`Removing existing Bitcoin calculations for ${TARGET_DATE}...`);
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
    
    // Step 3: Remove daily summary to force recalculation
    console.log(`Removing existing daily summary for ${TARGET_DATE}...`);
    await db.delete(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    // Step 4: Reprocess wind generation data (this is non-destructive)
    console.log(`\nProcessing wind generation data for ${TARGET_DATE}...`);
    try {
      const windRecords = await processSingleDate(TARGET_DATE);
      console.log(`Successfully processed ${windRecords} wind generation records for ${TARGET_DATE}`);
    } catch (error) {
      console.error(`Error processing wind generation data:`, error);
    }
    
    // Step 5: Process targeted settlement periods
    console.log(`\nProcessing targeted settlement periods for ${TARGET_DATE}...`);
    
    let allCurtailmentRecords = [];
    
    // Fetch all periods concurrently for speed
    const periodPromises = TARGET_PERIODS.map(async (period) => {
      try {
        console.log(`Fetching period ${period}...`);
        const records = await fetchBidsOffers(TARGET_DATE, period);
        
        // Filter for curtailment records (negative volume with flags)
        return records.filter(record => 
          record.volume < 0 && (record.soFlag || record.cadlFlag)
        ).map(record => {
          const absVolume = Math.abs(record.volume);
          const payment = absVolume * record.originalPrice;
          
          return {
            settlementDate: TARGET_DATE,
            settlementPeriod: period,
            farmId: record.id,
            leadPartyName: record.leadPartyName || 'Unknown',
            volume: record.volume.toString(), // Keep negative value
            payment: payment.toString(),
            originalPrice: record.originalPrice.toString(),
            finalPrice: record.finalPrice.toString(),
            soFlag: record.soFlag,
            cadlFlag: record.cadlFlag
          };
        });
      } catch (error) {
        console.error(`Error fetching period ${period}:`, error);
        return [];
      }
    });
    
    // Wait for all period fetches to complete
    const periodResults = await Promise.all(periodPromises);
    
    // Combine all records
    for (let i = 0; i < periodResults.length; i++) {
      const periodRecords = periodResults[i];
      const period = TARGET_PERIODS[i];
      
      if (periodRecords.length > 0) {
        console.log(`Period ${period}: Found ${periodRecords.length} curtailment records`);
        allCurtailmentRecords = [...allCurtailmentRecords, ...periodRecords];
      }
    }
    
    console.log(`\nInserting ${allCurtailmentRecords.length} total curtailment records...`);
    
    // Insert all records in chunks to avoid overwhelming the database
    const CHUNK_SIZE = 100;
    for (let i = 0; i < allCurtailmentRecords.length; i += CHUNK_SIZE) {
      const chunk = allCurtailmentRecords.slice(i, i + CHUNK_SIZE);
      await db.insert(curtailmentRecords).values(chunk);
      console.log(`Inserted records ${i+1}-${Math.min(i+CHUNK_SIZE, allCurtailmentRecords.length)}`);
    }
    
    // Calculate totals from inserted records
    const dbTotals = await db
      .select({
        recordCount: sql<string>`COUNT(*)`,
        periodCount: sql<string>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const recordCount = Number(dbTotals[0]?.recordCount || 0);
    const periodCount = Number(dbTotals[0]?.periodCount || 0);
    const totalVolume = Number(dbTotals[0]?.totalVolume || 0);
    const totalPayment = Number(dbTotals[0]?.totalPayment || 0);
    
    console.log(`\nProcessed ${recordCount} total curtailment records across ${periodCount} periods`);
    console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${totalPayment.toFixed(2)}`);
    
    if (recordCount === 0) {
      console.log(`No curtailment records found for ${TARGET_DATE}, skipping summary updates.`);
      return;
    }
    
    // Step 6: Update daily summary
    console.log(`\nUpdating daily summary for ${TARGET_DATE}...`);
    
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: totalVolume.toString(),
      totalPayment: totalPayment.toString(),
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totalVolume.toString(),
        totalPayment: totalPayment.toString(),
        lastUpdated: new Date()
      }
    });
    
    // Step 7: Update monthly summary
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
    
    // Step 8: Process Bitcoin calculations for each miner model
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
    
    // Final verification
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
    
    // Verify Bitcoin calculations
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
    
    const endTime = Date.now();
    const duration = (endTime - startTime) / 1000;
    
    console.log(`\n=== Direct Reprocessing Complete ===`);
    console.log(`Completed at: ${format(new Date(), "yyyy-MM-dd HH:mm:ss")}`);
    console.log(`Total duration: ${duration.toFixed(2)} seconds`);
    
  } catch (error) {
    console.error("Error during reprocessing:", error);
    process.exit(1);
  }
}

// Run the reprocessing script
directReprocess().catch(error => {
  console.error("Script execution error:", error);
  process.exit(1);
});