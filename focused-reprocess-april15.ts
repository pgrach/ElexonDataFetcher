/**
 * Focused Data Reprocessing Script for 2025-04-15
 * 
 * This script targets specific settlement periods that are known to have data
 * for more efficient processing.
 * 
 * Run with: npx tsx focused-reprocess-april15.ts
 */

import { db } from './db';
import { 
  curtailmentRecords, 
  dailySummaries, 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries,
  monthlySummaries
} from './db/schema';
import { eq, sql, and, inArray } from 'drizzle-orm';
import { format } from 'date-fns';
import { fetchBidsOffers } from './server/services/elexon';
import { processSingleDay } from './server/services/bitcoinService';
import { processSingleDate } from './server/services/windGenerationService';

const TARGET_DATE = '2025-04-15';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S']; // Standard miner models

// Target known periods with data based on previous runs
const TARGET_PERIODS = [45, 46, 47, 48];

// Query Elexon for curtailment data records
async function fetchElexonData(date: string, period: number): Promise<any[]> {
  try {
    console.log(`[${date} P${period}] Fetching data from Elexon...`);
    const records = await fetchBidsOffers(date, period);
    console.log(`[${date} P${period}] Retrieved ${records.length} records from Elexon`);
    return records;
  } catch (error) {
    console.error(`[${date} P${period}] Error fetching data:`, error);
    throw error;
  }
}

async function reprocessData() {
  console.log(`\n=== Starting Focused Reprocessing for ${TARGET_DATE} ===`);
  
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
    console.log(`\nProcessing targeted settlement periods for ${TARGET_DATE}: ${TARGET_PERIODS.join(', ')}...`);
    
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (const period of TARGET_PERIODS) {
      try {
        // Fetch and process raw data for each period
        const elexonRecords = await fetchElexonData(TARGET_DATE, period);
        
        // Process each record that has negative volume (curtailment)
        const curtailmentRecordsToInsert = elexonRecords
          .filter(record => record.volume < 0 && (record.soFlag || record.cadlFlag))
          .map(record => {
            const absVolume = Math.abs(record.volume);
            const payment = absVolume * record.originalPrice;
            
            totalVolume += absVolume;
            totalPayment += payment;
            totalRecords++;
            
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
        
        // Insert all valid records for this period
        if (curtailmentRecordsToInsert.length > 0) {
          await db.insert(curtailmentRecords).values(curtailmentRecordsToInsert);
          console.log(`[${TARGET_DATE} P${period}] Inserted ${curtailmentRecordsToInsert.length} curtailment records`);
        } else {
          console.log(`[${TARGET_DATE} P${period}] No valid curtailment records to insert`);
        }
      } catch (error) {
        console.error(`Error processing period ${period}:`, error);
      }
    }
    
    console.log(`\nProcessed ${totalRecords} total curtailment records`);
    console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${totalPayment.toFixed(2)}`);
    
    if (totalRecords === 0) {
      console.log(`No curtailment records found for ${TARGET_DATE}, skipping summary updates.`);
      return;
    }
    
    // Step 6: Update daily summary
    console.log(`\nUpdating daily summary for ${TARGET_DATE}...`);
    
    // Calculate totals from the database to ensure accuracy
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
    
    // Step 9: Final verification
    const verification = await db.select({
      recordCount: sql<string>`COUNT(*)`,
      periodCount: sql<string>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nFinal Verification:`);
    console.log(`Total Records: ${verification[0]?.recordCount || '0'}`);
    console.log(`Settlement Periods: ${verification[0]?.periodCount || '0'}`);
    console.log(`Total Volume: ${Number(verification[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Total Payment: £${Number(verification[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Step 10: Check daily summary
    const summary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    console.log(`\nDaily Summary After Reprocessing:`);
    if (summary.length > 0) {
      console.log(`Date: ${summary[0].summaryDate}`);
      console.log(`Total Curtailed Energy: ${summary[0].totalCurtailedEnergy} MWh`);
      console.log(`Total Payment: £${summary[0].totalPayment}`);
      if (summary[0].totalWindGeneration) {
        console.log(`Total Wind Generation: ${summary[0].totalWindGeneration} MWh`);
      }
    }
    
    console.log(`\n=== Focused Reprocessing Complete ===`);
    console.log(`Completed at: ${format(new Date(), "yyyy-MM-dd HH:mm:ss")}`);
    
  } catch (error) {
    console.error("Fatal error during reprocessing:", error);
    process.exit(1);
  }
}

// Run the reprocessing script
reprocessData().catch(error => {
  console.error("Script execution error:", error);
  process.exit(1);
});