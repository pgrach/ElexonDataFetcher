/**
 * Complete Data Reprocessing Script for 2025-04-14
 * 
 * This script performs a thorough reprocessing of ALL 48 settlement periods for 2025-04-14,
 * ensuring complete data capture from Elexon with no omissions.
 * 
 * Run with: npx tsx reprocess-april14.ts
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

const TARGET_DATE = '2025-04-14';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const ALL_SETTLEMENT_PERIODS = Array.from({ length: 48 }, (_, i) => i + 1);

// Store processed data for reporting
interface PeriodStats {
  period: number;
  records: number;
  volume: number;
  payment: number;
}

// Process each settlement period individually with retries
async function processSettlementPeriod(date: string, period: number): Promise<PeriodStats> {
  console.log(`[${date} P${period}] Processing settlement period...`);
  
  const MAX_RETRIES = 3;
  const stats: PeriodStats = { period, records: 0, volume: 0, payment: 0 };
  
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      // Fetch data from Elexon API
      const records = await fetchBidsOffers(date, period);
      console.log(`[${date} P${period}] Retrieved ${records.length} records from Elexon`);
      
      // First delete any existing records for this period to avoid duplication
      await db.delete(curtailmentRecords)
        .where(and(
          eq(curtailmentRecords.settlementDate, date),
          eq(curtailmentRecords.settlementPeriod, period)
        ));
      
      // Filter for curtailment records (negative volume with flags)
      const curtailmentRecordsToInsert = records
        .filter(record => record.volume < 0 && (record.soFlag || record.cadlFlag))
        .map(record => {
          const absVolume = Math.abs(record.volume);
          const payment = absVolume * record.originalPrice;
          
          stats.records++;
          stats.volume += absVolume;
          stats.payment += payment;
          
          return {
            settlementDate: date,
            settlementPeriod: period,
            farmId: record.id,
            leadPartyName: record.leadPartyName || 'Unknown',
            volume: record.volume.toString(), // Keep negative
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
        console.log(`[${date} P${period}] Inserted ${curtailmentRecordsToInsert.length} curtailment records`);
        console.log(`[${date} P${period}] Volume: ${stats.volume.toFixed(2)} MWh, Payment: £${stats.payment.toFixed(2)}`);
      } else {
        console.log(`[${date} P${period}] No valid curtailment records found`);
      }
      
      return stats;
    } catch (error) {
      if (attempt < MAX_RETRIES) {
        console.error(`[${date} P${period}] Error on attempt ${attempt}/${MAX_RETRIES}:`, error);
        console.log(`[${date} P${period}] Retrying...`);
        // Wait before retrying (exponential backoff)
        await new Promise(resolve => setTimeout(resolve, 1000 * Math.pow(2, attempt - 1)));
      } else {
        console.error(`[${date} P${period}] Failed after ${MAX_RETRIES} attempts:`, error);
        throw error;
      }
    }
  }
  
  return stats;
}

async function reprocessData() {
  console.log(`\n=== Starting Complete Reprocessing for ${TARGET_DATE} ===`);
  console.log(`Start Time: ${format(new Date(), "yyyy-MM-dd HH:mm:ss")}`);
  
  try {
    // Step 1: Delete existing data for the target date
    console.log(`\nRemoving existing curtailment records for ${TARGET_DATE}...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
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
    
    console.log(`Removing existing daily summary for ${TARGET_DATE}...`);
    await db.delete(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    // Step 2: Process wind generation data
    console.log(`\nProcessing wind generation data for ${TARGET_DATE}...`);
    try {
      const windRecords = await processSingleDate(TARGET_DATE);
      console.log(`Successfully processed ${windRecords} wind generation records`);
    } catch (error) {
      console.error(`Error processing wind generation data:`, error);
    }
    
    // Step 3: Process all 48 settlement periods
    console.log(`\nProcessing ALL 48 settlement periods for ${TARGET_DATE}...`);
    
    const periodStats: PeriodStats[] = [];
    const processedPeriods = new Set<number>();
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process each period one by one
    for (const period of ALL_SETTLEMENT_PERIODS) {
      try {
        const stats = await processSettlementPeriod(TARGET_DATE, period);
        periodStats.push(stats);
        
        if (stats.records > 0) {
          processedPeriods.add(period);
          totalRecords += stats.records;
          totalVolume += stats.volume;
          totalPayment += stats.payment;
        }
      } catch (error) {
        console.error(`Error processing period ${period}:`, error);
      }
      
      // Short delay between periods to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    console.log(`\nComplete! Processed ${periodStats.length} settlement periods`);
    console.log(`Periods with curtailment data: ${processedPeriods.size}`);
    console.log(`Total curtailment records: ${totalRecords}`);
    console.log(`Total curtailed energy: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${totalPayment.toFixed(2)}`);
    
    // Step 4: Update daily summary
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
    
    console.log(`Daily summary updated with ${dbEnergy.toFixed(2)} MWh and £${dbPayment.toFixed(2)}`);
    
    // Step 5: Update monthly summary for April 2025
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
      
      console.log(`Monthly summary for ${yearMonth} updated`);
    }
    
    // Step 6: Process Bitcoin calculations for each miner model
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
    
    // Step 7: Final verification
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
    
    console.log(`\nDaily Summary After Reprocessing:`);
    if (summary.length > 0) {
      console.log(`Date: ${summary[0].summaryDate}`);
      console.log(`Total Curtailed Energy: ${summary[0].totalCurtailedEnergy} MWh`);
      console.log(`Total Payment: £${summary[0].totalPayment}`);
      if (summary[0].totalWindGeneration) {
        console.log(`Total Wind Generation: ${summary[0].totalWindGeneration} MWh`);
      }
    }
    
    console.log(`\n=== Reprocessing Complete ===`);
    console.log(`End Time: ${format(new Date(), "yyyy-MM-dd HH:mm:ss")}`);
    
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