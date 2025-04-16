/**
 * Complete Data Reprocessing Script for 2025-04-15
 * 
 * This script performs a thorough reprocessing of data for 2025-04-15,
 * ensuring ALL data from Elexon is captured with no omissions.
 * It includes special handling to ensure complete processing of each period.
 * 
 * Run with: npx tsx complete-reprocess-april15.ts
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

const TARGET_DATE = '2025-04-15';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S']; // Standard miner models

// Function to load wind farm BMU IDs
async function loadWindFarmIds(): Promise<Set<string>> {
  try {
    console.log('Loading BMU mapping from data file...');
    // Get BMU mappings from database (reading data files directly can be unreliable)
    const result = await db.execute(sql`
      SELECT DISTINCT farm_id 
      FROM curtailment_records 
      WHERE settlement_date >= '2025-01-01'
    `);
    
    const windFarmIds = new Set<string>();
    if (result && result.rows) {
      for (const row of result.rows) {
        if (row.farm_id) {
          windFarmIds.add(row.farm_id as string);
        }
      }
    }
    
    console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs from database`);
    return windFarmIds;
  } catch (error) {
    console.error('Error loading wind farm BMU IDs:', error);
    throw error;
  }
}

// Directly process a single settlement period with retries
async function processSettlementPeriod(date: string, period: number): Promise<{ volume: number; payment: number }> {
  console.log(`[${date} P${period}] Fetching data from Elexon...`);
  
  const MAX_RETRIES = 3;
  
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      // Fetch data from Elexon API
      const records = await fetchBidsOffers(date, period);
      console.log(`[${date} P${period}] Records from API: ${records.length}`);
      
      // Get valid wind farm IDs
      const validWindFarmIds = await loadWindFarmIds();
      
      // Filter for curtailment records (negative volume with flags)
      const validRecords = records.filter(record => 
        record.volume < 0 &&
        (record.soFlag || record.cadlFlag) &&
        validWindFarmIds.has(record.id)
      );
      
      console.log(`[${date} P${period}] Valid curtailment records: ${validRecords.length}`);
      
      if (validRecords.length === 0) {
        console.log(`[${date} P${period}] No valid curtailment records found.`);
        return { volume: 0, payment: 0 };
      }
      
      // Delete existing records for this date and period
      await db.delete(curtailmentRecords)
        .where(and(
          eq(curtailmentRecords.settlementDate, date),
          eq(curtailmentRecords.settlementPeriod, period)
        ));
      
      // Process and insert each valid record
      let totalVolume = 0;
      let totalPayment = 0;
      
      for (const record of validRecords) {
        const volume = Math.abs(record.volume);
        const payment = volume * record.originalPrice;
        
        console.log(`[${date} P${period}] Processing record for ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
        
        // Insert the record
        await db.insert(curtailmentRecords).values({
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
        });
        
        totalVolume += volume;
        totalPayment += payment;
      }
      
      console.log(`[${date} P${period}] Total processed: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
      return { volume: totalVolume, payment: totalPayment };
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
  
  return { volume: 0, payment: 0 };
}

async function reprocessData() {
  console.log(`\n=== Starting Complete Reprocessing for ${TARGET_DATE} ===`);
  
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
    
    // Step 5: Process each settlement period individually (1-48)
    console.log(`\nProcessing all 48 settlement periods for ${TARGET_DATE}...`);
    let totalVolume = 0;
    let totalPayment = 0;
    const processedPeriods = new Set<number>();
    
    for (let period = 1; period <= 48; period++) {
      try {
        const result = await processSettlementPeriod(TARGET_DATE, period);
        totalVolume += result.volume;
        totalPayment += result.payment;
        
        if (result.volume > 0 || result.payment > 0) {
          processedPeriods.add(period);
        }
      } catch (error) {
        console.error(`Error processing period ${period}:`, error);
      }
    }
    
    console.log(`\nProcessed ${processedPeriods.size} periods with curtailment data`);
    console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${totalPayment.toFixed(2)}`);
    
    // Step 6: Update daily summary
    console.log(`\nUpdating daily summary for ${TARGET_DATE}...`);
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: totalVolume.toString(),
      totalPayment: (-totalPayment).toString(), // Store as negative
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totalVolume.toString(),
        totalPayment: (-totalPayment).toString(), // Store as negative
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
    
    // Proceed with Bitcoin calculations if we have curtailment data
    if (totalVolume > 0) {
      // Step 8: Process Bitcoin calculations for each miner model
      console.log(`\nProcessing Bitcoin calculations for ${TARGET_DATE}...`);
      
      for (const minerModel of MINER_MODELS) {
        try {
          console.log(`Processing ${minerModel}...`);
          
          // Double-check that Bitcoin calculations were deleted
          await db.delete(historicalBitcoinCalculations)
            .where(and(
              eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
              eq(historicalBitcoinCalculations.minerModel, minerModel)
            ));
            
          // Process Bitcoin calculations for this date and model
          await processSingleDay(TARGET_DATE, minerModel);
          console.log(`Successfully processed ${minerModel}`);
        } catch (error) {
          console.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
          // Continue with other models even if one fails
        }
      }
    } else {
      console.log(`No curtailment volume to process for Bitcoin calculations.`);
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
    } else {
      console.log(`No daily summary found for ${TARGET_DATE} after reprocessing.`);
    }
    
    console.log(`\n=== Complete Reprocessing Finished ===`);
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