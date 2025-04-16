/**
 * Data Reprocessing Script for 2025-04-15
 * 
 * This script manually triggers a complete reprocessing of data for 2025-04-15,
 * including curtailment records, wind generation data, and Bitcoin calculations.
 * 
 * Run with: npx tsx reprocess-april15.ts
 */

import { processDailyCurtailment } from './server/services/curtailment_enhanced';
import { processSingleDay } from './server/services/bitcoinService';
import { processSingleDate } from './server/services/windGenerationService';
import { db } from './db';
import { 
  curtailmentRecords, 
  dailySummaries, 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries
} from './db/schema';
import { eq, sql, and } from 'drizzle-orm';
import { format } from 'date-fns';

const TARGET_DATE = '2025-04-15';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S']; // Standard miner models

async function reprocessData() {
  console.log(`\n=== Starting Complete Reprocessing for ${TARGET_DATE} ===`);
  
  try {
    // Step 1: Delete existing data for the target date to ensure a clean slate
    console.log(`\nRemoving existing curtailment records for ${TARGET_DATE}...`);
    const deleteCurtailmentResult = await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
    // Step 2: Remove all Bitcoin-related data for this date
    console.log(`Removing existing Bitcoin calculations for ${TARGET_DATE}...`);
    
    // Delete each miner model separately to ensure complete removal
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
    const deleteSummaryResult = await db.delete(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    // Step 4: Reprocess wind generation data (this is non-destructive)
    console.log(`\nProcessing wind generation data for ${TARGET_DATE}...`);
    try {
      const windRecords = await processSingleDate(TARGET_DATE);
      console.log(`Successfully processed ${windRecords} wind generation records for ${TARGET_DATE}`);
    } catch (error) {
      console.error(`Error processing wind generation data:`, error);
      // Continue with the process even if wind data fails
    }
    
    // Step 5: Reprocess curtailment data (this creates new records and updates summaries)
    console.log(`\nReprocessing curtailment data for ${TARGET_DATE}...`);
    try {
      await processDailyCurtailment(TARGET_DATE);
      console.log(`Successfully reprocessed curtailment data for ${TARGET_DATE}`);
    } catch (error) {
      console.error(`Error processing curtailment data:`, error);
      throw error;
    }
    
    // Step 6: Verify curtailment records
    const countResult = await db.select({
      count: sql<string>`COUNT(*)`,
      periodCount: sql<string>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nCurtailment Verification:`);
    console.log(`Records: ${countResult[0]?.count || '0'}`);
    console.log(`Settlement periods: ${countResult[0]?.periodCount || '0'}`);
    console.log(`Total volume: ${Number(countResult[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Total payment: £${Number(countResult[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Skip Bitcoin calculations if no curtailment data was found
    if (!countResult[0] || Number(countResult[0].count) === 0) {
      console.log(`No curtailment records found for ${TARGET_DATE}, skipping Bitcoin calculations.`);
    } else {
      // Step 7: Process Bitcoin calculations for each miner model
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
    }
    
    // Step 8: Check daily summary
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
        console.log(`Onshore Wind: ${summary[0].windOnshoreGeneration} MWh`);
        console.log(`Offshore Wind: ${summary[0].windOffshoreGeneration} MWh`);
      }
    } else {
      console.log(`No daily summary found for ${TARGET_DATE} after reprocessing.`);
    }
    
    console.log(`\n=== Reprocessing Complete ===`);
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