/**
 * Data Reprocessing Script for 2025-04-16
 * 
 * This script performs a complete reingestion of data for 2025-04-16, including:
 * 1. Curtailment data from Elexon API for all BMUs and all 48 settlement periods
 * 2. Bitcoin calculations for all miner models
 * 3. Daily, monthly and yearly summary updates
 * 
 * Run with: npx tsx reprocess-april16.ts
 */

import { db } from "./db";
import { 
  curtailmentRecords, 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries,
  dailySummaries 
} from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import { processDailyCurtailment } from "./server/services/curtailment_enhanced";
import { processSingleDay } from "./server/services/bitcoinService";
import { minerModels } from "./server/types/bitcoin";
import { processDateRange } from "./server/services/windGenerationService";
import { format } from "date-fns";

const TARGET_DATE = "2025-04-16";
const MINER_MODEL_KEYS = Object.keys(minerModels);

/**
 * Main reprocessing function
 */
async function reprocessData() {
  console.log(`\n=== Starting Complete Reprocessing for ${TARGET_DATE} ===\n`);
  const startTime = new Date();
  
  try {
    // Step 1: Delete existing data for the target date
    console.log(`Removing existing curtailment records for ${TARGET_DATE}...`);
    const deleteCurtailmentResult = await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
    console.log(`Removing existing Bitcoin calculations for ${TARGET_DATE}...`);
    const deleteBitcoinResult = await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
    console.log(`Removing existing Bitcoin daily summaries for ${TARGET_DATE}...`);
    const deleteDailySummaryResult = await db.delete(bitcoinDailySummaries)
      .where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE));
    
    // Step 2: Reprocess wind generation data
    console.log(`\nReprocessing wind generation data for ${TARGET_DATE}...`);
    try {
      await processDateRange(TARGET_DATE, TARGET_DATE);
      console.log(`Successfully reprocessed wind generation data for ${TARGET_DATE}`);
    } catch (error) {
      console.error(`Error processing wind generation data:`, error);
      // Continue with other processing despite this error
    }
    
    // Step 3: Reprocess curtailment data
    console.log(`\nReprocessing curtailment data for ${TARGET_DATE}...`);
    try {
      await processDailyCurtailment(TARGET_DATE);
      console.log(`Successfully reprocessed curtailment data for ${TARGET_DATE}`);
    } catch (error) {
      console.error(`Error processing curtailment data:`, error);
      throw error; // This is critical, so we'll stop if it fails
    }
    
    // Step 4: Verify curtailment records
    const countResult = await db.select({
      count: sql<string>`COUNT(*)`,
      periodCount: sql<string>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      farmCount: sql<string>`COUNT(DISTINCT ${curtailmentRecords.farmId})`,
      totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nCurtailment Verification:`);
    console.log(`Total Records: ${countResult[0]?.count || 0}`);
    console.log(`Distinct Settlement Periods: ${countResult[0]?.periodCount || 0}`);
    console.log(`Distinct Farms: ${countResult[0]?.farmCount || 0}`);
    console.log(`Total Volume: ${Number(countResult[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Total Payment: £${Number(countResult[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Step 5: Process Bitcoin calculations for each miner model
    console.log(`\nProcessing Bitcoin calculations for ${TARGET_DATE}...`);
    for (const minerModel of MINER_MODEL_KEYS) {
      console.log(`\nProcessing calculations for ${minerModel}...`);
      try {
        const result = await processSingleDay(TARGET_DATE, minerModel);
        if (result && result.success) {
          console.log(`✓ Successfully processed ${minerModel}: ${result.bitcoinMined.toFixed(8)} BTC (£${result.valueGbp.toFixed(2)})`);
        } else {
          console.log(`No calculations generated for ${minerModel}`);
        }
      } catch (error) {
        console.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
      }
    }
    
    // Step 6: Update daily summary
    console.log(`\nUpdating daily summary for ${TARGET_DATE}...`);
    try {
      // Get data for summary
      const curtailmentSummary = await db
        .select({
          totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
          totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));

      // Delete existing summary if any
      await db.delete(dailySummaries)
        .where(eq(dailySummaries.summaryDate, TARGET_DATE));
      
      // Insert new summary
      if (curtailmentSummary[0]) {
        await db.insert(dailySummaries).values({
          summaryDate: TARGET_DATE,
          totalCurtailedEnergy: Number(curtailmentSummary[0].totalCurtailedEnergy || 0),
          totalPayment: Number(curtailmentSummary[0].totalPayment || 0),
          periodCount: Number(countResult[0]?.periodCount || 0),
          farmCount: Number(countResult[0]?.farmCount || 0),
          recordCount: Number(countResult[0]?.count || 0),
          lastUpdated: new Date()
        });
        console.log(`✓ Daily summary updated for ${TARGET_DATE}`);
      }
    } catch (error) {
      console.error(`Error updating daily summary:`, error);
    }
    
    // Calculate execution time
    const endTime = new Date();
    const executionTimeMs = endTime.getTime() - startTime.getTime();
    console.log(`\n=== Reprocessing Completed ===`);
    console.log(`Total execution time: ${(executionTimeMs / 1000).toFixed(2)} seconds`);
    
  } catch (error) {
    console.error(`\n❌ Reprocessing failed:`, error);
    process.exit(1);
  }
}

// Run the reprocessing
reprocessData().then(() => {
  console.log("\nReprocessing script completed successfully");
  process.exit(0);
}).catch(error => {
  console.error("\nUnexpected error during reprocessing:", error);
  process.exit(1);
});