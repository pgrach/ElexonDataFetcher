/**
 * Data Reprocessing Script for 2025-04-03
 * 
 * Run with: npx tsx reprocess.ts
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, historicalBitcoinCalculations } from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import { processDailyCurtailment } from "./server/services/curtailment";
import { processSingleDay } from "./server/services/bitcoinService";
import { format } from "date-fns";

const TARGET_DATE = "2025-04-03";
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

async function reprocessData() {
  console.log(`\n=== Starting Reprocessing for ${TARGET_DATE} ===`);
  
  try {
    // Step 1: Delete existing data for the target date
    console.log(`Removing existing curtailment records for ${TARGET_DATE}...`);
    const deleteResult = await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
    console.log(`Removing existing Bitcoin calculations for ${TARGET_DATE}...`);
    await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
    // Step 2: Reprocess curtailment data
    console.log(`\nReprocessing curtailment data for ${TARGET_DATE}...`);
    try {
      await processDailyCurtailment(TARGET_DATE);
      console.log(`Successfully reprocessed curtailment data for ${TARGET_DATE}`);
    } catch (error) {
      console.error(`Error processing curtailment data:`, error);
      throw error;
    }
    
    // Step 3: Verify curtailment records
    const countResult = await db.select({
      count: sql<string>`COUNT(*)`,
      periodCount: sql<string>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nCurtailment Verification:`);
    console.log(`Total Records: ${countResult[0]?.count || 0}`);
    console.log(`Settlement Periods: ${countResult[0]?.periodCount || 0}`);
    console.log(`Total Volume: ${Number(countResult[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Total Payment: £${Number(countResult[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Step 4: Process Bitcoin calculations for each model
    console.log(`\nProcessing Bitcoin calculations for ${TARGET_DATE}...`);
    
    let bitcoinError = false;
    for (const minerModel of MINER_MODELS) {
      try {
        console.log(`Processing ${minerModel}...`);
        await processSingleDay(TARGET_DATE, minerModel);
        console.log(`Successfully processed Bitcoin calculations for ${minerModel}`);
      } catch (error) {
        console.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
        bitcoinError = true;
      }
    }
    
    if (bitcoinError) {
      console.warn(`Some Bitcoin calculations failed but curtailment data was processed successfully.`);
    }
    
    // Step 5: Final verification
    const summary = await db.select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    console.log(`\nDaily Summary:`);
    if (summary.length > 0) {
      console.log(`Date: ${TARGET_DATE}`);
      console.log(`Total Energy: ${summary[0].totalCurtailedEnergy} MWh`);
      console.log(`Total Payment: £${summary[0].totalPayment}`);
      console.log(`Last Updated: ${summary[0].lastUpdated}`);
    } else {
      console.log(`No daily summary found for ${TARGET_DATE}`);
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