/**
 * CLI Tool for Direct Data Reprocessing
 * 
 * This tool bypasses the API and directly uses the server modules
 * to reprocess the data for 2025-04-03.
 */

// Import required modules
import { processDailyCurtailment } from './server/services/curtailment.js';
import { processSingleDay } from './server/services/bitcoinService.js';
import { db } from './db/index.js';
import { curtailmentRecords, dailySummaries } from './db/schema.js';
import { eq, sql } from 'drizzle-orm';

const TARGET_DATE = "2025-04-03";
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

async function reprocessTargetDate() {
  console.log(`\n=== Starting Reprocessing for ${TARGET_DATE} ===`);

  try {
    // Step 1: Process the curtailment data
    console.log(`\nReprocessing curtailment data for ${TARGET_DATE}...`);
    await processDailyCurtailment(TARGET_DATE);
    
    // Step 2: Verify curtailment records were created
    const verifyRecords = await db.select({
      count: sql`COUNT(*)`,
      totalVolume: sql`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      totalPayment: sql`SUM(${curtailmentRecords.payment}::numeric)`,
      periodCount: sql`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nCurtailment Records Verification:`);
    console.log(`Records: ${verifyRecords[0]?.count || 0}`);
    console.log(`Settlement Periods: ${verifyRecords[0]?.periodCount || 0}`);
    console.log(`Total Volume: ${Number(verifyRecords[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Total Payment: £${Number(verifyRecords[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Step 3: Process Bitcoin calculations for each miner model
    console.log(`\nProcessing Bitcoin calculations...`);
    
    for (const minerModel of MINER_MODELS) {
      try {
        console.log(`Processing ${minerModel}...`);
        await processSingleDay(TARGET_DATE, minerModel);
        console.log(`Successfully processed ${minerModel}`);
      } catch (error) {
        console.error(`Error processing ${minerModel}: ${error.message}`);
      }
    }
    
    // Step 4: Verify daily summary was updated
    const dailySummary = await db.select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    console.log(`\nDaily Summary Verification:`);
    if (dailySummary.length > 0) {
      console.log(`Summary Date: ${dailySummary[0].summaryDate}`);
      console.log(`Total Curtailed Energy: ${dailySummary[0].totalCurtailedEnergy} MWh`);
      console.log(`Total Payment: £${dailySummary[0].totalPayment}`);
      console.log(`Last Updated: ${dailySummary[0].lastUpdated}`);
    } else {
      console.log(`No daily summary found for ${TARGET_DATE}`);
    }
    
    console.log(`\n=== Reprocessing Complete ===`);
  } catch (error) {
    console.error(`\nError during reprocessing:`, error);
    process.exit(1);
  }
}

// Run the reprocessing
reprocessTargetDate().catch(error => {
  console.error("Fatal error:", error);
  process.exit(1);
});