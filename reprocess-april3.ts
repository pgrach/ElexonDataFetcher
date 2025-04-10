/**
 * Data Reprocessing Script for 2025-04-03
 * 
 * This script reprocesses all 48 settlement periods for 2025-04-03 by:
 * 1. Clearing existing data from curtailment_records for the date
 * 2. Fetching fresh data from Elexon API for all periods
 * 3. Updating all dependent tables (daily/monthly/yearly summaries)
 * 4. Recalculating all Bitcoin mining potential
 */

import { db } from "./db";
import { 
  curtailmentRecords, 
  historicalBitcoinCalculations 
} from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import { format } from "date-fns";
import { processDailyCurtailment } from "./server/services/curtailment";
import { processSingleDay } from "./server/services/bitcoinService";
import { delay } from "./server/services/elexon";

const TARGET_DATE = "2025-04-03";
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

async function reprocessDate() {
  console.log(`\n=== Starting Reprocessing for ${TARGET_DATE} ===`);
  
  try {
    // Step 1: Delete existing curtailment records for the date
    console.log(`Deleting existing curtailment records for ${TARGET_DATE}...`);
    const deleteResult = await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .returning({ id: curtailmentRecords.id });
    
    console.log(`Deleted ${deleteResult.length} existing curtailment records`);
    
    // Step 2: Delete existing Bitcoin calculations for the date
    console.log(`Deleting existing Bitcoin calculations for ${TARGET_DATE}...`);
    const deleteBitcoinResult = await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE))
      .returning({ id: historicalBitcoinCalculations.id });
    
    console.log(`Deleted ${deleteBitcoinResult.length} existing Bitcoin calculations`);
    
    // Step 3: Process daily curtailment data (fetches from Elexon and updates summaries)
    console.log(`\nProcessing curtailment data from Elexon for ${TARGET_DATE}...`);
    await processDailyCurtailment(TARGET_DATE);
    
    // Step 4: Process Bitcoin calculations for each miner model
    console.log(`\nProcessing Bitcoin calculations for ${TARGET_DATE}...`);
    try {
      for (const minerModel of MINER_MODELS) {
        console.log(`Processing calculations for ${minerModel}...`);
        try {
          await processSingleDay(TARGET_DATE, minerModel);
        } catch (modelError) {
          console.error(`Error processing Bitcoin calculations for ${minerModel}:`, modelError.message);
        }
        // Add a small delay between miner models to prevent potential rate limits
        await delay(500);
      }
    } catch (bitcoinError) {
      console.error(`Error during Bitcoin calculations: ${bitcoinError.message}`);
    }
    
    // Step 5: Verify the results
    const verificationResults = await db
      .select({
        recordCount: sql`COUNT(*)`,
        periodCount: sql`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        totalVolume: sql`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql`SUM(${curtailmentRecords.payment}::numeric)`,
        bitcoinCount: sql`COUNT(${historicalBitcoinCalculations.id})`
      })
      .from(curtailmentRecords)
      .leftJoin(
        historicalBitcoinCalculations,
        and(
          eq(historicalBitcoinCalculations.settlementDate, curtailmentRecords.settlementDate),
          eq(historicalBitcoinCalculations.farmId, curtailmentRecords.farmId)
        )
      )
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\n=== Reprocessing Complete ===`);
    console.log(`Date: ${TARGET_DATE}`);
    console.log(`Records processed: ${verificationResults[0]?.recordCount || 0}`);
    console.log(`Settlement periods: ${verificationResults[0]?.periodCount || 0}`);
    console.log(`Total volume: ${Number(verificationResults[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Total payment: £${Number(verificationResults[0]?.totalPayment || 0).toFixed(2)}`);
    console.log(`Bitcoin calculations: ${verificationResults[0]?.bitcoinCount || 0}`);
    console.log(`Completed at: ${format(new Date(), "yyyy-MM-dd HH:mm:ss")}`);
    
  } catch (error) {
    console.error("Error during reprocessing:", error);
    process.exit(1);
  }
}

// Execute the reprocessing
reprocessDate().catch(error => {
  console.error("Fatal error:", error);
  process.exit(1);
});