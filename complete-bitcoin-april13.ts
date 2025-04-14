/**
 * Complete Bitcoin Calculations for 2025-04-13
 * 
 * This script focuses specifically on processing Bitcoin calculations for April 13, 2025
 * after the curtailment data and wind generation data have already been processed.
 */

import { db } from "./db";
import { historicalBitcoinCalculations } from "./db/schema";
import { eq, sql } from "drizzle-orm";
import { processSingleDay } from "./server/services/bitcoinService";
import { calculateMonthlyBitcoinSummary, manualUpdateYearlyBitcoinSummary } from "./server/services/bitcoinService";

const TARGET_DATE = "2025-04-13";
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

/**
 * Log a step with a timestamp
 */
function logStep(message: string): void {
  console.log(`[${new Date().toISOString()}] ${message}`);
}

/**
 * Process Bitcoin calculations for the target date
 */
async function processBitcoinCalculations(): Promise<void> {
  try {
    // Start the processing
    logStep(`Starting Bitcoin calculations for ${TARGET_DATE}`);
    
    // Clear existing historical Bitcoin calculations if any exist
    logStep("Clearing any existing Bitcoin calculations");
    await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    logStep("Bitcoin calculations cleared");
    
    // Process calculations for each miner model
    for (const minerModel of MINER_MODELS) {
      logStep(`Processing Bitcoin calculations for ${TARGET_DATE} with model ${minerModel}`);
      try {
        await processSingleDay(TARGET_DATE, minerModel);
        logStep(`Successfully processed calculations for ${minerModel}`);
      } catch (error) {
        console.error(`Error processing Bitcoin calculations for ${TARGET_DATE} with ${minerModel}:`, error);
      }
    }
    
    // Update monthly summary
    logStep("Updating monthly Bitcoin summary");
    const yearMonth = TARGET_DATE.substring(0, 7); // "YYYY-MM" format
    const year = TARGET_DATE.substring(0, 4); // "YYYY" format
    
    // Update monthly summaries for all miner models
    for (const minerModel of MINER_MODELS) {
      logStep(`Calculating monthly Bitcoin summary for ${yearMonth} with ${minerModel}`);
      try {
        await calculateMonthlyBitcoinSummary(yearMonth, minerModel);
      } catch (error) {
        console.error(`Error updating monthly Bitcoin summary for ${yearMonth} with ${minerModel}:`, error);
      }
    }
    
    // Update yearly summary
    logStep(`Updating yearly Bitcoin summary for ${year}`);
    try {
      await manualUpdateYearlyBitcoinSummary(year);
    } catch (error) {
      console.error(`Error updating yearly Bitcoin summary for ${year}:`, error);
    }
    
    logStep("Monthly and yearly summaries updated");
    
    // Final verification
    const bitcoinCalcCount = await db.select({ count: sql`count(*)` })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
    logStep("Bitcoin calculations completed");
    console.log("Bitcoin calculation records:", bitcoinCalcCount[0]?.count || 0);
    
  } catch (error) {
    console.error("Error during Bitcoin calculations:", error);
    process.exit(1);
  }
}

// Execute the processing
processBitcoinCalculations().then(() => {
  console.log("Process completed, exiting");
  process.exit(0);
}).catch(err => {
  console.error("Unhandled error:", err);
  process.exit(1);
});