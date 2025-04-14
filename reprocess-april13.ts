/**
 * Data Reprocessing Script for 2025-04-13
 * 
 * This script reprocesses all 48 settlement periods for 2025-04-13 by:
 * 1. Clearing existing data from curtailment_records for the date
 * 2. Fetching fresh data from Elexon API for all periods
 * 3. Updating wind generation data records
 * 4. Updating all dependent tables (daily/monthly/yearly summaries)
 * 5. Recalculating all Bitcoin mining potential
 */

import { db } from "./db";
import { 
  curtailmentRecords, 
  historicalBitcoinCalculations,
  windGenerationData
} from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import { processDailyCurtailment } from "./server/services/curtailment_enhanced";
import { processSingleDay } from "./server/services/bitcoinService";
import { processWindDataForDate } from "./server/services/windDataUpdater";
import { reconcileDay } from "./server/services/historicalReconciliation";

const TARGET_DATE = "2025-04-13";

/**
 * Sleep utility function
 */
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Log a step with a timestamp
 */
function logStep(message: string): void {
  console.log(`[${new Date().toISOString()}] ${message}`);
}

/**
 * Run the reprocessing for the target date
 */
async function reprocessDate(): Promise<void> {
  try {
    // Start the reprocessing
    logStep(`Starting reprocessing for ${TARGET_DATE}`);
    
    // Step 1: Clear existing curtailment records
    logStep("Clearing existing curtailment records");
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    logStep("Curtailment records cleared");
    
    // Step 2: Clear existing wind generation data
    logStep("Clearing existing wind generation data");
    await db.delete(windGenerationData)
      .where(eq(windGenerationData.settlementDate, TARGET_DATE));
    logStep("Wind generation data cleared");

    // Step 3: Clear existing historical Bitcoin calculations
    logStep("Clearing existing historical Bitcoin calculations");
    await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    logStep("Historical Bitcoin calculations cleared");
    
    // Step 4: Process wind data - fetch and insert new records from Elexon
    logStep("Processing wind generation data");
    await processWindDataForDate(TARGET_DATE, true);
    logStep("Wind generation data processed");
    
    // Allow some breathing room between API calls
    await delay(2000);
    
    // Step 5: Process curtailment data - fetch and insert new records from Elexon
    logStep("Processing curtailment data");
    await processDailyCurtailment(TARGET_DATE);
    logStep("Curtailment data processed");
    
    // Allow some breathing room between operations
    await delay(2000);
    
    // Step 6: Reconcile the day's data and update all summaries
    logStep("Reconciling day and updating summaries");
    await reconcileDay(TARGET_DATE);
    logStep("Day reconciled successfully");
    
    // Step 7: Process Bitcoin calculations for the day
    logStep("Processing Bitcoin calculations");
    await processSingleDay(TARGET_DATE);
    logStep("Bitcoin calculations processed");
    
    // Final verification
    const curtailmentCount = await db.select({ count: sql<number>`count(*)` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const windDataCount = await db.select({ count: sql<number>`count(*)` })
      .from(windGenerationData)
      .where(eq(windGenerationData.settlementDate, TARGET_DATE));
    
    const bitcoinCalcCount = await db.select({ count: sql<number>`count(*)` })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
    logStep("Verification complete");
    console.log("Curtailment records:", curtailmentCount[0]?.count || 0);
    console.log("Wind generation records:", windDataCount[0]?.count || 0);
    console.log("Bitcoin calculation records:", bitcoinCalcCount[0]?.count || 0);
    
    logStep("Reprocessing completed successfully");
  } catch (error) {
    console.error("Error during reprocessing:", error);
    process.exit(1);
  }
}

// Execute the reprocessing
reprocessDate().then(() => {
  console.log("Process completed, exiting");
  process.exit(0);
}).catch(err => {
  console.error("Unhandled error:", err);
  process.exit(1);
});