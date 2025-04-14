/**
 * Run Enhanced Curtailment Processing for 2025-04-13
 * 
 * This script uses the enhanced curtailment processing to ensure all 48 periods
 * are properly fetched and processed from Elexon API.
 */

import { db } from "./db";
import { 
  curtailmentRecords, 
  historicalBitcoinCalculations,
  windGenerationData,
  dailySummaries
} from "./db/schema";
import { eq, sql } from "drizzle-orm";
import { processDailyCurtailment } from "./server/services/curtailment_enhanced";
import { processSingleDay } from "./server/services/bitcoinService";
import { processWindDataForDate } from "./server/services/windDataUpdater";

const TARGET_DATE = "2025-04-13";
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

/**
 * Log a step with a timestamp
 */
function logStep(message: string): Promise<void> {
  console.log(`[${new Date().toISOString()}] ${message}`);
  return Promise.resolve();
}

/**
 * Run the processing for April 13, 2025
 */
async function processData(): Promise<void> {
  try {
    await logStep(`Starting enhanced processing for ${TARGET_DATE}`);
    
    // Clear existing records
    await logStep("Clearing existing curtailment records");
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    await logStep("Clearing existing Bitcoin calculations");
    await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
    await logStep("Clearing existing daily summary");
    await db.delete(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    // Process wind data (will fetch all periods)
    await logStep("Processing wind generation data");
    await processWindDataForDate(TARGET_DATE, true);
    
    // Process curtailment data (will fetch all 48 periods)
    await logStep("Processing curtailment data (all 48 periods)");
    await processDailyCurtailment(TARGET_DATE);
    
    // Process Bitcoin calculations
    await logStep("Processing Bitcoin calculations for all miner models");
    for (const minerModel of MINER_MODELS) {
      await logStep(`Processing Bitcoin calculations for ${minerModel}`);
      await processSingleDay(TARGET_DATE, minerModel);
    }
    
    // Verify the results
    const curtailmentCount = await db.select({ 
      count: sql<number>`count(*)`,
      periods: sql<number>`count(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      volume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      payment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const bitcoinCount = await db.select({ 
      count: sql<number>`count(*)`,
      models: sql<number>`count(DISTINCT ${historicalBitcoinCalculations.minerModel})`,
      bitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
    })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
    // Print verification results
    await logStep("Processing completed. Verification results:");
    console.log("\n=== Database Records ===");
    console.log("Curtailment records:", curtailmentCount[0]?.count || 0);
    console.log("Distinct periods:", curtailmentCount[0]?.periods || 0);
    console.log("Total volume:", Number(curtailmentCount[0]?.volume || 0).toFixed(2), "MWh");
    console.log("Total payment: £", Number(curtailmentCount[0]?.payment || 0).toFixed(2));
    
    console.log("\nBitcoin calculation records:", bitcoinCount[0]?.count || 0);
    console.log("Distinct miner models:", bitcoinCount[0]?.models || 0);
    console.log("Total Bitcoin mined:", Number(bitcoinCount[0]?.bitcoin || 0).toFixed(8), "BTC");
    
    await logStep("Processing completed successfully");
  } catch (error) {
    console.error("Error during processing:", error);
    process.exit(1);
  }
}

// Execute the processing
processData().then(() => {
  console.log("Process completed, exiting");
  process.exit(0);
}).catch(err => {
  console.error("Unhandled error:", err);
  process.exit(1);
});