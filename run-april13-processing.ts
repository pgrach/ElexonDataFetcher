/**
 * Run Enhanced Curtailment Processing for 2025-04-13
 * 
 * This script uses the enhanced curtailment processing to ensure all 48 periods
 * are properly fetched and processed from Elexon API.
 */

import { processDailyCurtailment } from "./server/services/curtailment_enhanced";
import { processHistoricalCalculations } from "./server/services/bitcoinService";
import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations, bitcoinDailySummaries } from "./db/schema";
import { eq, and, sql } from "drizzle-orm";

// Target date
const TARGET_DATE = "2025-04-13";
// Miner models
const MINER_MODELS = ["S19J_PRO", "M20S", "S9"];

/**
 * Log a step with a timestamp
 */
function logStep(message: string): Promise<void> {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] ${message}`);
  return Promise.resolve();
}

/**
 * Run the processing for April 13, 2025
 */
async function processData(): Promise<void> {
  await logStep(`Starting enhanced processing for ${TARGET_DATE}`);
  
  // Step 1: Clear existing data
  await logStep("Clearing existing curtailment records...");
  await db.delete(curtailmentRecords).where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  await logStep("Clearing existing Bitcoin calculations...");
  await db.delete(historicalBitcoinCalculations).where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
  
  await logStep("Clearing existing Bitcoin daily summaries...");
  await db.delete(bitcoinDailySummaries).where(eq(bitcoinDailySummaries.summary_date, TARGET_DATE));
  
  // Step 2: Process daily curtailment using the enhanced service
  await logStep("Processing curtailment data with enhanced service...");
  await processDailyCurtailment(TARGET_DATE);
  
  // Step 3: Process Bitcoin calculations for each miner model
  for (const minerModel of MINER_MODELS) {
    await logStep(`Processing Bitcoin calculations for ${minerModel}...`);
    try {
      await processHistoricalCalculations(TARGET_DATE, minerModel);
    } catch (error) {
      await logStep(`ERROR processing Bitcoin calculations for ${minerModel}: ${error}`);
    }
  }
  
  // Step 4: Verify the results
  const curtailmentCount = await db
    .select({ count: sql<number>`count(*)` })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  const btcCalcCount = await db
    .select({ count: sql<number>`count(*)` })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
  
  // Get periods with curtailment
  const periods = await db
    .select({ period: curtailmentRecords.settlementPeriod })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
  
  const periodsWithCurtailment = periods.map(p => p.period);
  
  // Get Bitcoin totals
  const bitcoinTotals = await db
    .select({
      minerModel: historicalBitcoinCalculations.minerModel,
      bitcoinMined: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined})`
    })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE))
    .groupBy(historicalBitcoinCalculations.minerModel);
  
  // Print summary
  await logStep("==== Processing Summary ====");
  await logStep(`Total curtailment records: ${curtailmentCount[0].count}`);
  await logStep(`Periods with curtailment: ${periodsWithCurtailment.join(", ")}`);
  await logStep(`Total Bitcoin calculation records: ${btcCalcCount[0].count}`);
  
  bitcoinTotals.forEach(total => {
    const btc = parseFloat(total.bitcoinMined);
    logStep(`Total Bitcoin mined with ${total.minerModel}: ${btc.toFixed(8)} BTC`);
  });
  
  await logStep("Processing completed");
}

// Run the processing
processData()
  .then(() => {
    console.log("Script execution completed successfully");
    process.exit(0);
  })
  .catch(error => {
    console.error("Fatal error:", error);
    process.exit(1);
  });