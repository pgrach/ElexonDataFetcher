/**
 * Complete Bitcoin Calculations for 2025-04-13
 * 
 * This script focuses specifically on processing Bitcoin calculations for April 13, 2025
 * after the curtailment data and wind generation data have already been processed.
 */

import { db } from "./db";
import { 
  curtailmentRecords, 
  historicalBitcoinCalculations,
  bitcoinDailySummaries
} from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import { processHistoricalCalculations } from "./server/services/bitcoinService";
import fs from "fs";

// Target date for reprocessing
const TARGET_DATE = "2025-04-13";
// Miner models
const MINER_MODELS = ["S19J_PRO", "M20S", "S9"];
// Output log file
const LOG_FILE = `logs/complete_bitcoin_april13_${new Date().toISOString().replace(/:/g, '-')}.log`;

/**
 * Log a step with a timestamp
 */
function logStep(message: string): void {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${message}`;
  console.log(logMessage);
  fs.appendFileSync(LOG_FILE, logMessage + "\n");
}

/**
 * Process Bitcoin calculations for the target date
 */
async function processBitcoinCalculations(): Promise<void> {
  logStep(`Starting Bitcoin calculations for ${TARGET_DATE}...`);
  
  // Step 1: Clear existing Bitcoin-related records
  logStep("Clearing existing Bitcoin calculations...");
  await db.delete(historicalBitcoinCalculations).where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
  
  logStep("Clearing existing Bitcoin daily summaries...");
  await db.delete(bitcoinDailySummaries).where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE));
  
  // Step 2: Get information about curtailment records
  const periods = await db
    .select({ period: curtailmentRecords.settlementPeriod })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
  
  const periodsWithCurtailment = periods.map(p => p.period);
  
  // For the Edinburgh wind farm in periods 33-35
  const edinburghRecords = await db
    .select({
      period: curtailmentRecords.settlementPeriod,
      farmId: curtailmentRecords.farmId,
      volume: curtailmentRecords.volume
    })
    .from(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, TARGET_DATE),
        sql`${curtailmentRecords.farmId} LIKE 'T_EDINW%'`
      )
    );
  
  if (edinburghRecords.length > 0) {
    logStep(`Found ${edinburghRecords.length} Edinburgh wind farm records in periods ${edinburghRecords.map(r => r.period).join(', ')}`);
  }
  
  // Step 3: Process Bitcoin calculations for each miner model
  for (const minerModel of MINER_MODELS) {
    logStep(`Processing Bitcoin calculations for ${minerModel}...`);
    try {
      await processHistoricalCalculations(TARGET_DATE, TARGET_DATE, minerModel);
    } catch (error) {
      logStep(`ERROR processing Bitcoin calculations for ${minerModel}: ${error}`);
    }
  }
  
  // Step 4: Verify the results
  const btcCalcCount = await db
    .select({ count: sql<number>`count(*)` })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
  
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
  logStep("==== Processing Summary ====");
  logStep(`Periods with curtailment: ${periodsWithCurtailment.join(", ")}`);
  logStep(`Total Bitcoin calculation records: ${btcCalcCount[0].count}`);
  
  let totalBitcoin = 0;
  bitcoinTotals.forEach(total => {
    const btc = parseFloat(total.bitcoinMined);
    totalBitcoin += btc;
    logStep(`Total Bitcoin mined with ${total.minerModel}: ${btc.toFixed(8)} BTC`);
  });
  
  logStep(`Total Bitcoin mined across all models: ${totalBitcoin.toFixed(8)} BTC`);
  logStep("Processing completed");
}

// Run the processing
processBitcoinCalculations()
  .then(() => {
    logStep("Script execution completed successfully");
    process.exit(0);
  })
  .catch(error => {
    logStep(`FATAL ERROR: ${error}`);
    process.exit(1);
  });