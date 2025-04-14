/**
 * Complete Data Reprocessing Script for 2025-04-13
 * 
 * This script performs a thorough reprocessing of all 48 settlement periods for 2025-04-13:
 * 1. Clears existing data from all relevant tables
 * 2. Fetches each of the 48 settlement periods from Elexon API explicitly
 * 3. Updates wind generation data
 * 4. Recalculates Bitcoin mining potential for all miner models
 * 5. Updates all summary tables
 * 6. Performs comprehensive verification
 */

import { db } from "./db";
import { 
  curtailmentRecords, 
  historicalBitcoinCalculations,
  bitcoinDailySummaries
} from "./db/schema";
import { eq, and } from "drizzle-orm";
import { fetchBidsOffers } from "./server/services/elexon";
import { processDailyCurtailment } from "./server/services/curtailment_enhanced";
import { fetchWindGenerationData } from "./server/services/windGenerationService";
import { processHistoricalBitcoinCalculations, updateBitcoinDailySummary } from "./server/services/bitcoinService";
import { sql } from "drizzle-orm";
import fs from "fs";

// Target date for reprocessing
const TARGET_DATE = "2025-04-13";
// Periods with known curtailment to prioritize
const PRIORITY_PERIODS = [10, 11, 33, 34, 35, 48];
// Output log file
const LOG_FILE = `logs/reprocess_april13_complete_${new Date().toISOString().replace(/:/g, '-')}.log`;
// Miner models
const MINER_MODELS = ["S19J_PRO", "M20S", "S9"];

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
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${message}`;
  console.log(logMessage);
  fs.appendFileSync(LOG_FILE, logMessage + "\n");
}

/**
 * Manually fetch and verify all 48 settlement periods from Elexon API
 */
async function manuallyFetchAllPeriods(): Promise<{
  totalRecords: number;
  periodDetails: { period: number; records: number; volume: number; payment: number }[];
}> {
  logStep("Starting manual fetch of all 48 settlement periods from Elexon API...");
  
  const periodDetails: { period: number; records: number; volume: number; payment: number }[] = [];
  let totalRecords = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  
  // Process priority periods first to ensure we get the most important data
  const allPeriods = [...PRIORITY_PERIODS];
  for (let p = 1; p <= 48; p++) {
    if (!allPeriods.includes(p)) allPeriods.push(p);
  }
  
  for (const period of allPeriods) {
    logStep(`Fetching settlement period ${period}...`);
    try {
      const records = await fetchBidsOffers(TARGET_DATE, period);
      
      if (records.length > 0) {
        // Calculate totals for this period
        const volume = records.reduce((sum, r) => sum + Math.abs(r.volume), 0);
        const payment = records.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice), 0);
        
        logStep(`Period ${period}: Found ${records.length} records with total volume ${volume.toFixed(2)} MWh and total payment £${payment.toFixed(2)}`);
        
        // Just count the records - we'll use the enhanced daily processor for the actual ingestion
        totalRecords += records.length;
        totalVolume += volume;
        totalPayment += payment;
        
        periodDetails.push({
          period,
          records: records.length,
          volume,
          payment
        });
      } else {
        logStep(`Period ${period}: No records found`);
      }
      
      // Add slight delay to prevent rate limiting
      if (period < 48) await delay(300);
    } catch (error) {
      logStep(`ERROR fetching period ${period}: ${error}`);
    }
  }
  
  logStep(`Completed manual fetch of all periods. Total: ${totalRecords} records, ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
  
  return {
    totalRecords,
    periodDetails
  };
}

/**
 * Run the reprocessing for the target date
 */
async function reprocessDate(): Promise<void> {
  logStep(`Starting complete reprocessing for ${TARGET_DATE}...`);
  
  // Step 1: Clear existing data
  logStep("Clearing existing records from curtailment_records table...");
  await db.delete(curtailmentRecords).where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  logStep("Clearing existing records from historical_bitcoin_calculations table...");
  await db.delete(historicalBitcoinCalculations).where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
  
  logStep("Clearing existing records from bitcoin_daily_summaries table...");
  await db.delete(bitcoinDailySummaries).where(eq(bitcoinDailySummaries.date, TARGET_DATE));
  
  // Step 2: Fetch Elexon data for all 48 periods
  const fetchResult = await manuallyFetchAllPeriods();
  
  // Step 3: Process the daily curtailment data
  logStep("Processing curtailment data using enhanced processor...");
  await processDailyCurtailment(TARGET_DATE);
  
  // Step 4: Update wind generation data
  logStep("Updating wind generation data...");
  await fetchWindGenerationData(new Date(TARGET_DATE), new Date(TARGET_DATE));
  
  // Step 5: Process Bitcoin calculations for each miner model
  for (const minerModel of MINER_MODELS) {
    logStep(`Processing Bitcoin calculations for ${minerModel}...`);
    try {
      await processHistoricalBitcoinCalculations(TARGET_DATE, minerModel);
    } catch (error) {
      logStep(`ERROR processing Bitcoin calculations for ${minerModel}: ${error}`);
    }
  }
  
  // Step 6: Recalculate Bitcoin daily summary
  logStep("Recalculating Bitcoin daily summary...");
  await updateBitcoinDailySummary(TARGET_DATE);
  
  // Step 6: Verify data for each period with curtailment
  logStep("Performing verification...");
  for (const periodDetail of fetchResult.periodDetails) {
    const period = periodDetail.period;
    
    // Verify curtailment records
    const curtailmentCount = await db
      .select({ count: sql<number>`count(*)` })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.settlementPeriod, period)
        )
      );
    
    const btcCalcCount = await db
      .select({ count: sql<number>`count(*)` })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.settlementPeriod, period)
        )
      );
    
    logStep(`Verification - Period ${period}: ${curtailmentCount[0].count} curtailment records, ${btcCalcCount[0].count} Bitcoin calculation records`);
  }
  
  // Step 7: Final summary
  const finalCurtailmentCount = await db
    .select({ count: sql<number>`count(*)` })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  const finalBtcCalcCount = await db
    .select({ count: sql<number>`count(*)` })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
  
  const dailySummary = await db
    .select()
    .from(bitcoinDailySummaries)
    .where(eq(bitcoinDailySummaries.date, TARGET_DATE));
  
  logStep(`Reprocessing complete!`);
  logStep(`Total curtailment records: ${finalCurtailmentCount[0].count}`);
  logStep(`Total Bitcoin calculation records: ${finalBtcCalcCount[0].count}`);
  if (dailySummary.length > 0) {
    for (const summary of dailySummary) {
      logStep(`Bitcoin summary for ${summary.minerModel}: ${summary.bitcoinMined} BTC (${summary.valueAtMiningTime} GBP)`);
    }
  }
}

// Run the reprocessing
reprocessDate()
  .then(() => {
    logStep("Script execution completed");
    process.exit(0);
  })
  .catch(error => {
    logStep(`FATAL ERROR: ${error}`);
    process.exit(1);
  });