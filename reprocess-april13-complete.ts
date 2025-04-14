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
  windGenerationData,
  dailySummaries
} from "./db/schema";
import { eq, and, sql, desc } from "drizzle-orm";
import { processDailyCurtailment } from "./server/services/curtailment_enhanced";
import { processSingleDay } from "./server/services/bitcoinService";
import { processWindDataForDate } from "./server/services/windDataUpdater";
import { fetchBidsOffers } from "./server/services/elexon";
import { calculateMonthlyBitcoinSummary, manualUpdateYearlyBitcoinSummary } from "./server/services/bitcoinService";

const TARGET_DATE = "2025-04-13";
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

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
 * Manually fetch and verify all 48 settlement periods from Elexon API
 */
async function manuallyFetchAllPeriods(): Promise<{
  periodsWithData: number[],
  totalRecords: number,
  totalVolume: number,
  totalPayment: number
}> {
  logStep("Manually fetching all 48 settlement periods from Elexon API");
  
  const periodsWithData: number[] = [];
  let totalRecords = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  
  // Process all 48 periods sequentially to avoid rate limiting
  for (let period = 1; period <= 48; period++) {
    logStep(`Fetching period ${period} from Elexon API`);
    
    try {
      const records = await fetchBidsOffers(TARGET_DATE, period);
      
      // Only count periods with curtailment
      if (records.length > 0) {
        periodsWithData.push(period);
        totalRecords += records.length;
        
        // Calculate period totals
        for (const record of records) {
          totalVolume += Math.abs(record.volume);
          totalPayment += Math.abs(record.volume) * record.originalPrice;
        }
        
        logStep(`Period ${period}: ${records.length} records, Volume: ${Math.abs(records.reduce((sum, r) => sum + r.volume, 0)).toFixed(2)} MWh`);
      } else {
        logStep(`Period ${period}: No curtailment records`);
      }
      
      // Add delay between API calls to avoid rate limiting
      if (period < 48) {
        await delay(300);
      }
    } catch (error) {
      console.error(`Error fetching period ${period}:`, error);
    }
  }
  
  logStep(`Completed manual fetch of all 48 periods`);
  logStep(`Found ${periodsWithData.length} periods with data, ${totalRecords} total records`);
  logStep(`Total volume: ${totalVolume.toFixed(2)} MWh, Total payment: £${totalPayment.toFixed(2)}`);
  
  return {
    periodsWithData,
    totalRecords,
    totalVolume,
    totalPayment
  };
}

/**
 * Run the reprocessing for the target date
 */
async function reprocessDate(): Promise<void> {
  try {
    // Start the reprocessing
    logStep(`Starting complete reprocessing for ${TARGET_DATE}`);
    
    // Step 1: Manual check of all 48 periods from Elexon API
    const apiData = await manuallyFetchAllPeriods();
    
    // Step 2: Clear existing curtailment records
    logStep("Clearing existing curtailment records");
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    logStep("Curtailment records cleared");
    
    // Step 3: Clear existing wind generation data
    logStep("Clearing existing wind generation data");
    await db.delete(windGenerationData)
      .where(eq(windGenerationData.settlementDate, TARGET_DATE));
    logStep("Wind generation data cleared");

    // Step 4: Clear existing historical Bitcoin calculations
    logStep("Clearing existing historical Bitcoin calculations");
    await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    logStep("Historical Bitcoin calculations cleared");
    
    // Step 5: Clear existing daily summary
    logStep("Clearing existing daily summary");
    await db.delete(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    logStep("Daily summary cleared");
    
    // Step 6: Process wind data - fetch and insert new records from Elexon
    logStep("Processing wind generation data");
    await processWindDataForDate(TARGET_DATE, true);
    logStep("Wind generation data processed");
    
    // Allow some breathing room between API calls
    await delay(2000);
    
    // Step 7: Process curtailment data - this will fetch and insert records for all 48 periods
    logStep("Processing curtailment data for all 48 periods");
    await processDailyCurtailment(TARGET_DATE);
    logStep("Curtailment data processed");
    
    // Allow some breathing room between operations
    await delay(2000);
    
    // Step 8: Process Bitcoin calculations for all miner models
    logStep("Processing Bitcoin calculations for all miner models");
    for (const minerModel of MINER_MODELS) {
      logStep(`Processing Bitcoin calculations for ${TARGET_DATE} with model ${minerModel}`);
      try {
        await processSingleDay(TARGET_DATE, minerModel);
        logStep(`Successfully processed calculations for ${minerModel}`);
      } catch (error) {
        console.error(`Error processing Bitcoin calculations for ${TARGET_DATE} with ${minerModel}:`, error);
      }
      // Add delay between models
      await delay(1000);
    }
    logStep("Bitcoin calculations processed");
    
    // Step 9: Update monthly and yearly summaries
    logStep("Updating monthly and yearly summaries");
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
    
    // Step 10: Comprehensive verification
    // Get counts of curtailment records
    const curtailmentCount = await db.select({ 
      count: sql<number>`count(*)`,
      periods: sql<number>`count(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      volume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      payment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Get counts of wind generation data
    const windDataCount = await db.select({ 
      count: sql<number>`count(*)`,
      totalWind: sql<string>`SUM(${windGenerationData.totalWind})`
    })
    .from(windGenerationData)
    .where(eq(windGenerationData.settlementDate, TARGET_DATE));
    
    // Get counts of Bitcoin calculations
    const bitcoinCalcCount = await db.select({ 
      count: sql<number>`count(*)`,
      models: sql<number>`count(DISTINCT ${historicalBitcoinCalculations.minerModel})`,
      bitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
    })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
    // Verify daily summary
    const dailySummary = await db.select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    // Print verification results
    logStep("Verification complete");
    console.log("\n=== Elexon API Data ===");
    console.log(`Periods with data: ${apiData.periodsWithData.length}`);
    console.log(`Total records: ${apiData.totalRecords}`);
    console.log(`Total volume: ${apiData.totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${apiData.totalPayment.toFixed(2)}`);
    
    console.log("\n=== Database Records ===");
    console.log("Curtailment records:", curtailmentCount[0]?.count || 0);
    console.log("Distinct periods:", curtailmentCount[0]?.periods || 0);
    console.log("Total volume:", Number(curtailmentCount[0]?.volume || 0).toFixed(2), "MWh");
    console.log("Total payment: £", Number(curtailmentCount[0]?.payment || 0).toFixed(2));
    
    console.log("\nWind generation records:", windDataCount[0]?.count || 0);
    console.log("Total wind generation:", Number(windDataCount[0]?.totalWind || 0).toFixed(2), "MW");
    
    console.log("\nBitcoin calculation records:", bitcoinCalcCount[0]?.count || 0);
    console.log("Distinct miner models:", bitcoinCalcCount[0]?.models || 0);
    console.log("Total Bitcoin mined:", Number(bitcoinCalcCount[0]?.bitcoin || 0).toFixed(8), "BTC");
    
    console.log("\nDaily summary:");
    console.log(dailySummary[0] ? {
      date: dailySummary[0].summaryDate,
      energy: Number(dailySummary[0].totalCurtailedEnergy).toFixed(2),
      payment: Number(dailySummary[0].totalPayment).toFixed(2),
      windGeneration: Number(dailySummary[0].totalWindGeneration).toFixed(2)
    } : "No daily summary found");
    
    // Check that the API data roughly matches database data
    // Note: There will be some differences due to filtering in the processDailyCurtailment function
    const volumeDiff = Math.abs(apiData.totalVolume - Number(curtailmentCount[0]?.volume || 0));
    const paymentDiff = Math.abs(apiData.totalPayment - Math.abs(Number(curtailmentCount[0]?.payment || 0)));
    
    console.log("\n=== Verification Results ===");
    if (volumeDiff > 5 || paymentDiff > 50) {
      console.log("WARNING: Significant differences between API data and database records!");
      console.log(`Volume difference: ${volumeDiff.toFixed(2)} MWh`);
      console.log(`Payment difference: £${paymentDiff.toFixed(2)}`);
    } else {
      console.log("Verification passed: API data matches database records within acceptable margins");
    }
    
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