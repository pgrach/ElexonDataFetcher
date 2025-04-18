/**
 * Bitcoin Calculation Reprocessing Script for 2025-04-16
 * 
 * This script specifically reprocesses Bitcoin calculations for 2025-04-16
 * for all miner models. It ensures that all 48 settlement periods are properly
 * calculated based on the curtailment data.
 * 
 * Run with: npx tsx reprocess-april16-bitcoin.ts
 */

import { db } from "./db";
import { historicalBitcoinCalculations, bitcoinDailySummaries } from "./db/schema";
import { eq, and } from "drizzle-orm";
import { processSingleDay } from "./server/services/bitcoinService";
import { minerModels } from "./server/types/bitcoin";

const TARGET_DATE = "2025-04-16";
const MINER_MODEL_KEYS = Object.keys(minerModels);

/**
 * Reprocess Bitcoin calculations
 */
async function reprocessBitcoin() {
  console.log(`\n=== Starting Bitcoin Calculation Reprocessing for ${TARGET_DATE} ===\n`);
  const startTime = new Date();
  
  try {
    // Step 1: Delete existing Bitcoin calculations for the target date
    console.log(`Removing existing Bitcoin calculations for ${TARGET_DATE}...`);
    await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
    console.log(`Removing existing Bitcoin daily summaries for ${TARGET_DATE}...`);
    await db.delete(bitcoinDailySummaries)
      .where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE));
    
    // Step 2: Process Bitcoin calculations for each miner model
    console.log(`\nProcessing Bitcoin calculations for ${TARGET_DATE}...`);
    for (const minerModel of MINER_MODEL_KEYS) {
      console.log(`\nProcessing calculations for ${minerModel}...`);
      try {
        const result = await processSingleDay(TARGET_DATE, minerModel);
        if (result && result.success) {
          console.log(`✓ Successfully processed ${minerModel}: ${result.bitcoinMined.toFixed(8)} BTC (£${result.valueGbp.toFixed(2)})`);
        } else {
          console.log(`No calculations generated for ${minerModel}`);
        }
      } catch (error) {
        console.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
      }
    }
    
    // Calculate execution time
    const endTime = new Date();
    const executionTimeMs = endTime.getTime() - startTime.getTime();
    console.log(`\n=== Reprocessing Completed ===`);
    console.log(`Total execution time: ${(executionTimeMs / 1000).toFixed(2)} seconds`);
    
  } catch (error) {
    console.error(`\n❌ Reprocessing failed:`, error);
    process.exit(1);
  }
}

// Run the reprocessing
reprocessBitcoin().then(() => {
  console.log("\nBitcoin calculation reprocessing completed successfully");
  process.exit(0);
}).catch(error => {
  console.error("\nUnexpected error during reprocessing:", error);
  process.exit(1);
});