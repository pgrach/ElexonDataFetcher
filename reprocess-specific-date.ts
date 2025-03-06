#!/usr/bin/env tsx
/**
 * Fast Reprocessing of Specific Date
 * 
 * This script provides a direct, optimized way to reprocess Bitcoin calculations
 * for a specific date without the overhead of full reconciliation.
 * 
 * Usage:
 *   npx tsx reprocess-specific-date.ts <date>
 * 
 * Example:
 *   npx tsx reprocess-specific-date.ts 2025-03-04
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { eq, and, sql, count } from "drizzle-orm";
import { isValidDateString } from "./server/utils/dates";
import { getDifficultyData } from "./server/services/dynamodbService";
import { calculateBitcoin } from "./server/utils/bitcoin";
import { performance } from "perf_hooks";

// Constants
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const BATCH_SIZE = 100;

// Parse command line argument
const dateArg = process.argv[2];
if (!dateArg || !isValidDateString(dateArg)) {
  console.error("Please provide a valid date in YYYY-MM-DD format");
  console.error("Example: npx tsx reprocess-specific-date.ts 2025-03-04");
  process.exit(1);
}

// Helper function for console output
function log(message: string, type: "info" | "success" | "warning" | "error" = "info") {
  const timestamp = new Date().toISOString();
  let prefix = "";
  
  switch (type) {
    case "success":
      prefix = "\x1b[32m✓\x1b[0m "; // Green checkmark
      break;
    case "warning":
      prefix = "\x1b[33m⚠\x1b[0m "; // Yellow warning
      break;
    case "error":
      prefix = "\x1b[31m✗\x1b[0m "; // Red X
      break;
    default:
      prefix = "\x1b[36m•\x1b[0m "; // Blue dot for info
  }
  
  console.log(`${prefix}[${timestamp.split('T')[1].split('.')[0]}] ${message}`);
}

// Sleep utility
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function main() {
  const startTime = performance.now();
  
  log(`Starting Bitcoin recalculation for ${dateArg}`, "info");
  
  try {
    // Step 1: Get current status
    const initialQuery = await db.select({
      recordCount: count(curtailmentRecords.id),
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, dateArg));
    
    log(`Found ${initialQuery[0]?.recordCount || 0} curtailment records`, "info");
    log(`Across ${initialQuery[0]?.periodCount || 0} settlement periods and ${initialQuery[0]?.farmCount || 0} farms`, "info");
    log(`Total volume: ${Number(initialQuery[0]?.totalVolume || 0).toFixed(2)} MWh`, "info");
    log(`Total payment: £${Number(initialQuery[0]?.totalPayment || 0).toFixed(2)}`, "info");
    
    // Step 2: Get difficulty for the date
    log(`Fetching difficulty for ${dateArg}...`, "info");
    const difficulty = await getDifficultyData(dateArg);
    log(`Using difficulty: ${difficulty}`, "info");
    
    // Step 3: Process each miner model
    for (const minerModel of MINER_MODELS) {
      log(`Processing ${minerModel}...`, "info");
      
      // Step 3.1: Delete existing calculations for this model and date
      const deleteResult = await db.delete(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, dateArg),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
      
      log(`Deleted existing ${minerModel} calculations for ${dateArg}`, "info");
      
      // Step 3.2: Get all curtailment records for this date
      const curtailmentQuery = await db.select({
        settlementDate: curtailmentRecords.settlementDate,
        settlementPeriod: curtailmentRecords.settlementPeriod,
        farmId: curtailmentRecords.farmId,
        volume: curtailmentRecords.volume,
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, dateArg));
      
      log(`Processing ${curtailmentQuery.length} records for ${minerModel}...`, "info");
      
      // Step 3.3: Process in batches to prevent timeouts
      let insertedCount = 0;
      for (let i = 0; i < curtailmentQuery.length; i += BATCH_SIZE) {
        const batch = curtailmentQuery.slice(i, i + BATCH_SIZE);
        
        // Create Bitcoin calculations for each record
        const calculations = batch.map(record => {
          const volumeAbs = Math.abs(parseFloat(record.volume));
          const bitcoinMined = calculateBitcoin(
            volumeAbs,
            minerModel,
            difficulty
          );
          
          return {
            settlementDate: record.settlementDate,
            settlementPeriod: record.settlementPeriod,
            farmId: record.farmId,
            minerModel: minerModel,
            curtailedVolume: record.volume,
            difficulty: difficulty.toString(),
            bitcoinMined: bitcoinMined.toString(),
            calculatedAt: new Date().toISOString(),
          };
        });
        
        // Insert batch of calculations
        await db.insert(historicalBitcoinCalculations).values(calculations);
        
        insertedCount += calculations.length;
        log(`Inserted ${insertedCount}/${curtailmentQuery.length} records for ${minerModel}`, "info");
        
        // Add a small delay between batches
        if (i + BATCH_SIZE < curtailmentQuery.length) {
          await sleep(300); // 300ms pause between batches
        }
      }
      
      log(`Completed processing for ${minerModel}`, "success");
    }
    
    // Step 4: Verify results
    const verificationResults = await Promise.all(
      MINER_MODELS.map(async (model) => {
        const result = await db
          .select({
            calculationCount: count(historicalBitcoinCalculations.id),
            totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`
          })
          .from(historicalBitcoinCalculations)
          .where(
            and(
              eq(historicalBitcoinCalculations.settlementDate, dateArg),
              eq(historicalBitcoinCalculations.minerModel, model)
            )
          );
        
        return {
          model,
          count: result[0]?.calculationCount || 0,
          bitcoin: Number(result[0]?.totalBitcoin || 0).toFixed(8)
        };
      })
    );
    
    // Print verification results
    log("\nVerification Results:", "success");
    verificationResults.forEach(result => {
      log(`${result.model}: ${result.count} records, ${result.bitcoin} BTC mined`, "success");
    });
    
    const endTime = performance.now();
    const duration = ((endTime - startTime) / 1000).toFixed(1);
    
    log(`\nReprocessing completed successfully in ${duration}s`, "success");
    process.exit(0);
  } catch (error) {
    log(`Error during reprocessing: ${error}`, "error");
    process.exit(1);
  }
}

main();