#!/usr/bin/env tsx
/**
 * Fast Reprocessing for a Single Miner Model
 * 
 * This script provides a highly targeted way to reprocess Bitcoin calculations
 * for a specific date and single miner model.
 * 
 * Usage:
 *   npx tsx reprocess-single-model.ts <date> <minerModel>
 * 
 * Example:
 *   npx tsx reprocess-single-model.ts 2025-03-04 S19J_PRO
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { eq, and, sql, count } from "drizzle-orm";
import { isValidDateString } from "./server/utils/dates";
import { getDifficultyData } from "./server/services/dynamodbService";
import { calculateBitcoin } from "./server/utils/bitcoin";
import { performance } from "perf_hooks";

// Constants
const VALID_MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const BATCH_SIZE = 50; // Smaller batch size to prevent timeouts

// Parse command line arguments
const dateArg = process.argv[2];
const minerModelArg = process.argv[3]?.toUpperCase();

if (!dateArg || !isValidDateString(dateArg)) {
  console.error("Please provide a valid date in YYYY-MM-DD format");
  console.error("Example: npx tsx reprocess-single-model.ts 2025-03-04 S19J_PRO");
  process.exit(1);
}

if (!minerModelArg || !VALID_MINER_MODELS.includes(minerModelArg)) {
  console.error(`Please provide a valid miner model: ${VALID_MINER_MODELS.join(', ')}`);
  console.error("Example: npx tsx reprocess-single-model.ts 2025-03-04 S19J_PRO");
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
  
  log(`Starting Bitcoin recalculation for ${dateArg} with ${minerModelArg}`, "info");
  
  try {
    // Step 1: Get current status of curtailment records
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
    
    // Step 2: Get difficulty for the date
    log(`Fetching difficulty for ${dateArg}...`, "info");
    const difficulty = await getDifficultyData(dateArg);
    log(`Using difficulty: ${difficulty}`, "info");
    
    // Step 3: Delete existing calculations for this model and date
    log(`Deleting existing ${minerModelArg} calculations for ${dateArg}...`, "info");
    await db.delete(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, dateArg),
          eq(historicalBitcoinCalculations.minerModel, minerModelArg)
        )
      );
    
    log(`Deleted existing ${minerModelArg} calculations for ${dateArg}`, "success");
    
    // Step 4: Process by settlement period for better performance
    const periods = await db.select({
      period: curtailmentRecords.settlementPeriod
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, dateArg))
    .groupBy(curtailmentRecords.settlementPeriod);
    
    const uniquePeriods = periods.map(p => p.period).sort((a, b) => parseInt(a) - parseInt(b));
    log(`Found ${uniquePeriods.length} periods to process`, "info");
    
    // Step 5: Process each period
    let totalInsertedCount = 0;
    for (const period of uniquePeriods) {
      log(`Processing period ${period}...`, "info");
      
      // Get curtailment records for this period
      const curtailmentQuery = await db.select({
        settlementDate: curtailmentRecords.settlementDate,
        settlementPeriod: curtailmentRecords.settlementPeriod,
        farmId: curtailmentRecords.farmId,
        volume: curtailmentRecords.volume,
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, dateArg),
          eq(curtailmentRecords.settlementPeriod, period)
        )
      );
      
      log(`Processing ${curtailmentQuery.length} records for period ${period}...`, "info");
      
      // Process in smaller batches
      for (let i = 0; i < curtailmentQuery.length; i += BATCH_SIZE) {
        const batch = curtailmentQuery.slice(i, i + BATCH_SIZE);
        
        // Create Bitcoin calculations for each record
        const calculations = batch.map(record => {
          const volumeAbs = Math.abs(parseFloat(record.volume));
          const bitcoinMined = calculateBitcoin(
            volumeAbs,
            minerModelArg,
            difficulty
          );
          
          return {
            settlementDate: record.settlementDate,
            settlementPeriod: record.settlementPeriod,
            farmId: record.farmId,
            minerModel: minerModelArg,
            curtailedVolume: record.volume,
            difficulty: difficulty.toString(),
            bitcoinMined: bitcoinMined.toString(),
            calculatedAt: new Date().toISOString(),
          };
        });
        
        // Insert batch of calculations
        await db.insert(historicalBitcoinCalculations).values(calculations);
        
        totalInsertedCount += calculations.length;
        log(`Inserted ${totalInsertedCount} calculations so far`, "info");
        
        // Add a small delay between batches
        if (i + BATCH_SIZE < curtailmentQuery.length) {
          await sleep(200); // 200ms pause between batches
        }
      }
      
      log(`Completed processing for period ${period}`, "success");
      
      // Add a pause between periods
      if (period !== uniquePeriods[uniquePeriods.length - 1]) {
        await sleep(500); // 500ms pause between periods
      }
    }
    
    // Step 6: Verify results
    const verificationResult = await db
      .select({
        calculationCount: count(historicalBitcoinCalculations.id),
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, dateArg),
          eq(historicalBitcoinCalculations.minerModel, minerModelArg)
        )
      );
    
    // Print verification results
    log("\nVerification Results:", "success");
    log(`${minerModelArg}: ${verificationResult[0]?.calculationCount || 0} records, ${Number(verificationResult[0]?.totalBitcoin || 0).toFixed(8)} BTC mined`, "success");
    
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