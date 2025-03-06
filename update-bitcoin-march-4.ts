/**
 * Update Bitcoin Calculations for March 4, 2025
 * 
 * This script updates the Bitcoin calculations for March 4, 2025 after fixing
 * the duplicate records and adding the missing settlement periods (47 and 48).
 * It ensures that all Bitcoin mining potential calculations are up-to-date.
 */

import { processSingleDay } from './server/services/bitcoinService';
import { db } from "@db";
import { historicalBitcoinCalculations } from "@db/schema";
import { eq, and, inArray } from "drizzle-orm";

const TARGET_DATE = '2025-03-04';
const MINER_MODELS = ['S19J_PRO', 'M20S', 'S9'];

function log(message: string, type: "info" | "success" | "warning" | "error" = "info") {
  const colors = {
    info: "\x1b[36m", // cyan
    success: "\x1b[32m", // green
    warning: "\x1b[33m", // yellow
    error: "\x1b[31m", // red
    reset: "\x1b[0m" // reset
  };
  
  const timestamp = new Date().toISOString();
  console.log(`${colors[type]}[${timestamp}] ${message}${colors.reset}`);
}

async function main() {
  try {
    log("Starting Bitcoin calculation update for March 4, 2025", "info");
    
    // Check current calculation coverage
    const currentCalcs = await db
      .select({
        minerModel: historicalBitcoinCalculations.minerModel,
        count: { value: historicalBitcoinCalculations.farmId, fn: "count" },
        totalBitcoin: { value: historicalBitcoinCalculations.bitcoinMined, fn: "sum" },
        distinctPeriods: { 
          value: historicalBitcoinCalculations.settlementPeriod, 
          fn: "count", 
          distinct: true 
        }
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE))
      .groupBy(historicalBitcoinCalculations.minerModel);
    
    log("Current Bitcoin calculation status:", "info");
    currentCalcs.forEach(calc => {
      log(`Model ${calc.minerModel}: ${calc.count.value} calculations, ${calc.distinctPeriods.value} periods, ${Number(calc.totalBitcoin.value).toFixed(8)} BTC`, "info");
    });
    
    // Process each miner model
    for (const minerModel of MINER_MODELS) {
      log(`Processing ${TARGET_DATE} for miner model ${minerModel}...`, "info");
      await processSingleDay(TARGET_DATE, minerModel);
      log(`Completed processing for ${minerModel}`, "success");
    }
    
    // Check updated calculation coverage
    const updatedCalcs = await db
      .select({
        minerModel: historicalBitcoinCalculations.minerModel,
        count: { value: historicalBitcoinCalculations.farmId, fn: "count" },
        totalBitcoin: { value: historicalBitcoinCalculations.bitcoinMined, fn: "sum" },
        distinctPeriods: { 
          value: historicalBitcoinCalculations.settlementPeriod, 
          fn: "count", 
          distinct: true 
        }
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE))
      .groupBy(historicalBitcoinCalculations.minerModel);
    
    log("Updated Bitcoin calculation status:", "success");
    updatedCalcs.forEach(calc => {
      log(`Model ${calc.minerModel}: ${calc.count.value} calculations, ${calc.distinctPeriods.value} periods, ${Number(calc.totalBitcoin.value).toFixed(8)} BTC`, "success");
    });
    
    // Verify that the newly added periods 47 and 48 have Bitcoin calculations
    const newPeriodCalcs = await db
      .select({
        settlementPeriod: historicalBitcoinCalculations.settlementPeriod,
        count: { value: historicalBitcoinCalculations.farmId, fn: "count" },
        totalBitcoin: { value: historicalBitcoinCalculations.bitcoinMined, fn: "sum" }
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, 'S19J_PRO'),
          inArray(historicalBitcoinCalculations.settlementPeriod, [47, 48])
        )
      )
      .groupBy(historicalBitcoinCalculations.settlementPeriod);
    
    if (newPeriodCalcs.length === 2) {
      log("Successfully verified Bitcoin calculations for periods 47 and 48:", "success");
      newPeriodCalcs.forEach(calc => {
        log(`Period ${calc.settlementPeriod}: ${calc.count.value} farm calculations, ${Number(calc.totalBitcoin.value).toFixed(8)} BTC`, "success");
      });
    } else {
      log(`Warning: Expected 2 periods (47, 48) but found ${newPeriodCalcs.length}`, "warning");
    }
    
    log("Bitcoin calculation update completed successfully", "success");
  } catch (error) {
    log(`Error updating Bitcoin calculations: ${error}`, "error");
    process.exit(1);
  }
}

main().catch(error => {
  log(`Unhandled error: ${error}`, "error");
  process.exit(1);
});