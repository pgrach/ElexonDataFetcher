#!/usr/bin/env tsx
/**
 * Script to reingest Elexon API data for 2025-03-11
 * 
 * This script uses the existing data pipeline to:
 * 1. Reingest all curtailment data from Elexon API for 2025-03-11
 * 2. Ensure there are no duplicates by clearing existing data first
 * 3. Update Bitcoin calculations for all miner models
 * 4. Verify the results with comprehensive statistics
 */

import { processDailyCurtailment } from "./server/services/curtailment";
import { processSingleDay } from "./server/services/bitcoinService";
import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { eq, sql, count } from "drizzle-orm";
import { performance } from "perf_hooks";

// Configuration
const DATE = "2025-03-11";
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// Helper function for logging
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

async function main() {
  const startTime = performance.now();
  log(`Starting data reingestion for ${DATE}`, "info");
  
  try {
    // Step 1: Get initial state for comparison
    log("Checking initial state...", "info");
    const initialState = await db
      .select({
        recordCount: count(curtailmentRecords.id),
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE));
    
    log(`Initial record count: ${initialState[0]?.recordCount || 0}`, "info");
    log(`Initial volume: ${Number(initialState[0]?.totalVolume || 0).toFixed(2)} MWh`, "info");
    log(`Initial payment: £${Number(initialState[0]?.totalPayment || 0).toFixed(2)}`, "info");
    
    // Step 2: Process curtailment data
    log("Reingesting curtailment data from Elexon API...", "info");
    await processDailyCurtailment(DATE);
    log("Curtailment data reingestion completed", "success");
    
    // Step 3: Process Bitcoin calculations
    log("Updating Bitcoin calculations...", "info");
    
    for (const minerModel of MINER_MODELS) {
      log(`Processing ${minerModel}...`, "info");
      await processSingleDay(DATE, minerModel);
    }
    
    log("Bitcoin calculations completed", "success");
    
    // Step 4: Verify results
    const verificationStats = await db
      .select({
        recordCount: count(curtailmentRecords.id),
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE));
    
    // Get Bitcoin calculation stats
    const bitcoinStats = await Promise.all(
      MINER_MODELS.map(async (model) => {
        const result = await db
          .select({
            totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`,
            recordCount: count()
          })
          .from(historicalBitcoinCalculations)
          .where(
            eq(historicalBitcoinCalculations.settlementDate, DATE) &&
            eq(historicalBitcoinCalculations.minerModel, model)
          );
        
        return { 
          model, 
          bitcoin: result[0]?.totalBitcoin || "0",
          count: result[0]?.recordCount || 0
        };
      })
    );
    
    const endTime = performance.now();
    const duration = ((endTime - startTime) / 1000).toFixed(1);
    
    log(`Reingestion completed in ${duration}s`, "success");
    
    // Print results
    console.log("\n=== Results ===");
    console.log(`Date: ${DATE}`);
    console.log(`Records: ${verificationStats[0]?.recordCount || 0}`);
    console.log(`Periods: ${verificationStats[0]?.periodCount || 0} / 48`);
    console.log(`Farms: ${verificationStats[0]?.farmCount || 0}`);
    console.log(`Volume: ${Number(verificationStats[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Payment: £${Number(verificationStats[0]?.totalPayment || 0).toFixed(2)}`);
    
    console.log("\n=== Bitcoin Calculations ===");
    for (const stat of bitcoinStats) {
      console.log(`${stat.model}: ${Number(stat.bitcoin).toFixed(8)} BTC (${stat.count} records)`);
    }
    
    const missingPeriods = 48 - (verificationStats[0]?.periodCount || 0);
    if (missingPeriods > 0) {
      log(`Warning: Missing data for ${missingPeriods} periods`, "warning");
    } else {
      log("All 48 settlement periods have data", "success");
    }
    
    process.exit(0);
  } catch (error) {
    log(`Error during data reingestion: ${error}`, "error");
    process.exit(1);
  }
}

// Run the script
main();