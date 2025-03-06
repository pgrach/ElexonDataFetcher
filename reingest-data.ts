#!/usr/bin/env tsx
/**
 * Data Reingestion Tool
 * 
 * This script provides a standardized way to reingest Elexon data for a specific date,
 * update all curtailment records, and trigger cascading updates to Bitcoin calculations.
 * 
 * Usage:
 *   npx tsx reingest-data.ts <date> [options]
 * 
 * Arguments:
 *   date              The date to reingest in YYYY-MM-DD format
 * 
 * Options:
 *   --skip-bitcoin    Skip Bitcoin calculation updates
 *   --skip-verify     Skip verification step
 *   --verbose         Show detailed logs during processing
 *   --help            Show this help message
 * 
 * Examples:
 *   npx tsx reingest-data.ts 2025-03-04
 *   npx tsx reingest-data.ts 2025-03-05 --verbose
 *   npx tsx reingest-data.ts 2025-03-06 --skip-bitcoin
 */

import { processDailyCurtailment } from "./server/services/curtailment";
import { processSingleDay } from "./server/services/bitcoinService";
import { db } from "./db";
import { curtailmentRecords, dailySummaries, historicalBitcoinCalculations } from "./db/schema";
import { eq, sql, count } from "drizzle-orm";
import { isValidDateString } from "./server/utils/dates";
import { performance } from "perf_hooks";

// Default configuration
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// Command line argument parsing
const args = process.argv.slice(2);
const options = {
  date: "",
  skipBitcoin: false,
  skipVerify: false,
  verbose: false,
  showHelp: false
};

// Parse arguments
for (let i = 0; i < args.length; i++) {
  const arg = args[i];
  if (arg === "--skip-bitcoin") {
    options.skipBitcoin = true;
  } else if (arg === "--skip-verify") {
    options.skipVerify = true;
  } else if (arg === "--verbose") {
    options.verbose = true;
  } else if (arg === "--help" || arg === "-h") {
    options.showHelp = true;
  } else if (!options.date && isValidDateString(arg)) {
    options.date = arg;
  }
}

// Show help if requested or missing required date
if (options.showHelp || !options.date) {
  const scriptName = process.argv[1].split("/").pop();
  console.log(`
Data Reingestion Tool

Usage:
  npx tsx ${scriptName} <date> [options]

Arguments:
  date              The date to reingest in YYYY-MM-DD format

Options:
  --skip-bitcoin    Skip Bitcoin calculation updates
  --skip-verify     Skip verification step
  --verbose         Show detailed logs during processing
  --help            Show this help message

Examples:
  npx tsx ${scriptName} 2025-03-04
  npx tsx ${scriptName} 2025-03-05 --verbose
  npx tsx ${scriptName} 2025-03-06 --skip-bitcoin
  `);
  process.exit(options.showHelp ? 0 : 1);
}

// Verify date format
if (!isValidDateString(options.date)) {
  console.error(`Error: Invalid date format. Please use YYYY-MM-DD format.`);
  process.exit(1);
}

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

// Helper function for printing results
function printResults(stats: any) {
  console.log("\n=== Results ===");
  console.log(`Date: ${options.date}`);
  console.log(`Records: ${stats.recordCount || 0}`);
  console.log(`Periods: ${stats.periodCount || 0}`);
  console.log(`Farms: ${stats.farmCount || 0}`);
  console.log(`Volume: ${Number(stats.totalVolume || 0).toFixed(2)} MWh`);
  console.log(`Payment: £${Number(stats.totalPayment || 0).toFixed(2)}`);
  
  if (!options.skipBitcoin) {
    console.log(`Bitcoin mined (S19J_PRO): ${Number(stats.bitcoinS19 || 0).toFixed(8)}`);
    console.log(`Bitcoin mined (S9): ${Number(stats.bitcoinS9 || 0).toFixed(8)}`);
    console.log(`Bitcoin mined (M20S): ${Number(stats.bitcoinM20S || 0).toFixed(8)}`);
  }
}

// Main function
async function main() {
  const startTime = performance.now();
  log(`Starting data reingestion for ${options.date}`, "info");
  
  try {
    // Step 1: Get initial state for comparison
    if (!options.skipVerify) {
      log("Checking initial state...", "info");
      const initialState = await db
        .select({
          recordCount: count(curtailmentRecords.id),
          totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
          totalPayment: sql<string>`SUM(payment::numeric)`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, options.date));
      
      if (options.verbose) {
        log(`Initial record count: ${initialState[0]?.recordCount || 0}`, "info");
        log(`Initial volume: ${Number(initialState[0]?.totalVolume || 0).toFixed(2)} MWh`, "info");
        log(`Initial payment: £${Number(initialState[0]?.totalPayment || 0).toFixed(2)}`, "info");
      }
    }
    
    // Step 2: Process curtailment data
    log("Reingesting curtailment data from Elexon API...", "info");
    await processDailyCurtailment(options.date);
    log("Curtailment data reingestion completed", "success");
    
    // Step 3: Process Bitcoin calculations
    if (!options.skipBitcoin) {
      log("Updating Bitcoin calculations...", "info");
      
      for (const minerModel of MINER_MODELS) {
        if (options.verbose) {
          log(`Processing ${minerModel}...`, "info");
        }
        await processSingleDay(options.date, minerModel);
      }
      
      log("Bitcoin calculations completed", "success");
    }
    
    // Step 4: Verify results
    const stats = await db
      .select({
        recordCount: count(curtailmentRecords.id),
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, options.date));
    
    // Get Bitcoin calculation stats
    if (!options.skipBitcoin) {
      const bitcoinStats = await Promise.all(
        MINER_MODELS.map(async (model) => {
          const result = await db
            .select({
              totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`
            })
            .from(historicalBitcoinCalculations)
            .where(
              eq(historicalBitcoinCalculations.settlementDate, options.date) &&
              eq(historicalBitcoinCalculations.minerModel, model)
            );
          
          return { model, bitcoin: result[0]?.totalBitcoin || "0" };
        })
      );
      
      stats[0].bitcoinS19 = bitcoinStats.find(s => s.model === 'S19J_PRO')?.bitcoin || "0";
      stats[0].bitcoinS9 = bitcoinStats.find(s => s.model === 'S9')?.bitcoin || "0";
      stats[0].bitcoinM20S = bitcoinStats.find(s => s.model === 'M20S')?.bitcoin || "0";
    }
    
    // Get daily summary
    const summary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, options.date)
    });
    
    const endTime = performance.now();
    const duration = ((endTime - startTime) / 1000).toFixed(1);
    
    log(`Reingestion completed in ${duration}s`, "success");
    printResults(stats[0]);
    
    process.exit(0);
  } catch (error) {
    log(`Error during data reingestion: ${error}`, "error");
    process.exit(1);
  }
}

// Run the script
main();