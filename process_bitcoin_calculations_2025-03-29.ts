/**
 * Process Bitcoin Calculations for 2025-03-29
 * 
 * This script processes Bitcoin calculations for the newly ingested March 29, 2025 data.
 * It will calculate Bitcoin mining potential for all three standard miner models
 * (S19J_PRO, S9, M20S) for the specified date and update monthly/yearly summaries.
 * 
 * Usage:
 *   npx tsx process_bitcoin_calculations_2025-03-29.ts
 */

import { processSingleDay } from "./server/services/bitcoinService";
import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq } from "drizzle-orm";
import { format } from "date-fns";
import fs from "fs";
import path from "path";

// Constants
const TARGET_DATE = "2025-03-29";
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];
const LOG_FILE = `process_bitcoin_calculations_${TARGET_DATE}.log`;

// Create log directory if it doesn't exist
const logDir = path.join(process.cwd(), "logs");
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir, { recursive: true });
}

// Logging utility
async function log(message: string, level: "info" | "error" | "warning" | "success" = "info"): Promise<void> {
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] [${level.toUpperCase()}] ${message}`;
  
  // Log to console with color
  const colors = {
    info: "\x1b[36m", // Cyan
    error: "\x1b[31m", // Red
    warning: "\x1b[33m", // Yellow
    success: "\x1b[32m" // Green
  };
  console.log(`${colors[level]}${formattedMessage}\x1b[0m`);
  
  // Log to file
  const logPath = path.join(logDir, LOG_FILE);
  fs.appendFileSync(logPath, formattedMessage + "\n");
}

// Delay utility
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Process the Bitcoin calculations for the target date
async function processTargetDate(): Promise<void> {
  try {
    await log(`Starting Bitcoin calculations for ${TARGET_DATE}`, "info");
    
    // First, verify we have curtailment records for this date
    const { sql } = await import("drizzle-orm");
    const recordCount = await db
      .select({ 
        count: sql<string>`COUNT(*)` 
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const count = Number(recordCount[0]?.count || "0");
    
    if (count === 0) {
      await log(`No curtailment records found for ${TARGET_DATE}. Cannot proceed with Bitcoin calculations.`, "error");
      return;
    }
    
    await log(`Found ${count} curtailment records for ${TARGET_DATE}. Proceeding with Bitcoin calculations.`, "success");
    
    // Process each miner model
    for (const minerModel of MINER_MODELS) {
      try {
        await log(`Processing ${TARGET_DATE} with ${minerModel}...`, "info");
        await processSingleDay(TARGET_DATE, minerModel);
        await log(`Successfully processed ${TARGET_DATE} with ${minerModel}`, "success");
      } catch (error) {
        await log(`Error processing ${TARGET_DATE} with ${minerModel}: ${error}`, "error");
      }
      
      // Add a small delay between processing different miner models
      await delay(2000);
    }
    
    // Now verify the calculations were successful
    await log(`Verifying Bitcoin calculations for ${TARGET_DATE}...`, "info");
    
    // Run SQL to check the calculations
    try {
      const { sql } = await import("drizzle-orm");
      const { historicalBitcoinCalculations } = await import("./db/schema");
      
      const verificationResults = await db
        .select({
          minerModel: historicalBitcoinCalculations.minerModel,
          recordCount: sql<number>`COUNT(*)`,
          periodCount: sql<number>`COUNT(DISTINCT ${historicalBitcoinCalculations.settlementPeriod})`,
          farmCount: sql<number>`COUNT(DISTINCT ${historicalBitcoinCalculations.farmId})`,
          totalBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
        })
        .from(historicalBitcoinCalculations)
        .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE))
        .groupBy(historicalBitcoinCalculations.minerModel);
      
      if (verificationResults.length === 0) {
        await log(`No Bitcoin calculation results found for ${TARGET_DATE} after processing.`, "error");
      } else {
        await log(`Bitcoin calculation results for ${TARGET_DATE}:`, "success");
        for (const result of verificationResults) {
          await log(`- ${result.minerModel}: ${result.recordCount} records across ${result.periodCount} periods and ${result.farmCount} farms, total: ${Number(result.totalBitcoin).toFixed(8)} BTC`, "success");
        }
      }
    } catch (error) {
      await log(`Error verifying calculations: ${error}`, "error");
    }
    
    // Run the reconciliation to update the monthly summary
    try {
      await log(`Updating monthly Bitcoin summary for 2025-03...`, "info");
      
      // We'll execute this as a shell command since the function might not be directly importable
      const { exec } = require("child_process");
      
      await new Promise<void>((resolve, reject) => {
        exec("npx tsx server/services/bitcoinService.ts --update-monthly 2025-03", (error: Error | null, stdout: string, stderr: string) => {
          if (error) {
            log(`Error updating monthly summary: ${error}`, "error");
            log(stderr, "error");
            reject(error);
            return;
          }
          
          log(stdout, "info");
          resolve();
        });
      });
      
      await log(`Monthly Bitcoin summary updated`, "success");
    } catch (error) {
      await log(`Error updating monthly summary: ${error}`, "error");
    }
    
    await log(`Bitcoin calculations for ${TARGET_DATE} completed successfully`, "success");
  } catch (error) {
    await log(`Unhandled error during processing: ${error}`, "error");
  }
}

// Run the script
processTargetDate()
  .then(() => {
    console.log(`\nProcess completed. See ${LOG_FILE} for details.`);
    process.exit(0);
  })
  .catch(error => {
    console.error(`\nUnhandled error: ${error}`);
    process.exit(1);
  });