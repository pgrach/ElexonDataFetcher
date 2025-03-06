/**
 * Data Reingestion Script for 2025-03-04
 * 
 * This script handles the reingestion of Elexon data for 2025-03-04,
 * ensures data integrity by preventing duplicates and checking for missed periods,
 * and then triggers cascade updates of dependent tables.
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations, dailySummaries } from "./db/schema";
import { eq, sql, count, and, desc, asc } from "drizzle-orm";
import { processDailyCurtailment } from "./server/services/curtailment";
import { processSingleDay } from "./server/services/bitcoinService";
import fs from "fs";
import path from "path";

// Configuration
const DATE_TO_PROCESS = "2025-03-04";
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];
const LOG_FILE = "./reingest_2025_03_04.log";

// Logging utility
function log(message: string, type: "info" | "success" | "warning" | "error" = "info") {
  const timestamp = new Date().toISOString();
  let prefix = "";
  
  switch (type) {
    case "success":
      prefix = "✓ ";
      break;
    case "warning":
      prefix = "⚠ ";
      break;
    case "error":
      prefix = "✗ ";
      break;
    default:
      prefix = "• ";
  }
  
  const logMessage = `[${timestamp}] ${prefix}${message}`;
  console.log(logMessage);
  
  // Also log to file
  fs.appendFileSync(LOG_FILE, logMessage + "\n");
}

// Check if there are missing periods in the data
async function checkForMissingPeriods(date: string): Promise<number[]> {
  // Get all settlement periods from the database
  const periodsResult = await db
    .select({
      period: curtailmentRecords.settlementPeriod
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date))
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(asc(curtailmentRecords.settlementPeriod));
  
  const periods = periodsResult.map(r => r.period);
  const missingPeriods: number[] = [];
  
  // A day should have 48 settlement periods (1-48)
  for (let i = 1; i <= 48; i++) {
    if (!periods.includes(i)) {
      missingPeriods.push(i);
    }
  }
  
  return missingPeriods;
}

// Clear existing data for the date to avoid duplicates
async function clearExistingData(date: string): Promise<void> {
  log(`Clearing existing data for ${date} to prevent duplicates...`);
  
  // First, get counts for logging purposes
  const curtailmentCount = await db
    .select({ count: count() })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  const bitcoinCount = await db
    .select({ count: count() })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, date));
  
  log(`Found ${curtailmentCount[0].count} curtailment records to clear`);
  log(`Found ${bitcoinCount[0].count} Bitcoin calculation records to clear`);
  
  // Delete existing curtailment records
  const deletedCurtailment = await db
    .delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date))
    .returning({ id: curtailmentRecords.id });
  
  log(`Deleted ${deletedCurtailment.length} curtailment records`, "success");
  
  // Delete existing Bitcoin calculations
  const deletedBitcoin = await db
    .delete(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, date))
    .returning({ id: historicalBitcoinCalculations.id });
  
  log(`Deleted ${deletedBitcoin.length} Bitcoin calculation records`, "success");
  
  // Delete existing daily summary if it exists
  await db
    .delete(dailySummaries)
    .where(eq(dailySummaries.summaryDate, date));
  
  log(`Cleared daily summary for ${date}`, "success");
}

// Main function to run the reingestion
async function main() {
  try {
    // Start the process
    log(`Starting data reingestion for ${DATE_TO_PROCESS}`, "info");
    
    // Clear existing data to avoid duplicates
    await clearExistingData(DATE_TO_PROCESS);
    
    // Reingest curtailment data
    log(`Reingesting curtailment data from Elexon API...`, "info");
    await processDailyCurtailment(DATE_TO_PROCESS);
    
    // Verify data completeness - check for missing periods
    const missingPeriods = await checkForMissingPeriods(DATE_TO_PROCESS);
    if (missingPeriods.length > 0) {
      log(`Warning: Missing periods detected: ${missingPeriods.join(', ')}`, "warning");
      log(`Retrying to fetch missing periods...`, "info");
      
      // Retry fetching curtailment data to fill missing periods
      await processDailyCurtailment(DATE_TO_PROCESS);
      
      // Check again for missing periods
      const stillMissingPeriods = await checkForMissingPeriods(DATE_TO_PROCESS);
      if (stillMissingPeriods.length > 0) {
        log(`Error: Still missing periods after retry: ${stillMissingPeriods.join(', ')}`, "error");
      } else {
        log(`All periods successfully fetched on retry`, "success");
      }
    } else {
      log(`All 48 settlement periods successfully fetched`, "success");
    }
    
    // Get statistics on reingested data
    const dataStats = await db
      .select({
        recordCount: count(curtailmentRecords.id),
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE_TO_PROCESS));
    
    log(`Reingestion results:`, "info");
    log(`- Records: ${dataStats[0]?.recordCount || 0}`);
    log(`- Periods: ${dataStats[0]?.periodCount || 0}`);
    log(`- Farms: ${dataStats[0]?.farmCount || 0}`);
    log(`- Volume: ${Number(dataStats[0]?.totalVolume || 0).toFixed(2)} MWh`);
    log(`- Payment: £${Number(dataStats[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Process Bitcoin calculations
    log(`Processing Bitcoin calculations for dependent tables...`, "info");
    
    for (const minerModel of MINER_MODELS) {
      log(`Processing ${minerModel}...`, "info");
      await processSingleDay(DATE_TO_PROCESS, minerModel);
    }
    
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
            and(
              eq(historicalBitcoinCalculations.settlementDate, DATE_TO_PROCESS),
              eq(historicalBitcoinCalculations.minerModel, model)
            )
          );
        
        return { 
          model, 
          bitcoin: result[0]?.totalBitcoin || "0",
          count: result[0]?.recordCount || 0
        };
      })
    );
    
    log(`Bitcoin calculation results:`, "info");
    for (const stat of bitcoinStats) {
      log(`- ${stat.model}: ${Number(stat.bitcoin).toFixed(8)} BTC (${stat.count} records)`);
    }
    
    // Verify daily summary was generated
    const summary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, DATE_TO_PROCESS)
    });
    
    if (summary) {
      log(`Daily summary generated successfully:`, "success");
      log(`- Total curtailed energy: ${Number(summary.totalCurtailedEnergy).toFixed(2)} MWh`);
      log(`- Total payment: £${Number(summary.totalPayment).toFixed(2)}`);
    } else {
      log(`Warning: Daily summary not found`, "warning");
    }
    
    // Process completed successfully
    log(`Reingestion completed successfully!`, "success");
    
  } catch (error) {
    log(`Error during reingestion: ${error}`, "error");
    process.exit(1);
  }
}

// Start the process
main();