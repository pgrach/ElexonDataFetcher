#!/usr/bin/env tsx
/**
 * Critical Date Processing Tool
 * 
 * This script is designed to process specific problematic dates with enhanced
 * error handling, retry logic, and very small batch sizes to avoid timeouts.
 * 
 * Usage:
 *   npx tsx process_critical_date.ts <date> [start_period] [end_period]
 * 
 * Example:
 *   npx tsx process_critical_date.ts 2025-03-09
 *   npx tsx process_critical_date.ts 2025-03-09 44 48
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, and, between, sql } from "drizzle-orm";
import { fetchBidsOffers, delay } from "./server/services/elexon";
import * as fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

// Get directory info
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Constants
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");
const LOG_FILE = "process_critical_date.log";
const MAX_RETRIES = 3;
const BATCH_SIZE = 5; // Very small batch size to avoid timeouts
const RETRY_DELAY = 5000; // 5 seconds

// Parse command line arguments
const args = process.argv.slice(2);
const date = args[0];
const startPeriodArg = args[1] ? parseInt(args[1]) : undefined;
const endPeriodArg = args[2] ? parseInt(args[2]) : undefined;

if (!date) {
  console.error("Usage: npx tsx process_critical_date.ts <date> [start_period] [end_period]");
  process.exit(1);
}

// Logging
async function logToFile(message: string): Promise<void> {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${message}\n`;
  
  try {
    await fs.appendFile(LOG_FILE, logMessage);
  } catch (error) {
    console.error(`Error writing to log file: ${error}`);
  }
}

function log(message: string, type: "info" | "success" | "warning" | "error" = "info"): void {
  const timestamp = new Date().toISOString().split('T')[1].split('.')[0];
  let prefix = "";
  let color = "";
  
  switch (type) {
    case "success":
      prefix = "✓";
      color = "\x1b[32m"; // Green
      break;
    case "warning":
      prefix = "⚠";
      color = "\x1b[33m"; // Yellow
      break;
    case "error":
      prefix = "✗";
      color = "\x1b[31m"; // Red
      break;
    default:
      prefix = "ℹ";
      color = "\x1b[36m"; // Cyan
  }
  
  const consoleMessage = `${color}${prefix} [${timestamp}] ${message}\x1b[0m`;
  console.log(consoleMessage);
  logToFile(`${prefix} ${message}`).catch(() => {});
}

// Load wind farm BMU mappings
async function loadWindFarmIds(): Promise<{
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
}> {
  try {
    log(`Loading BMU mapping from: ${BMU_MAPPING_PATH}`);
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    
    const windFarmIds = new Set<string>(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => bmu.elexonBmUnit as string)
    );
    
    const bmuLeadPartyMap = new Map<string, string>(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => [bmu.elexonBmUnit as string, bmu.leadPartyName as string || 'Unknown'])
    );
    
    log(`Found ${windFarmIds.size} wind farm BMUs`, "success");
    
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    log(`Error loading BMU mapping: ${error}`, "error");
    throw error;
  }
}

// Process a single period with retries
async function processPeriodWithRetries(
  period: number,
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>,
  attempt: number = 1
): Promise<{
  success: boolean;
  records: number;
  volume: number;
  payment: number;
}> {
  try {
    log(`Processing period ${period} (attempt ${attempt})`);
    
    // Fetch data from Elexon API
    const records = await fetchBidsOffers(date, period);
    
    // Filter for valid curtailment records
    const validRecords = records.filter(record =>
      record.volume < 0 &&
      (record.soFlag || record.cadlFlag) &&
      windFarmIds.has(record.id)
    );
    
    if (validRecords.length > 0) {
      log(`Period ${period}: Found ${validRecords.length} valid records`, "success");
    } else {
      log(`Period ${period}: No valid curtailment records found`, "warning");
    }
    
    let recordsAdded = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process each record
    for (const record of validRecords) {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice;
      
      try {
        // Clear any existing records first to avoid conflicts
        await db.delete(curtailmentRecords)
          .where(
            and(
              eq(curtailmentRecords.settlementDate, date),
              eq(curtailmentRecords.settlementPeriod, period),
              eq(curtailmentRecords.farmId, record.id)
            )
          );
        
        // Insert the record
        await db.insert(curtailmentRecords).values({
          settlementDate: date,
          settlementPeriod: period,
          farmId: record.id,
          leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
          volume: record.volume.toString(), // Keep negative value
          payment: payment.toString(),
          originalPrice: record.originalPrice.toString(),
          finalPrice: record.finalPrice.toString(),
          soFlag: record.soFlag,
          cadlFlag: record.cadlFlag
        });
        
        recordsAdded++;
        totalVolume += volume;
        totalPayment += payment;
        
        log(`Period ${period}: Added ${record.id} (${volume.toFixed(2)} MWh, £${payment.toFixed(2)})`, "success");
      } catch (error) {
        log(`Period ${period}: Error inserting record for ${record.id}: ${error}`, "error");
      }
    }
    
    if (recordsAdded > 0) {
      log(`Period ${period} complete: ${recordsAdded} records, ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`, "success");
    }
    
    return { 
      success: true, 
      records: recordsAdded,
      volume: totalVolume,
      payment: totalPayment
    };
    
  } catch (error) {
    log(`Error processing period ${period}: ${error}`, "error");
    
    // Retry logic
    if (attempt < MAX_RETRIES) {
      log(`Retrying period ${period} in ${RETRY_DELAY/1000} seconds... (attempt ${attempt + 1}/${MAX_RETRIES})`, "warning");
      await delay(RETRY_DELAY);
      return processPeriodWithRetries(period, windFarmIds, bmuLeadPartyMap, attempt + 1);
    }
    
    return { 
      success: false, 
      records: 0,
      volume: 0,
      payment: 0
    };
  }
}

// Process periods in small batches
async function processBatch(
  startPeriod: number,
  endPeriod: number,
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  success: boolean;
  processed: number;
  total: number;
  records: number;
  volume: number;
  payment: number;
}> {
  log(`Processing batch: periods ${startPeriod}-${endPeriod}`);
  
  let totalRecords = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  let periodsProcessed = 0;
  let totalPeriods = endPeriod - startPeriod + 1;
  let success = true;
  
  for (let period = startPeriod; period <= endPeriod; period++) {
    const result = await processPeriodWithRetries(period, windFarmIds, bmuLeadPartyMap);
    
    if (result.success) {
      periodsProcessed++;
      totalRecords += result.records;
      totalVolume += result.volume;
      totalPayment += result.payment;
    } else {
      success = false;
      log(`Failed to process period ${period} after ${MAX_RETRIES} attempts`, "error");
    }
    
    // Add a delay between periods to avoid rate limiting
    await delay(1500);
  }
  
  log(`Batch ${startPeriod}-${endPeriod} complete:`, success ? "success" : "warning");
  log(`- Periods processed: ${periodsProcessed}/${totalPeriods}`);
  log(`- Records added: ${totalRecords}`);
  log(`- Total volume: ${totalVolume.toFixed(2)} MWh`);
  log(`- Total payment: £${totalPayment.toFixed(2)}`);
  
  return {
    success,
    processed: periodsProcessed,
    total: totalPeriods,
    records: totalRecords,
    volume: totalVolume,
    payment: totalPayment
  };
}

// Process a specific date with small batches
async function processDate(
  startPeriod: number = 1,
  endPeriod: number = 48
): Promise<void> {
  try {
    log(`Starting critical date processing for ${date}`, "info");
    log(`Target periods: ${startPeriod}-${endPeriod}`, "info");
    
    // Initialize log file
    await fs.writeFile(LOG_FILE, `=== Critical Date Processing: ${date} (Periods ${startPeriod}-${endPeriod}) ===\n`);
    
    // Load BMU mappings once
    const { windFarmIds, bmuLeadPartyMap } = await loadWindFarmIds();
    
    // First check if we need to clear existing records - simpler approach
    try {
      log(`Checking for existing records for periods ${startPeriod}-${endPeriod}...`, "info");
        
      // Clear any existing records
      const deleteResult = await db.delete(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, date),
            between(curtailmentRecords.settlementPeriod, startPeriod, endPeriod)
          )
        )
        .returning({ id: curtailmentRecords.id });
          
      if (deleteResult.length > 0) {
        log(`Cleared ${deleteResult.length} existing records for periods ${startPeriod}-${endPeriod}`, "success");
      } else {
        log(`No existing records found for periods ${startPeriod}-${endPeriod}`, "info");
      }
    } catch (error) {
      log(`Error clearing existing records: ${error}`, "error");
      // Continue with processing anyway
    }
    
    // Process periods in small batches to avoid timeouts
    let totalProcessed = 0;
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (let batchStart = startPeriod; batchStart <= endPeriod; batchStart += BATCH_SIZE) {
      const batchEnd = Math.min(batchStart + BATCH_SIZE - 1, endPeriod);
      
      const batchResult = await processBatch(
        batchStart,
        batchEnd,
        windFarmIds,
        bmuLeadPartyMap
      );
      
      totalProcessed += batchResult.processed;
      totalRecords += batchResult.records;
      totalVolume += batchResult.volume;
      totalPayment += batchResult.payment;
      
      // Add a longer delay between batches
      log(`Pausing between batches...`, "info");
      await delay(3000);
    }
    
    // Final summary
    const totalPeriods = endPeriod - startPeriod + 1;
    const successRate = (totalProcessed / totalPeriods) * 100;
    
    log(`=== Processing complete for ${date} ===`, successRate === 100 ? "success" : "warning");
    log(`- Periods processed: ${totalProcessed}/${totalPeriods} (${successRate.toFixed(1)}%)`, "info");
    log(`- Records added: ${totalRecords}`, "info");
    log(`- Total volume: ${totalVolume.toFixed(2)} MWh`, "info");
    log(`- Total payment: £${totalPayment.toFixed(2)}`, "info");
    
    await logToFile(`=== Processing complete for ${date} ===`);
    await logToFile(`- Periods processed: ${totalProcessed}/${totalPeriods} (${successRate.toFixed(1)}%)`);
    await logToFile(`- Records added: ${totalRecords}`);
    await logToFile(`- Total volume: ${totalVolume.toFixed(2)} MWh`);
    await logToFile(`- Total payment: £${totalPayment.toFixed(2)}`);
    
  } catch (error) {
    log(`Fatal error during processing: ${error}`, "error");
    await logToFile(`Fatal error during processing: ${error}`);
    process.exit(1);
  }
}

// Main function
async function main() {
  const startPeriod = startPeriodArg || 1;
  const endPeriod = endPeriodArg || 48;
  
  await processDate(startPeriod, endPeriod);
  process.exit(0);
}

// Run the script
main();