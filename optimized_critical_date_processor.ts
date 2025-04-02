/**
 * Optimized Critical Date Processor
 * 
 * This script provides a streamlined way to reingest and fix Elexon API data for specific
 * settlement periods on any date. It handles multiple records for the same farm within a period
 * correctly and ensures all Bitcoin calculations are updated.
 * 
 * Features:
 * - Bulk clearing and insertion for better handling of duplicate farm records
 * - Efficient batch processing to avoid timeouts
 * - Comprehensive logging and verification
 * - Simple command-line interface
 * - Improved validation and error handling
 * - Safety checks for invalid period numbers
 * 
 * Usage:
 *   npx tsx optimized_critical_date_processor.ts <date> [start_period] [end_period]
 * 
 * Example:
 *   npx tsx optimized_critical_date_processor.ts 2025-03-09 44 48
 * 
 * For processing entire days:
 *   npx tsx optimized_critical_date_processor.ts 2025-03-28
 */

import { db } from './db';
import { and, between, eq } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';

// ES module support for __dirname equivalent
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const BMU_MAPPING_PATH = path.join(__dirname, 'server', 'data', 'bmuMapping.json');
const LOG_FILE = `process_critical_date_${new Date().toISOString().split('T')[0]}.log`;
const MAX_RETRIES = 3;
const RETRY_DELAY = 5000; // 5 seconds
const BATCH_SIZE = 5; // Number of periods to process in a batch

// Parse command line arguments
const [dateArg, startPeriodArg, endPeriodArg] = process.argv.slice(2);
const date = dateArg || new Date().toISOString().split('T')[0];
const startPeriod = startPeriodArg ? parseInt(startPeriodArg, 10) : 1;
const endPeriod = endPeriodArg ? parseInt(endPeriodArg, 10) : 48;

// Logging utility
async function logToFile(message: string): Promise<void> {
  await fs.appendFile(LOG_FILE, `${message}\n`, 'utf8').catch(console.error);
}

function log(message: string, type: "info" | "success" | "warning" | "error" = "info"): void {
  const timestamp = new Date().toLocaleTimeString();
  let coloredMessage: string;
  
  switch (type) {
    case "success":
      coloredMessage = `\x1b[32m✓ [${timestamp}] ${message}\x1b[0m`;
      break;
    case "warning":
      coloredMessage = `\x1b[33m⚠ [${timestamp}] ${message}\x1b[0m`;
      break;
    case "error":
      coloredMessage = `\x1b[31m✖ [${timestamp}] ${message}\x1b[0m`;
      break;
    default:
      coloredMessage = `\x1b[36mℹ [${timestamp}] ${message}\x1b[0m`;
  }
  
  console.log(coloredMessage);
  logToFile(`[${timestamp}] [${type.toUpperCase()}] ${message}`).catch(() => {});
}

// Helper function to delay execution
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mappings once
async function loadBmuMappings(): Promise<{
  windFarmIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}> {
  log(`Loading BMU mapping from: ${BMU_MAPPING_PATH}`, "info");
  
  try {
    const data = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(data);
    
    const windFarmIds = new Set<string>();
    const bmuLeadPartyMap = new Map<string, string>();
    
    for (const bmu of bmuMapping) {
      windFarmIds.add(bmu.elexonBmUnit);
      bmuLeadPartyMap.set(bmu.elexonBmUnit, bmu.leadPartyName);
    }
    
    log(`Found ${windFarmIds.size} wind farm BMUs`, "success");
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    log(`Error loading BMU mapping: ${error}`, "error");
    return { windFarmIds: new Set(), bmuLeadPartyMap: new Map() };
  }
}

// Process a single settlement period
async function processPeriod(
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
  log(`Processing period ${period} (attempt ${attempt})`, "info");
  
  try {
    // Fetch data from the Elexon API - make parallel requests for bids and offers
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get(`${API_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000
      }),
      axios.get(`${API_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000
      })
    ]);

    // Combine both datasets
    const bidsData = bidsResponse.data.data || [];
    const offersData = offersResponse.data.data || [];
    const data = [...bidsData, ...offersData];
    
    console.log(`Loading BMU mapping from: ${BMU_MAPPING_PATH}`);
    console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
    
    // Filter to keep only valid wind farm records
    const validRecords = data.filter((record: any) => {
      return windFarmIds.has(record.id) && record.volume < 0 && record.soFlag; // Negative volume indicates curtailment
    });
    
    const totalVolume = validRecords.reduce((sum: number, record: any) => sum + Math.abs(record.volume), 0);
    const totalPayment = validRecords.reduce((sum: number, record: any) => sum + (Math.abs(record.volume) * record.originalPrice), 0);
    
    console.log(`[${date} P${period}] Records: ${validRecords.length} (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`);
    log(`Period ${period}: Found ${validRecords.length} valid records`, "success");
    
    let recordsAdded = 0;
    let totalVolumeAdded = 0;
    let totalPaymentAdded = 0;
    
    // Get the unique farm IDs for this period
    const uniqueFarmIds = Array.from(new Set(validRecords.map((record: any) => record.id)));
    
    // Clear all existing records for this period - simpler approach that guarantees no duplicates
    try {
      // Just clear all records for this period to avoid any duplicates
      const deleteResult = await db.delete(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, date),
            eq(curtailmentRecords.settlementPeriod, period)
          )
        );
      
      log(`Period ${period}: Cleared existing records before insertion`, "info");
    } catch (error) {
      log(`Period ${period}: Error clearing existing records: ${error}`, "error");
    }
    
    // Prepare all records for bulk insertion
    const recordsToInsert = validRecords.map((record: any) => {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice;
      
      // Track totals for return value
      totalVolumeAdded += volume;
      totalPaymentAdded += payment;
      
      return {
        settlementDate: date,
        settlementPeriod: period,
        farmId: record.id,
        leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
        volume: record.volume.toString(), // Keep negative value
        payment: payment.toString(),
        originalPrice: record.originalPrice.toString(),
        finalPrice: record.finalPrice.toString(),
        soFlag: record.soFlag || false,
        cadlFlag: record.cadlFlag || false
      };
    });
    
    // Insert all records in a single transaction if there are any
    if (recordsToInsert.length > 0) {
      try {
        await db.insert(curtailmentRecords).values(recordsToInsert);
        recordsAdded = recordsToInsert.length;
        
        // Log individual records for visibility
        for (const record of validRecords) {
          const volume = Math.abs(record.volume);
          const payment = volume * record.originalPrice;
          log(`Period ${period}: Added ${record.id} (${volume.toFixed(2)} MWh, £${payment.toFixed(2)})`, "success");
        }
        
      } catch (error) {
        log(`Period ${period}: Error bulk inserting records: ${error}`, "error");
      }
    }
    
    if (recordsAdded > 0) {
      log(`Period ${period} complete: ${recordsAdded} records, ${totalVolumeAdded.toFixed(2)} MWh, £${totalPaymentAdded.toFixed(2)}`, "success");
    }
    
    return { 
      success: true, 
      records: recordsAdded,
      volume: totalVolumeAdded,
      payment: totalPaymentAdded
    };
    
  } catch (error) {
    log(`Error processing period ${period}: ${error}`, "error");
    
    // Retry logic
    if (attempt < MAX_RETRIES) {
      log(`Retrying period ${period} in ${RETRY_DELAY/1000} seconds... (attempt ${attempt + 1}/${MAX_RETRIES})`, "warning");
      await delay(RETRY_DELAY);
      return processPeriod(period, windFarmIds, bmuLeadPartyMap, attempt + 1);
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
    const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
    
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
export async function processDate(
  targetDate: string = date,
  targetStartPeriod: number = startPeriod,
  targetEndPeriod: number = endPeriod
): Promise<{
  success: boolean;
  recordsProcessed: number;
  recordsAdded: number;
  periodsProcessed: number;
}> {
  try {
    // Validate date format (YYYY-MM-DD)
    if (!targetDate.match(/^\d{4}-\d{2}-\d{2}$/)) {
      log(`Invalid date format: ${targetDate}. Expected format: YYYY-MM-DD`, "error");
      return {
        success: false,
        recordsProcessed: 0,
        recordsAdded: 0,
        periodsProcessed: 0
      };
    }
    
    // Use passed parameters instead of global variables
    const currentDate = targetDate;
    
    // Ensure periods are valid numbers, defaulting to 1-48 if not
    let currentStartPeriod = isNaN(targetStartPeriod) ? 1 : targetStartPeriod;
    let currentEndPeriod = isNaN(targetEndPeriod) ? 48 : targetEndPeriod;
    
    // Ensure periods are within valid range
    currentStartPeriod = Math.max(1, Math.min(48, currentStartPeriod));
    currentEndPeriod = Math.max(1, Math.min(48, currentEndPeriod));
    
    log(`Starting critical date processing for ${currentDate}`, "info");
    log(`Target periods: ${currentStartPeriod}-${currentEndPeriod}`, "info");
    
    // Initialize log file
    await fs.writeFile(LOG_FILE, `=== Critical Date Processing: ${currentDate} (Periods ${currentStartPeriod}-${currentEndPeriod}) ===\n`);
    
    // Load BMU mappings once
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // First check if we need to clear existing records
    try {
      log(`Checking for existing records for periods ${currentStartPeriod}-${currentEndPeriod}...`, "info");
        
      // Clear any existing records
      const deleteResult = await db.delete(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, currentDate),
            between(curtailmentRecords.settlementPeriod, currentStartPeriod, currentEndPeriod)
          )
        )
        .returning({ id: curtailmentRecords.id });
          
      if (deleteResult.length > 0) {
        log(`Cleared ${deleteResult.length} existing records for periods ${currentStartPeriod}-${currentEndPeriod}`, "success");
      } else {
        log(`No existing records found for periods ${currentStartPeriod}-${currentEndPeriod}`, "info");
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
    
    for (let batchStart = currentStartPeriod; batchStart <= currentEndPeriod; batchStart += BATCH_SIZE) {
      const batchEnd = Math.min(batchStart + BATCH_SIZE - 1, currentEndPeriod);
      
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
    const totalPeriods = currentEndPeriod - currentStartPeriod + 1;
    const successRate = (totalProcessed / totalPeriods) * 100;
    
    log(`=== Processing complete for ${currentDate} ===`, successRate === 100 ? "success" : "warning");
    log(`- Periods processed: ${totalProcessed}/${totalPeriods} (${successRate.toFixed(1)}%)`, "info");
    log(`- Records added: ${totalRecords}`, "info");
    log(`- Total volume: ${totalVolume.toFixed(2)} MWh`, "info");
    log(`- Total payment: £${totalPayment.toFixed(2)}`, "info");
    
    // Run reconciliation to update Bitcoin calculations
    log(`Running reconciliation to update Bitcoin calculations...`, "info");
    await runReconciliation(currentDate);
    
    // Final status
    log(`=== Complete ===`, "success");
    log(`Data reingestion and reconciliation completed for ${currentDate}`, "success");
    
    // Return result object for use in daily_reconciliation_check
    return {
      success: successRate > 90, // Consider success if >90% of periods were processed
      recordsProcessed: totalRecords,
      recordsAdded: totalRecords,
      periodsProcessed: totalProcessed
    };
    
  } catch (error) {
    log(`Fatal error during processing: ${error}`, "error");
    await logToFile(`Fatal error during processing: ${error}`);
    
    // Return failure result instead of exiting
    return {
      success: false,
      recordsProcessed: 0,
      recordsAdded: 0,
      periodsProcessed: 0
    };
  }
}

// Run reconciliation to update Bitcoin calculations
async function runReconciliation(targetDate: string = date): Promise<void> {
  return new Promise((resolve, reject) => {
    log(`Running reconciliation for ${targetDate}...`, "info");
    
    const reconciliation = spawn('npx', ['tsx', 'update_summaries.ts', targetDate]);
    
    reconciliation.stdout.on('data', (data) => {
      console.log(`${data}`);
    });
    
    reconciliation.stderr.on('data', (data) => {
      console.error(`${data}`);
    });
    
    reconciliation.on('close', (code) => {
      if (code === 0) {
        log(`Reconciliation completed successfully for ${targetDate}`, "success");
        resolve();
      } else {
        log(`Reconciliation failed with code ${code}`, "error");
        resolve(); // Resolve anyway to continue
      }
    });
    
    // Add timeout to prevent hanging
    setTimeout(() => {
      log(`Reconciliation timed out after 60 seconds, continuing anyway`, "warning");
      resolve();
    }, 60000);
  });
}

// Main function
async function main() {  
  await processDate();
  process.exit(0);
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});