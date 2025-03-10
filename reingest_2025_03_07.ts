/**
 * Script to reingest Elexon API data for 2025-03-07
 * Based on optimized_critical_date_processor.ts
 * 
 * This script focuses on fetching missing data for 2025-03-07, particularly periods 19-41 and 44-48.
 */

import { db } from './db';
import { and, between, eq, count } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { spawn } from 'child_process';

// Get current directory (ESM replacement for __dirname)
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const BMU_MAPPING_PATH = path.join(__dirname, 'server', 'data', 'bmuMapping.json');
const LOG_FILE = `reingest_2025_03_07_${new Date().toISOString().split('T')[0]}.log`;
const MAX_RETRIES = 3;
const RETRY_DELAY = 5000; // 5 seconds
const BATCH_SIZE = 5; // Number of periods to process in a batch

// Fixed date for this script
const date = '2025-03-07';

// Define the ranges of missing periods
const missingRanges = [
  { start: 19, end: 41 },
  { start: 44, end: 48 }
];

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
      windFarmIds.add(bmu.id);
      bmuLeadPartyMap.set(bmu.id, bmu.leadPartyName);
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
    // Fetch data from the Elexon API
    const response = await axios.get(`${API_BASE_URL}/balancing/bid-offer/accepted/settlement-period/${period}/settlement-date/${date}`);
    const data = response.data.data || [];
    
    // Filter to keep only valid wind farm records
    const validRecords = data.filter((record: any) => {
      return windFarmIds.has(record.id) && record.volume < 0; // Negative volume indicates curtailment
    });
    
    const totalVolume = validRecords.reduce((sum: number, record: any) => sum + Math.abs(record.volume), 0);
    const totalPayment = validRecords.reduce((sum: number, record: any) => sum + (Math.abs(record.volume) * record.originalPrice), 0);
    
    log(`[${date} P${period}] Records: ${validRecords.length} (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`);
    
    let recordsAdded = 0;
    let totalVolumeAdded = 0;
    let totalPaymentAdded = 0;
    
    // Clear all existing records for this period - simpler approach that guarantees no duplicates
    try {
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
        soFlag: record.soFlag,
        cadlFlag: record.cadlFlag
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
          log(`[${date} P${period}] Added record for ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`, "success");
        }
        
      } catch (error) {
        log(`Period ${period}: Error bulk inserting records: ${error}`, "error");
      }
    }
    
    if (recordsAdded > 0) {
      log(`Period ${period} complete: ${recordsAdded} records, ${totalVolumeAdded.toFixed(2)} MWh, £${totalPaymentAdded.toFixed(2)}`, "success");
    } else {
      log(`Period ${period}: No valid records found to insert`, "warning");
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

// Run reconciliation to update Bitcoin calculations
async function runReconciliation(): Promise<void> {
  return new Promise((resolve, reject) => {
    log(`Running reconciliation for ${date}...`, "info");
    
    const reconciliation = spawn('npx', ['tsx', 'unified_reconciliation.ts', 'date', date]);
    
    reconciliation.stdout.on('data', (data) => {
      console.log(`${data}`);
    });
    
    reconciliation.stderr.on('data', (data) => {
      console.error(`${data}`);
    });
    
    reconciliation.on('close', (code) => {
      if (code === 0) {
        log(`Reconciliation completed successfully for ${date}`, "success");
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

// Process missing periods for the date
async function processMissingPeriods(): Promise<void> {
  try {
    log(`Starting data reingestion for ${date}`, "info");
    
    // Initialize log file
    await fs.writeFile(LOG_FILE, `=== Data Reingestion: ${date} ===\n`);
    
    // Load BMU mappings once
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    let totalProcessed = 0;
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    let totalPeriods = 0;
    
    // Process each range of missing periods
    for (const range of missingRanges) {
      log(`Processing missing range: periods ${range.start}-${range.end}`, "info");
      totalPeriods += (range.end - range.start + 1);
      
      // Process periods in small batches
      for (let batchStart = range.start; batchStart <= range.end; batchStart += BATCH_SIZE) {
        const batchEnd = Math.min(batchStart + BATCH_SIZE - 1, range.end);
        
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
    }
    
    // Final summary
    const successRate = (totalProcessed / totalPeriods) * 100;
    
    log(`=== Processing complete for ${date} ===`, successRate === 100 ? "success" : "warning");
    log(`- Periods processed: ${totalProcessed}/${totalPeriods} (${successRate.toFixed(1)}%)`, "info");
    log(`- Records added: ${totalRecords}`, "info");
    log(`- Total volume: ${totalVolume.toFixed(2)} MWh`, "info");
    log(`- Total payment: £${totalPayment.toFixed(2)}`, "info");
    
    // Run reconciliation to update Bitcoin calculations
    log(`Running reconciliation to update Bitcoin calculations...`, "info");
    await runReconciliation();
    
    // Verify data completeness
    const periodsAfter = await db
      .select({ period: curtailmentRecords.settlementPeriod, count: count(curtailmentRecords.id) })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
    
    log(`=== Verification ===`, "info");
    log(`Periods in database after reingestion: ${periodsAfter.length}/48`, periodsAfter.length === 48 ? "success" : "warning");
    
    // Final status
    log(`=== Complete ===`, "success");
    log(`Data reingestion and reconciliation completed for ${date}`, "success");
    
  } catch (error) {
    log(`Fatal error during processing: ${error}`, "error");
    await logToFile(`Fatal error during processing: ${error}`);
    process.exit(1);
  }
}

// Main function
async function main() {  
  await processMissingPeriods();
  process.exit(0);
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});