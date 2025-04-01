/**
 * Process Missing Data for 2025-03-28
 * 
 * This script will:
 * 1. Check current data for 2025-03-28
 * 2. Process any missing or incomplete data from Elexon API
 * 3. Update all Bitcoin calculations to ensure completeness
 */

import { db } from './db';
import { and, between, eq, sql } from 'drizzle-orm';
import { curtailmentRecords, historicalBitcoinCalculations } from './db/schema';
import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { processSingleDay } from './server/services/bitcoinService';
import { auditAndFixBitcoinCalculations } from './server/services/historicalReconciliation';

// Get __dirname equivalent in ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const BMU_MAPPING_PATH = path.join(__dirname, 'server', 'data', 'bmuMapping.json');
const LOG_FILE = `process_missing_periods_2025-03-28.log`;
const MAX_RETRIES = 3;
const RETRY_DELAY = 5000; // 5 seconds
const BATCH_SIZE = 5; // Number of periods to process in a batch

// Date to process
const date = '2025-03-28';
const startPeriod = 1;
const endPeriod = 48;
const MINER_MODEL_LIST = ['S19J_PRO', 'S9', 'M20S'];

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

// Check which periods have data from the Elexon API but are missing in our database
async function comparePeriods(
  windFarmIds: Set<string>,
  period: number
): Promise<{
  apiRecords: number;
  dbRecords: number;
  apiVolume: number;
  dbVolume: number;
  needsProcessing: boolean;
}> {
  try {
    // Get data from Elexon API
    const response = await axios.get(`${API_BASE_URL}/balancing/bid-offer/accepted/settlement-period/${period}/settlement-date/${date}`);
    const apiData = response.data.data || [];
    
    // Filter to keep only valid wind farm records
    const validApiRecords = apiData.filter((record: any) => {
      return windFarmIds.has(record.id) && record.volume < 0; // Negative volume indicates curtailment
    });
    
    const apiVolume = validApiRecords.reduce((sum: number, record: any) => sum + Math.abs(record.volume), 0);
    
    // Get data from our database
    const dbRecords = await db
      .select({
        count: sql<number>`COUNT(*)`,
        volume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          eq(curtailmentRecords.settlementPeriod, period)
        )
      );
    
    const dbCount = dbRecords[0]?.count || 0;
    const dbVolume = Number(dbRecords[0]?.volume || 0);
    
    // Check if there's a significant difference
    const volumeDifference = Math.abs(apiVolume - dbVolume);
    const recordDifference = Math.abs(validApiRecords.length - dbCount);
    
    // Determine if this period needs processing
    const needsProcessing = 
      validApiRecords.length > 0 && 
      (dbCount === 0 || recordDifference > 0 || volumeDifference > 0.1);
    
    return {
      apiRecords: validApiRecords.length,
      dbRecords: dbCount,
      apiVolume,
      dbVolume,
      needsProcessing
    };
  } catch (error) {
    log(`Error comparing period ${period}: ${error}`, "error");
    return {
      apiRecords: 0,
      dbRecords: 0,
      apiVolume: 0,
      dbVolume: 0,
      needsProcessing: false
    };
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
    
    log(`Period ${period}: Found ${validRecords.length} valid records (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`, "success");
    
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

// Process a specific date with small batches
async function processDate(): Promise<void> {
  try {
    log(`Starting data processing for ${date}`, "info");
    log(`Target periods: ${startPeriod}-${endPeriod}`, "info");
    
    // Initialize log file
    await fs.writeFile(LOG_FILE, `=== Processing Missing Data: ${date} (Periods ${startPeriod}-${endPeriod}) ===\n`);
    
    // Load BMU mappings once
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Check all periods to identify discrepancies
    log(`Checking all periods for discrepancies between API and database...`, "info");
    
    let periodsToProcess: number[] = [];
    
    // Process in small batches to avoid API rate limits
    for (let period = startPeriod; period <= endPeriod; period += BATCH_SIZE) {
      const batchEndPeriod = Math.min(period + BATCH_SIZE - 1, endPeriod);
      log(`Checking periods ${period}-${batchEndPeriod}...`, "info");
      
      const promises = [];
      for (let p = period; p <= batchEndPeriod; p++) {
        promises.push(comparePeriods(windFarmIds, p));
      }
      
      // Wait for all comparisons to complete
      const results = await Promise.all(promises);
      
      // Process results
      for (let i = 0; i < results.length; i++) {
        const result = results[i];
        const currentPeriod = period + i;
        
        if (result.needsProcessing) {
          log(`Period ${currentPeriod}: Discrepancy detected - API: ${result.apiRecords} records (${result.apiVolume.toFixed(2)} MWh), DB: ${result.dbRecords} records (${result.dbVolume.toFixed(2)} MWh)`, "warning");
          periodsToProcess.push(currentPeriod);
        } else {
          if (result.apiRecords > 0) {
            log(`Period ${currentPeriod}: No discrepancy - ${result.apiRecords} records (${result.apiVolume.toFixed(2)} MWh)`, "success");
          }
        }
      }
      
      // Add a delay between batches to avoid API rate limits
      if (period + BATCH_SIZE <= endPeriod) {
        log(`Pausing between check batches...`, "info");
        await delay(3000);
      }
    }
    
    // Process only periods with discrepancies
    if (periodsToProcess.length > 0) {
      log(`Found ${periodsToProcess.length} periods with discrepancies. Processing...`, "warning");
      
      let totalProcessed = 0;
      let totalRecords = 0;
      let totalVolume = 0;
      let totalPayment = 0;
      
      // Process in small batches
      for (let i = 0; i < periodsToProcess.length; i += BATCH_SIZE) {
        const batch = periodsToProcess.slice(i, i + BATCH_SIZE);
        log(`Processing batch: periods ${batch.join(', ')}`, "info");
        
        for (const period of batch) {
          const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
          
          if (result.success) {
            totalProcessed++;
            totalRecords += result.records;
            totalVolume += result.volume;
            totalPayment += result.payment;
          }
          
          // Add a delay between periods
          await delay(1500);
        }
        
        // Add a longer delay between batches
        if (i + BATCH_SIZE < periodsToProcess.length) {
          log(`Pausing between process batches...`, "info");
          await delay(3000);
        }
      }
      
      // Final summary
      log(`=== Processing complete for ${date} ===`, "success");
      log(`- Periods processed: ${totalProcessed}/${periodsToProcess.length}`, "info");
      log(`- Records added: ${totalRecords}`, "info");
      log(`- Total volume: ${totalVolume.toFixed(2)} MWh`, "info");
      log(`- Total payment: £${totalPayment.toFixed(2)}`, "info");
    } else {
      log(`No discrepancies found for ${date}. All data is up to date.`, "success");
    }
    
    // Run reconciliation to update Bitcoin calculations
    log(`Updating Bitcoin calculations...`, "info");
    await updateBitcoinCalculations();
    
    // Final status
    log(`=== Complete ===`, "success");
    log(`Data processing and Bitcoin calculations completed for ${date}`, "success");
    
  } catch (error) {
    log(`Fatal error during processing: ${error}`, "error");
    await logToFile(`Fatal error during processing: ${error}`);
    process.exit(1);
  }
}

// Update Bitcoin calculations for the date
async function updateBitcoinCalculations(): Promise<void> {
  try {
    log(`Updating Bitcoin calculations for ${date}...`, "info");
    
    const result = await auditAndFixBitcoinCalculations(date);
    
    if (result.success) {
      log(`Bitcoin calculations updated successfully: ${result.message}`, "success");
    } else {
      log(`Failed to update Bitcoin calculations: ${result.message}`, "error");
      
      // Try manual approach with each miner model
      log(`Trying manual approach for each miner model...`, "info");
      
      for (const minerModel of MINER_MODEL_LIST) {
        try {
          log(`Processing ${minerModel} for ${date}...`, "info");
          await processSingleDay(date, minerModel);
          log(`Successfully processed ${minerModel} for ${date}`, "success");
        } catch (error) {
          log(`Error processing ${minerModel} for ${date}: ${error}`, "error");
        }
      }
    }
  } catch (error) {
    log(`Error updating Bitcoin calculations: ${error}`, "error");
  }
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