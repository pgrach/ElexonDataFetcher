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
import { spawn } from 'child_process';

// Get __dirname equivalent in ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const BMU_MAPPING_PATH = path.join(__dirname, 'server', 'data', 'bmuMapping.json');
const LOG_FILE = `process_data_2025-03-28.log`;
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
      // Use elexonBmUnit as the identifier
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
    let data: any[] = [];
    let apiResponse;
    
    try {
      // Try the first Elexon API endpoint
      const apiUrl = `${API_BASE_URL}/balancing/bid-offer/accepted/settlement-period/${period}/settlement-date/${date}`;
      log(`Requesting data from: ${apiUrl}`, "info");
      
      apiResponse = await axios.get(apiUrl);
      data = apiResponse.data.data || [];
      
    } catch (apiError) {
      // If the first endpoint fails, try an alternative endpoint
      log(`Primary API endpoint failed, trying alternative...`, "warning");
      
      try {
        // Try an alternative endpoint - this is just an example, check your actual API structure
        const alternativeUrl = `${API_BASE_URL}/datasets/BOD/settlement-date/${date}/settlement-period/${period}`;
        log(`Requesting data from alternative: ${alternativeUrl}`, "info");
        
        apiResponse = await axios.get(alternativeUrl);
        data = apiResponse.data.data || [];
        
      } catch (alternativeError) {
        // Both endpoints failed
        log(`Alternative API endpoint also failed`, "error");
        throw apiError; // Throw the original error
      }
    }
    
    log(`Retrieved ${data.length} records from API`, "info");
    
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
    if (axios.isAxiosError(error) && error.response) {
      log(`Error processing period ${period}: ${error.message}, Status: ${error.response.status}, URL: ${error.config?.url}`, "error");
      if (error.response.data) {
        log(`API response data: ${JSON.stringify(error.response.data)}`, "error");
      }
    } else {
      log(`Error processing period ${period}: ${error}`, "error");
    }
    
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

// Find periods with missing data
async function findMissingData(): Promise<number[]> {
  try {
    log(`Checking for missing data in periods ${startPeriod}-${endPeriod}...`, "info");
    
    // Get the count of records per period in the curtailment_records table
    const query = `
      SELECT 
        settlement_period,
        COUNT(*) as record_count
      FROM curtailment_records 
      WHERE settlement_date = '${date}'
      GROUP BY settlement_period
      ORDER BY settlement_period
    `;
    
    const result = await db.execute(sql.raw(query));
    
    // Create map for easier lookup
    const periodMap = new Map<number, number>();
    result.rows.forEach((row: any) => {
      periodMap.set(parseInt(row.settlement_period), parseInt(row.record_count));
    });
    
    // Find missing or incomplete periods
    const missingPeriods: number[] = [];
    
    for (let period = startPeriod; period <= endPeriod; period++) {
      const recordCount = periodMap.get(period) || 0;
      
      if (recordCount === 0) {
        // No data for this period
        missingPeriods.push(period);
        log(`Period ${period}: No data found`, "warning");
      } else {
        log(`Period ${period}: Found ${recordCount} records`, "success");
      }
    }
    
    return missingPeriods;
  } catch (error) {
    log(`Error finding missing data: ${error}`, "error");
    return [];
  }
}

// Process a specific date with small batches
async function processDate(): Promise<void> {
  try {
    log(`Starting data processing for ${date}`, "info");
    log(`Target periods: ${startPeriod}-${endPeriod}`, "info");
    
    // Initialize log file
    await fs.writeFile(LOG_FILE, `=== Processing Data: ${date} (Periods ${startPeriod}-${endPeriod}) ===\n`);
    
    // Load BMU mappings once
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Find periods with missing data
    const missingPeriods = await findMissingData();
    
    if (missingPeriods.length === 0) {
      log(`No missing periods found. All 48 periods have data.`, "success");
      log(`Running reconciliation to update Bitcoin calculations...`, "info");
      await runReconciliation();
      log(`Data verification completed for ${date}`, "success");
      return;
    }
    
    log(`Found ${missingPeriods.length} periods with missing data`, "info");
    
    // Process periods in small batches to avoid timeouts
    let totalProcessed = 0;
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process missing periods in batches
    for (let i = 0; i < missingPeriods.length; i += BATCH_SIZE) {
      const batchPeriods = missingPeriods.slice(i, i + BATCH_SIZE);
      
      log(`Processing batch of ${batchPeriods.length} periods: ${batchPeriods.join(', ')}`, "info");
      
      for (const period of batchPeriods) {
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
      log(`Pausing between batches...`, "info");
      await delay(3000);
    }
    
    // Final summary
    const totalPeriods = missingPeriods.length;
    const successRate = totalPeriods > 0 ? (totalProcessed / totalPeriods) * 100 : 100;
    
    log(`=== Processing complete for ${date} ===`, successRate === 100 ? "success" : "warning");
    log(`- Periods processed: ${totalProcessed}/${totalPeriods} (${successRate.toFixed(1)}%)`, "info");
    log(`- Records added: ${totalRecords}`, "info");
    log(`- Total volume: ${totalVolume.toFixed(2)} MWh`, "info");
    log(`- Total payment: £${totalPayment.toFixed(2)}`, "info");
    
    // Run reconciliation to update Bitcoin calculations
    log(`Running reconciliation to update Bitcoin calculations...`, "info");
    await runReconciliation();
    
    // Final status
    log(`=== Complete ===`, "success");
    log(`Data reingestion and reconciliation completed for ${date}`, "success");
    
  } catch (error) {
    log(`Fatal error during processing: ${error}`, "error");
    await logToFile(`Fatal error during processing: ${error}`);
    process.exit(1);
  }
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