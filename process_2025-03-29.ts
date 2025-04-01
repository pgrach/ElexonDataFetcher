/**
 * Process Missing Data for 2025-03-29
 * 
 * This script will:
 * 1. Check current data for 2025-03-29
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
const LOG_FILE = `process_missing_periods_2025-03-29.log`;
const MAX_RETRIES = 3;
const RETRY_DELAY = 5000; // 5 seconds
const BATCH_SIZE = 5; // Number of periods to process in a batch

// Date to process
const date = '2025-03-29';
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
    // Import the fetchBidsOffers function from the existing service
    const { fetchBidsOffers } = await import('./server/services/elexon');
    const validRecords = await fetchBidsOffers(date, period);
    
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
async function processDate(): Promise<void> {
  try {
    log(`Starting data processing for ${date}`, "info");
    log(`Target periods: ${startPeriod}-${endPeriod}`, "info");
    
    // Initialize log file
    await fs.writeFile(LOG_FILE, `=== Processing Missing Data: ${date} (Periods ${startPeriod}-${endPeriod}) ===\n`);
    
    // Load BMU mappings once
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Check for missing Bitcoin calculations
    const missingCalculations = await findMissingCalculations();
    log(`Found ${missingCalculations.length} periods with missing calculations`, "info");
    
    // First check if we need to clear existing records
    try {
      log(`Checking for existing records for periods ${startPeriod}-${endPeriod}...`, "info");
      
      // We'll keep existing records and only process the ones with missing calculations
      log(`Will keep existing records and only process periods with missing calculations`, "info");
    } catch (error) {
      log(`Error checking existing records: ${error}`, "error");
      // Continue with processing anyway
    }
    
    // Process periods in small batches to avoid timeouts
    let totalProcessed = 0;
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Only process the periods with missing calculations
    if (missingCalculations.length > 0) {
      for (let i = 0; i < missingCalculations.length; i += BATCH_SIZE) {
        const batch = missingCalculations.slice(i, i + BATCH_SIZE);
        
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
        log(`Pausing between batches...`, "info");
        await delay(3000);
      }
    } else {
      log(`No missing periods to process. All 48 periods have data.`, "success");
    }
    
    // Final summary
    log(`=== Processing complete for ${date} ===`, "success");
    log(`- Periods processed: ${totalProcessed}/${missingCalculations.length}`, "info");
    log(`- Records added: ${totalRecords}`, "info");
    log(`- Total volume: ${totalVolume.toFixed(2)} MWh`, "info");
    log(`- Total payment: £${totalPayment.toFixed(2)}`, "info");
    
    // Update daily summary using curtailment service
    try {
      log('Using processDailyCurtailment to regenerate summaries', "info");
      const { processDailyCurtailment } = await import('./server/services/curtailment');
      await processDailyCurtailment(date);
      log('Daily summary updated successfully', "success");
    } catch (error) {
      log(`Error updating daily summary: ${error}`, "error");
      // Continue even if summary update fails - it's not critical
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

// Find periods with missing Bitcoin calculations
async function findMissingCalculations(): Promise<number[]> {
  try {
    // First, get the count of records per period in the curtailment_records table
    const curtailmentQuery = `
      SELECT 
        settlement_period,
        COUNT(*) as farm_count,
        COUNT(*) * 3 as expected_calculations
      FROM curtailment_records 
      WHERE settlement_date = '${date}'
      GROUP BY settlement_period
      ORDER BY settlement_period
    `;
    
    const curtailmentResult = await db.execute(sql.raw(curtailmentQuery));
    
    // Then, get the count of records per period in the historical_bitcoin_calculations table
    const calculationsQuery = `
      SELECT 
        settlement_period,
        COUNT(*) as actual_calculations
      FROM historical_bitcoin_calculations
      WHERE settlement_date = '${date}'
      GROUP BY settlement_period
      ORDER BY settlement_period
    `;
    
    const calculationsResult = await db.execute(sql.raw(calculationsQuery));
    
    // Create maps for easier lookup
    const curtailmentMap = new Map<number, { farmCount: number, expectedCalculations: number }>();
    curtailmentResult.rows.forEach((row: any) => {
      curtailmentMap.set(parseInt(row.settlement_period), {
        farmCount: parseInt(row.farm_count),
        expectedCalculations: parseInt(row.expected_calculations)
      });
    });
    
    const calculationsMap = new Map<number, number>();
    calculationsResult.rows.forEach((row: any) => {
      calculationsMap.set(parseInt(row.settlement_period), parseInt(row.actual_calculations));
    });
    
    // Find periods with discrepancies
    const missingPeriods: number[] = [];
    
    for (let period = 1; period <= 48; period++) {
      const curtailmentData = curtailmentMap.get(period);
      const actualCalculations = calculationsMap.get(period) || 0;
      
      if (!curtailmentData) {
        // No curtailment data for this period, needs to be processed
        missingPeriods.push(period);
      } else if (actualCalculations < curtailmentData.expectedCalculations) {
        // Missing some calculations for this period
        missingPeriods.push(period);
      }
    }
    
    return missingPeriods;
  } catch (error) {
    log(`Error finding missing calculations: ${error}`, "error");
    return [];
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