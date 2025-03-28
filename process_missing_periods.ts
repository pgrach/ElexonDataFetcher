/**
 * Process Missing Periods
 * 
 * This script is based on optimized_critical_date_processor.ts but modified to use
 * the historicalReconciliation.ts functions directly for Bitcoin reconciliation
 * instead of the unified_reconciliation.ts script.
 * 
 * Usage:
 *   npx tsx process_missing_periods.ts <date> [start_period] [end_period]
 * 
 * Example:
 *   npx tsx process_missing_periods.ts 2025-03-27 35 48
 */

import { db } from './db';
import { and, between, eq } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { processDate } from './server/services/historicalReconciliation';

// Get current file directory (ESM equivalent of __dirname)
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const BMU_MAPPING_PATH = path.join(__dirname, 'server', 'data', 'bmuMapping.json');
const LOG_FILE = `process_missing_periods_${new Date().toISOString().split('T')[0]}.log`;
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
      // Use the elexonBmUnit field as the ID for wind farms
      if (bmu.elexonBmUnit) {
        windFarmIds.add(bmu.elexonBmUnit);
        bmuLeadPartyMap.set(bmu.elexonBmUnit, bmu.leadPartyName);
        
        // Log some sample mappings for debugging
        if (windFarmIds.size <= 5) {
          log(`  Mapping: ${bmu.elexonBmUnit} -> ${bmu.leadPartyName}`, "info");
        }
      }
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
    // Determine which API endpoint to use based on the period
    // For periods 35-48, use the bid-offer stack endpoint which has data
    // For periods 1-34, use the original endpoint
    let response;
    let data;
    
    if (period >= 35 && period <= 48) {
      // Use the Bid-Offer Stack endpoint for periods 35-48
      log(`Using alternative Bid-Offer Stack endpoint for period ${period}`, "info");
      response = await axios.get(`${API_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`);
      data = response.data.data || [];
    } else {
      // Use the original endpoint for periods 1-34
      log(`Using original Bid-Offer Accepted endpoint for period ${period}`, "info");
      response = await axios.get(`${API_BASE_URL}/balancing/bid-offer/accepted/settlement-period/${period}/settlement-date/${date}`);
      data = response.data.data || [];
    }
    
    // Filter to keep only valid wind farm records - handle both API formats
    const validRecords = data.filter((record: any) => {
      // For bid-offer stack endpoint, negative volume indicates curtailment
      const farmId = record.id;
      // Check if volume is negative (curtailment)
      const volume = record.volume;
      
      // Log first 10 negative volume records as examples
      if (period === 35 && record.volume < 0 && data.filter(r => r.volume < 0).indexOf(record) < 10) {
        log(`Record in period ${period}: ID=${farmId}, Volume=${volume}, Original price=${record.originalPrice}`, "info");
      }
      
      return windFarmIds.has(farmId) && volume < 0;
    });
    
    const totalVolume = validRecords.reduce((sum: number, record: any) => sum + Math.abs(record.volume), 0);
    const totalPayment = validRecords.reduce((sum: number, record: any) => sum + (Math.abs(record.volume) * record.originalPrice), 0);
    
    log(`Period ${period}: Found ${validRecords.length} valid records (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`, "success");
    
    let recordsAdded = 0;
    let totalVolumeAdded = 0;
    let totalPaymentAdded = 0;
    
    // Get the unique farm IDs for this period
    const uniqueFarmIds = [...new Set(validRecords.map((record: any) => record.id))];
    
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
        soFlag: record.soFlag,
        cadlFlag: record.cadlFlag
      };
    });
    
    // Insert all records in a single transaction if there are any
    if (recordsToInsert.length > 0) {
      try {
        await db.insert(curtailmentRecords).values(recordsToInsert);
        recordsAdded = recordsToInsert.length;
        
        // Log a summary of records by farm
        const farmSummary = validRecords.reduce((summary: Record<string, { count: number, volume: number, payment: number }>, record: any) => {
          const farmId = record.id;
          const volume = Math.abs(record.volume);
          const payment = volume * record.originalPrice;
          
          if (!summary[farmId]) {
            summary[farmId] = { count: 0, volume: 0, payment: 0 };
          }
          
          summary[farmId].count++;
          summary[farmId].volume += volume;
          summary[farmId].payment += payment;
          
          return summary;
        }, {});
        
        // Log farm summaries for the period
        log(`Period ${period}: Added records for ${Object.keys(farmSummary).length} farms:`, "success");
        
        // Log top 5 farms by volume
        const topFarms = Object.entries(farmSummary)
          .sort((a, b) => b[1].volume - a[1].volume)
          .slice(0, 5);
          
        for (const [farmId, data] of topFarms) {
          log(`  ${farmId}: ${data.count} records, ${data.volume.toFixed(2)} MWh, £${data.payment.toFixed(2)}`, "success");
        }
        
        if (Object.keys(farmSummary).length > 5) {
          log(`  ... and ${Object.keys(farmSummary).length - 5} more farms`, "success");
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
async function processDatePeriods(): Promise<void> {
  try {
    log(`Starting critical date processing for ${date}`, "info");
    log(`Target periods: ${startPeriod}-${endPeriod}`, "info");
    
    // Initialize log file
    await fs.writeFile(LOG_FILE, `=== Processing Missing Periods: ${date} (Periods ${startPeriod}-${endPeriod}) ===\n`);
    
    // Load BMU mappings once
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // First check if we need to clear existing records
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

// Run reconciliation to update Bitcoin calculations using the functions from historicalReconciliation.ts
async function runReconciliation(): Promise<void> {
  try {
    log(`Starting reconciliation process for ${date}...`, "info");
    
    // Use the processDate function from historicalReconciliation.ts
    const result = await processDate(date);
    
    if (result.success) {
      log(`Reconciliation completed successfully: ${result.message}`, "success");
    } else {
      log(`Reconciliation had some issues: ${result.message}`, "warning");
    }
  } catch (error) {
    log(`Error during reconciliation: ${error}`, "error");
  }
}

// Main function
async function main() {  
  await processDatePeriods();
  process.exit(0);
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});