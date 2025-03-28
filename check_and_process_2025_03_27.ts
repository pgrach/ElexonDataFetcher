/**
 * Script to check and process data for 2025-03-27
 * 
 * This script checks the original data from Elexon API for 2025-03-27
 * and compares it with existing data in the database. It will process
 * any missing periods (specifically 35-48) for this date.
 */

// Add TypeScript ES modules configuration
// @ts-check
/// <reference types="node" />
/// <reference types="vite/client" />
/// <reference types="typescript" />

// Force this file to be treated as an ES module
export {};

import { db } from './db';
import { and, between, eq, sql } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';

// Get the directory name using ES module approach
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const BMU_MAPPING_PATH = path.join(__dirname, 'server', 'data', 'bmuMapping.json');
const LOG_FILE = `process_2025_03_27_${new Date().toISOString().split('T')[0]}.log`;
const MAX_RETRIES = 3;
const RETRY_DELAY = 5000; // 5 seconds
const BATCH_SIZE = 5; // Number of periods to process in a batch

// Set target date
const targetDate = '2025-03-27';
const startPeriod = 35; // We'll process periods 35-48
const endPeriod = 48;

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

// Get current status from database
async function checkDatabaseStatus(): Promise<{
  earliestPeriod: number | null;
  latestPeriod: number | null;
  recordCount: number;
  periodCount: number;
  missingPeriods: number[];
}> {
  try {
    // Get a summary of existing records
    const result = await db.execute(sql`
      SELECT 
        MIN(settlement_period) as min_period,
        MAX(settlement_period) as max_period,
        COUNT(*) as record_count,
        COUNT(DISTINCT settlement_period) as period_count
      FROM 
        curtailment_records
      WHERE 
        settlement_date = ${targetDate}
    `);

    const row = result.rows[0];
    const min_period = row.min_period ? row.min_period.toString() : null;
    const max_period = row.max_period ? row.max_period.toString() : null;
    const record_count = row.record_count ? row.record_count.toString() : '0';
    const period_count = row.period_count ? row.period_count.toString() : '0';
    
    // Find missing periods (if any)
    const periodsResult = await db.execute(sql`
      SELECT settlement_period 
      FROM curtailment_records 
      WHERE settlement_date = ${targetDate} 
      GROUP BY settlement_period 
      ORDER BY settlement_period
    `);
    
    const existingPeriods = periodsResult.rows.map(row => parseInt(row.settlement_period.toString(), 10));
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    const missingPeriods = allPeriods.filter(p => !existingPeriods.includes(p));
    
    return {
      earliestPeriod: min_period ? parseInt(min_period, 10) : null,
      latestPeriod: max_period ? parseInt(max_period, 10) : null,
      recordCount: parseInt(record_count, 10),
      periodCount: parseInt(period_count, 10),
      missingPeriods
    };
  } catch (error) {
    log(`Error checking database status: ${error}`, "error");
    return {
      earliestPeriod: null,
      latestPeriod: null,
      recordCount: 0,
      periodCount: 0,
      missingPeriods: Array.from({ length: 48 }, (_, i) => i + 1)
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
    const response = await axios.get(`${API_BASE_URL}/balancing/bid-offer/accepted/settlement-period/${period}/settlement-date/${targetDate}`);
    const data = response.data.data || [];
    
    // Filter to keep only valid wind farm records
    const validRecords = data.filter((record: any) => {
      return windFarmIds.has(record.id) && record.volume < 0; // Negative volume indicates curtailment
    });
    
    const totalVolume = validRecords.reduce((sum: number, record: any) => sum + Math.abs(record.volume), 0);
    const totalPayment = validRecords.reduce((sum: number, record: any) => sum + (Math.abs(record.volume) * record.originalPrice), 0);
    
    log(`[${targetDate} P${period}] Records: ${validRecords.length} (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`);
    log(`Period ${period}: Found ${validRecords.length} valid records`, "success");
    
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
            eq(curtailmentRecords.settlementDate, targetDate),
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
        settlementDate: targetDate,
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

// Trigger Bitcoin calculation update
async function runReconciliation(): Promise<void> {
  return new Promise((resolve, reject) => {
    log(`Running reconciliation for ${targetDate}...`, "info");
    
    const reconciliationProcess = spawn('npx', ['tsx', 'optimized_critical_date_processor.ts', targetDate, '35', '48']);
    
    reconciliationProcess.stdout.on('data', (data) => {
      console.log(`${data}`);
    });
    
    reconciliationProcess.stderr.on('data', (data) => {
      console.error(`${data}`);
    });
    
    reconciliationProcess.on('close', (code) => {
      if (code === 0) {
        log(`Reconciliation completed successfully for ${targetDate}`, "success");
        resolve();
      } else {
        log(`Reconciliation process exited with code ${code}`, "error");
        resolve(); // Resolve anyway to continue with our process
      }
    });
    
    // Add timeout to prevent hanging
    setTimeout(() => {
      log(`Reconciliation timed out after 120 seconds, continuing anyway`, "warning");
      resolve();
    }, 120000);
  });
}

// Main function
async function main() {
  try {
    log(`=== Starting data check and processing for ${targetDate} ===`, "info");
    
    // Initialize log file
    await fs.writeFile(LOG_FILE, `=== Data Check and Processing: ${targetDate} ===\n`);
    
    // Check current database status
    const status = await checkDatabaseStatus();
    log(`Current database status for ${targetDate}:`, "info");
    log(`- Record count: ${status.recordCount}`, "info");
    log(`- Period count: ${status.periodCount}/48`, "info");
    log(`- Earliest period: ${status.earliestPeriod || 'None'}`, "info");
    log(`- Latest period: ${status.latestPeriod || 'None'}`, "info");
    
    if (status.missingPeriods.length > 0) {
      log(`Missing periods: ${status.missingPeriods.join(', ')}`, "warning");
      
      // Load BMU mappings once
      const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
      
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
      
      log(`=== Processing complete for ${targetDate} ===`, successRate === 100 ? "success" : "warning");
      log(`- Periods processed: ${totalProcessed}/${totalPeriods} (${successRate.toFixed(1)}%)`, "info");
      log(`- Records added: ${totalRecords}`, "info");
      log(`- Total volume: ${totalVolume.toFixed(2)} MWh`, "info");
      log(`- Total payment: £${totalPayment.toFixed(2)}`, "info");
      
      // Run reconciliation to update Bitcoin calculations
      log(`Running reconciliation to update Bitcoin calculations...`, "info");
      await runReconciliation();
      
      // Final verification
      const finalStatus = await checkDatabaseStatus();
      log(`Final database status for ${targetDate}:`, "info");
      log(`- Record count: ${finalStatus.recordCount}`, "info");
      log(`- Period count: ${finalStatus.periodCount}/48`, "info");
      
      if (finalStatus.missingPeriods.length > 0) {
        log(`Still missing periods: ${finalStatus.missingPeriods.join(', ')}`, "warning");
      } else {
        log(`All 48 periods now processed for ${targetDate}`, "success");
      }
    } else {
      log(`All 48 periods already exist for ${targetDate}`, "success");
    }
    
    log(`=== Complete ===`, "success");
    
  } catch (error) {
    log(`Fatal error during processing: ${error}`, "error");
    await logToFile(`Fatal error during processing: ${error}`);
    process.exit(1);
  }
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});