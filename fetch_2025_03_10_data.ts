/**
 * Script to fetch and validate Elexon data for 2025-03-10
 * This script will check and fetch any missing periods, particularly 47-48
 * Based on optimized_critical_date_processor.ts
 */

import { db } from './db';
import { and, between, eq } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { spawn } from 'child_process';

// Handle ESM module dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const BMU_MAPPING_PATH = path.join(__dirname, 'server', 'data', 'bmuMapping.json');
const LOG_FILE = `fetch_2025_03_10_data_${new Date().toISOString().split('T')[0]}.log`;
const MAX_RETRIES = 3;
const RETRY_DELAY = 5000; // 5 seconds
const BATCH_SIZE = 5; // Number of periods to process in a batch

// Define the date we're processing
const date = '2025-03-10';

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
    
    log(`Period ${period}: Found ${validRecords.length} valid records (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`, "success");
    
    let recordsAdded = 0;
    let totalVolumeAdded = 0;
    let totalPaymentAdded = 0;
    
    // Get the unique farm IDs for this period
    const uniqueFarmIds = [...new Set(validRecords.map((record: any) => record.id))];
    
    // Clear all existing records for this period to avoid duplicates
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

// Check if a period has data but might be incomplete or missing some farms
async function checkPeriodCompleteness(
  period: number, 
  windFarmIds: Set<string>
): Promise<{
  complete: boolean;
  existingRecords: number;
  missingFarms: string[];
}> {
  try {
    // Get existing records for this period
    const existingRecords = await db.select({
      farmId: curtailmentRecords.farmId
    })
    .from(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, date),
        eq(curtailmentRecords.settlementPeriod, period)
      )
    );
    
    // Check API for this period
    const response = await axios.get(`${API_BASE_URL}/balancing/bid-offer/accepted/settlement-period/${period}/settlement-date/${date}`);
    const data = response.data.data || [];
    
    // Filter to keep only valid wind farm records
    const validApiRecords = data.filter((record: any) => {
      return windFarmIds.has(record.id) && record.volume < 0; // Negative volume indicates curtailment
    });
    
    // Get farm IDs that exist in API but not in database
    const dbFarmIds = new Set(existingRecords.map(r => r.farmId));
    const apiFarmIds = new Set(validApiRecords.map((r: any) => r.id));
    
    const missingFarms = [...apiFarmIds].filter((farmId: string) => !dbFarmIds.has(farmId));
    
    // If we have missing farms or no records at all, consider it incomplete
    const isComplete = missingFarms.length === 0 && existingRecords.length === validApiRecords.length;
    
    return {
      complete: isComplete,
      existingRecords: existingRecords.length,
      missingFarms
    };
  } catch (error) {
    log(`Error checking completeness for period ${period}: ${error}`, "error");
    return {
      complete: false,
      existingRecords: 0,
      missingFarms: []
    };
  }
}

// Process a set of periods that need attention
async function processPeriods(
  periodsToProcess: number[],
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  totalRecords: number;
  totalVolume: number;
  totalPayment: number;
}> {
  let totalRecords = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  
  for (const period of periodsToProcess) {
    const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
    
    if (result.success) {
      totalRecords += result.records;
      totalVolume += result.volume;
      totalPayment += result.payment;
    }
    
    // Add a delay between periods to avoid rate limiting
    await delay(1500);
  }
  
  return {
    totalRecords,
    totalVolume,
    totalPayment
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

// Main function
async function main() {
  try {
    log(`Starting data validation and fetch for ${date}`, "info");
    
    // Initialize log file
    await fs.writeFile(LOG_FILE, `=== 2025-03-10 Data Validation and Fetch ===\n`);
    
    // Load BMU mappings once
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Identify missing or incomplete periods
    const incompletePeriodsDetected: number[] = [];
    const missingPeriodsDetected: number[] = [];
    
    // First check if periods 47-48 exist, as they were identified as missing
    log(`Checking for missing periods 47-48...`, "info");
    
    const periodCounts = await db.select({
      period: curtailmentRecords.settlementPeriod,
      count: db.sql<number>`count(*)`
    })
    .from(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, date),
        between(curtailmentRecords.settlementPeriod, 47, 48)
      )
    )
    .groupBy(curtailmentRecords.settlementPeriod);
    
    // Check if we're missing periods 47-48
    const existingPeriods = new Set(periodCounts.map(p => p.period));
    for (let period = 47; period <= 48; period++) {
      if (!existingPeriods.has(period)) {
        missingPeriodsDetected.push(period);
        log(`Period ${period} is missing completely`, "warning");
      }
    }
    
    // Check all periods 1-48 for completeness
    log(`Checking completeness of all periods 1-48...`, "info");
    
    for (let period = 1; period <= 48; period++) {
      // Skip periods we already know are missing
      if (missingPeriodsDetected.includes(period)) continue;
      
      const completeness = await checkPeriodCompleteness(period, windFarmIds);
      
      if (!completeness.complete) {
        if (completeness.existingRecords === 0) {
          missingPeriodsDetected.push(period);
          log(`Period ${period} is missing completely`, "warning");
        } else {
          incompletePeriodsDetected.push(period);
          log(`Period ${period} is incomplete (has ${completeness.existingRecords} records, missing ${completeness.missingFarms.length} farms)`, "warning");
        }
      } else {
        log(`Period ${period} is complete with ${completeness.existingRecords} records`, "success");
      }
      
      // Add delay to avoid rate limiting
      await delay(1000);
    }
    
    // Process missing periods
    if (missingPeriodsDetected.length > 0) {
      log(`Processing ${missingPeriodsDetected.length} missing periods: ${missingPeriodsDetected.join(', ')}`, "info");
      
      const missingResult = await processPeriods(
        missingPeriodsDetected, 
        windFarmIds, 
        bmuLeadPartyMap
      );
      
      log(`Processed ${missingPeriodsDetected.length} missing periods:`, "success");
      log(`- Added ${missingResult.totalRecords} records`, "info");
      log(`- Total volume: ${missingResult.totalVolume.toFixed(2)} MWh`, "info");
      log(`- Total payment: £${missingResult.totalPayment.toFixed(2)}`, "info");
    } else {
      log(`No completely missing periods detected`, "success");
    }
    
    // Process incomplete periods
    if (incompletePeriodsDetected.length > 0) {
      log(`Refreshing ${incompletePeriodsDetected.length} incomplete periods: ${incompletePeriodsDetected.join(', ')}`, "info");
      
      const incompleteResult = await processPeriods(
        incompletePeriodsDetected, 
        windFarmIds, 
        bmuLeadPartyMap
      );
      
      log(`Refreshed ${incompletePeriodsDetected.length} incomplete periods:`, "success");
      log(`- Updated ${incompleteResult.totalRecords} records`, "info");
      log(`- Total volume: ${incompleteResult.totalVolume.toFixed(2)} MWh`, "info");
      log(`- Total payment: £${incompleteResult.totalPayment.toFixed(2)}`, "info");
    } else {
      log(`No incomplete periods detected`, "success");
    }
    
    // Run reconciliation if we made any changes
    if (missingPeriodsDetected.length > 0 || incompletePeriodsDetected.length > 0) {
      log(`Running reconciliation to update Bitcoin calculations...`, "info");
      await runReconciliation();
    } else {
      log(`No changes were made, skipping reconciliation`, "info");
    }
    
    // Final check
    const finalStats = await db.select({
      count: db.sql<number>`count(*)`,
      totalVolume: db.sql<number>`SUM(CAST(${curtailmentRecords.volume} AS DECIMAL))`,
      totalPayment: db.sql<number>`SUM(CAST(${curtailmentRecords.payment} AS DECIMAL))`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
    
    log(`=== Final statistics for ${date} ===`, "success");
    log(`- Total records: ${finalStats[0].count}`, "info");
    log(`- Total volume: ${Math.abs(Number(finalStats[0].totalVolume)).toFixed(2)} MWh`, "info");
    log(`- Total payment: £${Math.abs(Number(finalStats[0].totalPayment)).toFixed(2)}`, "info");
    
    log(`=== Process completed ===`, "success");
    
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