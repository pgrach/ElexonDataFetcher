/**
 * Script to update Elexon data for 2025-03-10
 * This script will check all periods (1-48) and ensure complete data,
 * focusing particularly on missing periods 47-48
 */

import { db } from './db';
import { and, between, count, eq, sql } from 'drizzle-orm';
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
const LOG_FILE = `update_elexon_data_2025_03_10_${new Date().toISOString().split('T')[0]}.log`;
const MAX_RETRIES = 3;
const RETRY_DELAY = 5000; // 5 seconds
const API_DELAY = 1500; // Delay between API calls to avoid rate limiting

// Define the date we're processing
const DATE = '2025-03-10';

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
    throw new Error(`Failed to load BMU mapping: ${error}`);
  }
}

// Check database for existing records in a period
async function checkExistingRecords(period: number): Promise<{
  count: number;
  farmIds: string[];
}> {
  try {
    const records = await db.select({
      farmId: curtailmentRecords.farmId
    })
    .from(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, DATE),
        eq(curtailmentRecords.settlementPeriod, period)
      )
    );
    
    return {
      count: records.length,
      farmIds: records.map(r => r.farmId)
    };
  } catch (error) {
    log(`Error checking existing records for period ${period}: ${error}`, "error");
    return { count: 0, farmIds: [] };
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
    // Make parallel requests for bids and offers to get the most complete data
    const [bidsResponse, offersResponse, acceptedResponse] = await Promise.all([
      axios.get(`${API_BASE_URL}/balancing/settlement/stack/all/bid/${DATE}/${period}`).catch(() => ({ data: { data: [] } })),
      axios.get(`${API_BASE_URL}/balancing/settlement/stack/all/offer/${DATE}/${period}`).catch(() => ({ data: { data: [] } })),
      axios.get(`${API_BASE_URL}/balancing/bid-offer/accepted/settlement-period/${period}/settlement-date/${DATE}`).catch(() => ({ data: { data: [] } }))
    ]);
    
    // Extract data from all sources
    const bidsData = bidsResponse.data?.data || [];
    const offersData = offersResponse.data?.data || [];
    const acceptedData = acceptedResponse.data?.data || [];
    
    // Filter to keep only valid wind farm records
    const validBids = bidsData.filter((record: any) => 
      windFarmIds.has(record.id) && record.volume < 0 && record.soFlag
    );
    
    const validOffers = offersData.filter((record: any) => 
      windFarmIds.has(record.id) && record.volume < 0 && record.soFlag
    );
    
    const validAccepted = acceptedData.filter((record: any) => 
      windFarmIds.has(record.id) && record.volume < 0
    );
    
    // Combine all records and remove duplicates by using a Map with a unique key
    const recordMap = new Map();
    
    // Helper to add records to the map
    const addRecordsToMap = (records: any[], source: string) => {
      for (const record of records) {
        const key = `${record.id}_${record.volume}_${record.originalPrice}`;
        if (!recordMap.has(key)) {
          recordMap.set(key, { ...record, source });
        }
      }
    };
    
    addRecordsToMap(validBids, 'bids');
    addRecordsToMap(validOffers, 'offers');
    addRecordsToMap(validAccepted, 'accepted');
    
    const validRecords = Array.from(recordMap.values());
    
    // Calculate totals
    const totalVolume = validRecords.reduce((sum: number, record: any) => sum + Math.abs(record.volume), 0);
    const totalPayment = validRecords.reduce((sum: number, record: any) => sum + (Math.abs(record.volume) * record.originalPrice), 0);
    
    log(`Period ${period}: Found ${validRecords.length} valid records (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`, "success");
    
    // Clear all existing records for this period to avoid duplicates
    try {
      const deleteResult = await db.delete(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, DATE),
            eq(curtailmentRecords.settlementPeriod, period)
          )
        );
      
      log(`Period ${period}: Cleared existing records before insertion`, "info");
    } catch (error) {
      log(`Period ${period}: Error clearing existing records: ${error}`, "error");
      throw error;
    }
    
    // Prepare all records for bulk insertion
    const recordsToInsert = validRecords.map((record: any) => {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice;
      
      return {
        settlementDate: DATE,
        settlementPeriod: period,
        farmId: record.id,
        leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
        volume: record.volume.toString(), // Keep negative value
        payment: payment.toString(),
        originalPrice: record.originalPrice.toString(),
        finalPrice: (record.finalPrice || record.originalPrice).toString(),
        soFlag: Boolean(record.soFlag),
        cadlFlag: record.cadlFlag !== undefined ? Boolean(record.cadlFlag) : null
      };
    });
    
    // Insert all records in a single transaction if there are any
    if (recordsToInsert.length > 0) {
      try {
        await db.insert(curtailmentRecords).values(recordsToInsert);
        log(`Period ${period}: Inserted ${recordsToInsert.length} records successfully`, "success");
        
        // Log individual records for visibility
        for (const record of validRecords) {
          const volume = Math.abs(record.volume);
          const payment = volume * record.originalPrice;
          log(`Period ${period}: Added ${record.id} (${volume.toFixed(2)} MWh, £${payment.toFixed(2)})`, "info");
        }
        
      } catch (error) {
        log(`Period ${period}: Error bulk inserting records: ${error}`, "error");
        throw error;
      }
    } else {
      log(`Period ${period}: No valid records to insert`, "warning");
    }
    
    return { 
      success: true, 
      records: recordsToInsert.length,
      volume: totalVolume,
      payment: totalPayment
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

// Process a prioritized list of periods
async function processPeriods(
  periodsToProcess: number[],
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  totalRecords: number;
  totalVolume: number;
  totalPayment: number;
  processedPeriods: number;
  failedPeriods: number[];
}> {
  let totalRecords = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  let processedPeriods = 0;
  const failedPeriods: number[] = [];
  
  for (const period of periodsToProcess) {
    const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
    
    if (result.success) {
      totalRecords += result.records;
      totalVolume += result.volume;
      totalPayment += result.payment;
      processedPeriods++;
    } else {
      failedPeriods.push(period);
    }
    
    // Add a delay between periods to avoid rate limiting
    await delay(API_DELAY);
  }
  
  return {
    totalRecords,
    totalVolume,
    totalPayment,
    processedPeriods,
    failedPeriods
  };
}

// Run reconciliation to update Bitcoin calculations
async function runReconciliation(): Promise<boolean> {
  return new Promise((resolve) => {
    log(`Running reconciliation for ${DATE}...`, "info");
    
    const reconciliation = spawn('npx', ['tsx', 'unified_reconciliation.ts', 'date', DATE]);
    
    reconciliation.stdout.on('data', (data) => {
      console.log(`${data}`);
    });
    
    reconciliation.stderr.on('data', (data) => {
      console.error(`${data}`);
    });
    
    reconciliation.on('close', (code) => {
      if (code === 0) {
        log(`Reconciliation completed successfully for ${DATE}`, "success");
        resolve(true);
      } else {
        log(`Reconciliation failed with code ${code}`, "error");
        resolve(false);
      }
    });
    
    // Add timeout to prevent hanging
    setTimeout(() => {
      log(`Reconciliation timed out after 60 seconds, continuing anyway`, "warning");
      resolve(false);
    }, 60000);
  });
}

// Get final statistics
async function getFinalStats(): Promise<{
  count: number;
  periods: number;
  volume: number;
  payment: number;
  missingPeriods: number[];
}> {
  try {
    // Get all records for this date
    const stats = await db.select({
      count: db.sql<number>`count(*)`,
      totalVolume: db.sql<number>`SUM(ABS(CAST(${curtailmentRecords.volume} AS DECIMAL)))`,
      totalPayment: db.sql<number>`SUM(CAST(${curtailmentRecords.payment} AS DECIMAL))`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE));
    
    // Get count of unique periods
    const periodCounts = await db.select({
      period: curtailmentRecords.settlementPeriod,
      count: db.sql<number>`count(*)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE))
    .groupBy(curtailmentRecords.settlementPeriod);
    
    const existingPeriods = periodCounts.map(p => p.period);
    const missingPeriods: number[] = [];
    
    // Find missing periods
    for (let p = 1; p <= 48; p++) {
      if (!existingPeriods.includes(p)) {
        missingPeriods.push(p);
      }
    }
    
    return {
      count: Number(stats[0]?.count || 0),
      periods: existingPeriods.length,
      volume: Number(stats[0]?.totalVolume || 0),
      payment: Number(stats[0]?.totalPayment || 0),
      missingPeriods
    };
  } catch (error) {
    log(`Error getting final stats: ${error}`, "error");
    return {
      count: 0,
      periods: 0,
      volume: 0,
      payment: 0,
      missingPeriods: []
    };
  }
}

// Main function
async function main() {
  try {
    const startTime = performance.now();
    log(`Starting data update for ${DATE}`, "info");
    
    // Initialize log file
    await fs.writeFile(LOG_FILE, `=== ${DATE} Elexon Data Update ===\n`);
    
    // Load BMU mappings once
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Get initial state for comparison
    const initialStats = await getFinalStats();
    
    log(`Initial state: ${initialStats.count} records across ${initialStats.periods} periods`, "info");
    log(`Initial volume: ${initialStats.volume.toFixed(2)} MWh, payment: £${Math.abs(initialStats.payment).toFixed(2)}`, "info");
    
    // Identify missing periods
    const missingPeriods = initialStats.missingPeriods;
    
    if (missingPeriods.length > 0) {
      log(`Missing periods detected: ${missingPeriods.join(', ')}`, "warning");
    } else {
      log(`All 48 periods have some data`, "success");
    }
    
    // Process missing periods first (highest priority)
    if (missingPeriods.length > 0) {
      log(`Processing ${missingPeriods.length} missing periods...`, "info");
      
      const missingResults = await processPeriods(
        missingPeriods,
        windFarmIds,
        bmuLeadPartyMap
      );
      
      log(`Processed ${missingResults.processedPeriods} missing periods:`, "success");
      log(`- Added ${missingResults.totalRecords} records`, "info");
      log(`- Total volume: ${missingResults.totalVolume.toFixed(2)} MWh`, "info");
      log(`- Total payment: £${missingResults.totalPayment.toFixed(2)}`, "info");
      
      if (missingResults.failedPeriods.length > 0) {
        log(`Failed to process periods: ${missingResults.failedPeriods.join(', ')}`, "error");
      }
    }
    
    // Now check all periods to ensure they have the latest data
    // Focus on periods with low record counts first
    log(`Checking completeness of all periods...`, "info");
    
    const periodStats: Array<{ period: number; count: number }> = [];
    
    for (let period = 1; period <= 48; period++) {
      // Skip periods we just processed
      if (missingPeriods.includes(period)) continue;
      
      const existingRecords = await checkExistingRecords(period);
      periodStats.push({ period, count: existingRecords.count });
      
      // Add a short delay to avoid database overload
      await delay(50);
    }
    
    // Sort periods by record count (ascending) to prioritize periods with fewer records
    const periodsToUpdate = periodStats
      .sort((a, b) => a.count - b.count)
      .map(p => p.period);
    
    // Process in batches of 10 to avoid overwhelming the API
    const batchSize = 10;
    for (let i = 0; i < periodsToUpdate.length; i += batchSize) {
      const batch = periodsToUpdate.slice(i, i + batchSize);
      log(`Processing batch of ${batch.length} periods: ${batch.join(', ')}...`, "info");
      
      const batchResults = await processPeriods(
        batch,
        windFarmIds,
        bmuLeadPartyMap
      );
      
      log(`Processed batch:`, "success");
      log(`- Updated ${batchResults.totalRecords} records`, "info");
      log(`- Total volume: ${batchResults.totalVolume.toFixed(2)} MWh`, "info");
      log(`- Total payment: £${batchResults.totalPayment.toFixed(2)}`, "info");
      
      if (batchResults.failedPeriods.length > 0) {
        log(`Failed to process periods: ${batchResults.failedPeriods.join(', ')}`, "error");
      }
      
      // Add a longer delay between batches
      await delay(5000);
    }
    
    // Run reconciliation to update Bitcoin calculations
    log(`Running reconciliation to update Bitcoin calculations...`, "info");
    const reconciliationSuccess = await runReconciliation();
    
    // Get final stats
    const finalStats = await getFinalStats();
    
    log(`=== Final statistics for ${DATE} ===`, "success");
    log(`- Total records: ${finalStats.count}`, "info");
    log(`- Total periods: ${finalStats.periods}/48`, "info");
    log(`- Total volume: ${finalStats.volume.toFixed(2)} MWh`, "info");
    log(`- Total payment: £${Math.abs(finalStats.payment).toFixed(2)}`, "info");
    
    if (finalStats.missingPeriods.length > 0) {
      log(`Still missing ${finalStats.missingPeriods.length} periods: ${finalStats.missingPeriods.join(', ')}`, "warning");
    } else {
      log(`All 48 periods are now present in the database`, "success");
    }
    
    const endTime = performance.now();
    const duration = ((endTime - startTime) / 1000).toFixed(1);
    
    log(`=== Process completed in ${duration}s ===`, "success");
    
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