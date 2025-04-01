/**
 * Fetch March 28, 2025 Data - Part 3 (Periods 25-36)
 * 
 * This script:
 * 1. Fetches data from Elexon API for 2025-03-28 for periods 25-36
 * 2. Uses both bid and offer endpoints to ensure complete data
 * 3. Stores the data in the curtailment_records table
 */

import { db } from './db';
import { and, eq, sql } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import axios from 'axios';
import * as fs from 'fs';
import * as path from 'path';

// Configuration
const DATE_TO_FETCH = '2025-03-28';
const START_PERIOD = 25;
const END_PERIOD = 36;
const LOG_FILE = `fetch_march28_part3.log`;
const ELEXON_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'bmuMapping.json');

// Helper function to log to file
function log(message: string, level: "info" | "error" | "warning" | "success" = "info"): void {
  const timestamp = new Date().toISOString().replace('T', ' ').split('.')[0];
  const logMessage = `[${timestamp}] ${message}\n`;
  fs.appendFileSync(LOG_FILE, logMessage);
  
  // Also log to console with colors for better visibility
  const colors = {
    info: '\x1b[36m', // Cyan
    error: '\x1b[31m', // Red
    warning: '\x1b[33m', // Yellow
    success: '\x1b[32m', // Green
    reset: '\x1b[0m' // Reset
  };
  
  console.log(`${colors[level]}${message}${colors.reset}`);
}

// Helper function for delays
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function loadBmuMappings(): Promise<{
  windFarmIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}> {
  try {
    log(`Loading BMU mapping from: ${BMU_MAPPING_PATH}`, "info");
    const mappingContent = fs.readFileSync(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    
    const windFarmIds = new Set(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => bmu.elexonBmUnit)
    );
    
    const bmuLeadPartyMap = new Map(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => [bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown'])
    );
    
    log(`Found ${windFarmIds.size} wind farm BMUs`, "success");
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    log(`Error loading BMU mapping: ${error}`, "error");
    throw error;
  }
}

async function fetchPeriodData(period: number, windFarmIds: Set<string>): Promise<any[]> {
  try {
    // Make parallel requests for both bids and offers to ensure complete data
    const bidUrl = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${DATE_TO_FETCH}/${period}`;
    const offerUrl = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${DATE_TO_FETCH}/${period}`;
    
    log(`[P${period}] Fetching data from Elexon API...`, "info");
    
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get(bidUrl),
      axios.get(offerUrl)
    ]).catch(error => {
      log(`[P${period}] API error: ${error.message}`, "warning");
      return [{ data: { data: [] } }, { data: { data: [] } }];
    });
    
    const bidsData = bidsResponse.data?.data || [];
    const offersData = offersResponse.data?.data || [];
    
    log(`[P${period}] Retrieved ${bidsData.length} bids and ${offersData.length} offers`, "info");
    
    // Filter to keep only valid wind farm records
    const validBids = bidsData.filter((record: any) => 
      record.volume < 0 && 
      (record.soFlag || record.cadlFlag) && 
      windFarmIds.has(record.id)
    );
    
    const validOffers = offersData.filter((record: any) => 
      record.volume < 0 && 
      (record.soFlag || record.cadlFlag) && 
      windFarmIds.has(record.id)
    );
    
    // Combine all valid records
    const validRecords = [...validBids, ...validOffers];
    
    // Log period stats
    const periodVolume = validRecords.reduce((sum: number, record: any) => sum + Math.abs(record.volume), 0);
    const periodPayment = validRecords.reduce((sum: number, record: any) => sum + (Math.abs(record.volume) * record.originalPrice), 0);
    
    log(`[P${period}] Valid records: ${validRecords.length} (${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`, 
      validRecords.length > 0 ? "success" : "warning");
    
    return validRecords;
  } catch (error) {
    log(`[P${period}] Error fetching data: ${error}`, "error");
    throw error;
  }
}

async function processPeriod(
  period: number, 
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  success: boolean;
  records: number;
  volume: number;
  payment: number;
}> {
  try {
    // Fetch data for this period
    const validRecords = await fetchPeriodData(period, windFarmIds);
    
    // Clear existing records for this period to avoid duplicates
    try {
      await db.delete(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, DATE_TO_FETCH),
            eq(curtailmentRecords.settlementPeriod, period)
          )
        );
      
      log(`[P${period}] Cleared existing records before insertion`, "info");
    } catch (error) {
      log(`[P${period}] Error clearing existing records: ${error}`, "error");
      return { success: false, records: 0, volume: 0, payment: 0 };
    }
    
    // Track totals for reporting
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Create records for insertion
    const recordsToInsert = validRecords.map((record: any) => {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice;
      
      // Track totals
      totalVolume += volume;
      totalPayment += payment;
      
      return {
        settlementDate: DATE_TO_FETCH,
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
    
    // Insert records into the database
    if (recordsToInsert.length > 0) {
      try {
        await db.insert(curtailmentRecords).values(recordsToInsert);
        log(`[P${period}] Successfully inserted ${recordsToInsert.length} records`, "success");
      } catch (error) {
        log(`[P${period}] Error inserting records: ${error}`, "error");
        return { success: false, records: 0, volume: 0, payment: 0 };
      }
    } else {
      log(`[P${period}] No valid records to insert`, "warning");
    }
    
    return {
      success: true,
      records: recordsToInsert.length,
      volume: totalVolume,
      payment: totalPayment
    };
  } catch (error) {
    log(`[P${period}] Error processing period: ${error}`, "error");
    return { success: false, records: 0, volume: 0, payment: 0 };
  }
}

async function processPeriods(): Promise<void> {
  try {
    // Load wind farm mappings
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    log(`Starting fetch and update process for ${DATE_TO_FETCH} periods ${START_PERIOD}-${END_PERIOD}`, "info");
    
    // Process all the specified settlement periods
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    let failedPeriods = 0;
    
    // Process periods sequentially to avoid rate limiting
    for (let period = START_PERIOD; period <= END_PERIOD; period++) {
      log(`Processing period ${period}`, "info");
      
      const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
      
      if (result.success) {
        totalRecords += result.records;
        totalVolume += result.volume;
        totalPayment += result.payment;
      } else {
        failedPeriods++;
      }
      
      // Brief pause between periods
      await delay(2000);
    }
    
    log(`\nFetch and process complete:`, "success");
    log(`Total records processed: ${totalRecords}`, "success");
    log(`Total volume: ${totalVolume.toFixed(2)} MWh`, "success");
    log(`Total payment: £${totalPayment.toFixed(2)}`, "success");
    
    if (failedPeriods > 0) {
      log(`Failed periods: ${failedPeriods}`, "warning");
    }
    
  } catch (error) {
    log(`Error processing periods: ${error}`, "error");
    throw error;
  }
}

// Run the process
(async () => {
  log(`Starting fetch and update script for ${DATE_TO_FETCH} periods ${START_PERIOD}-${END_PERIOD}\n`, "info");
  
  try {
    await processPeriods();
    log(`\nScript completed successfully!`, "success");
  } catch (error) {
    log(`\nScript failed: ${error}`, "error");
    process.exit(1);
  }
})();