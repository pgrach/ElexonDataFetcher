/**
 * Reprocess May 4th, 2025 Data
 * 
 * This script reprocesses all data for May 4th, 2025 with improved
 * BMU mapping and filtering logic to detect curtailment.
 * 
 * Usage:
 *   npx tsx scripts/reprocess-may-4th.ts
 */

import axios from 'axios';
import * as path from 'path';
import * as fs from 'fs/promises';
import pLimit from 'p-limit';
import { format } from 'date-fns';
import { db } from '../db';
import { curtailmentRecords, insertCurtailmentRecordSchema } from '../db/schema';
import { eq, and, sql } from 'drizzle-orm';
import { getAvgPrice } from '../server/services/curtailment_enhanced';
import { calculateBitcoinPotential } from '../server/utils/bitcoin';

// Constants
const DATE_TO_PROCESS = '2025-05-04';
const API_BASE_URL = 'https://data.bmreports.com/bmrs/api/v1/datasets';
const BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'bmuMapping.json');
const CONCURRENCY_LIMIT = 3;

// Initialize the concurrency limiter
const limit = pLimit(CONCURRENCY_LIMIT);

/**
 * Log with timestamps
 */
function log(message: string, data?: any) {
  const timestamp = new Date().toISOString();
  if (data) {
    console.log(`[${timestamp}] ${message}`, data);
  } else {
    console.log(`[${timestamp}] ${message}`);
  }
}

/**
 * Get a set of wind farm IDs from BMU mapping file
 */
async function getUnifiedWindFarmIds(): Promise<Set<string>> {
  try {
    // Load BMU mappings
    const bmuMappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(bmuMappingContent);
    
    // Filter for wind farms only
    const windFarmIds = new Set<string>();
    for (const mapping of bmuMapping) {
      if (mapping.fuelType === 'WIND') {
        windFarmIds.add(mapping.elexonBmUnit);
      }
    }
    
    log(`Found ${windFarmIds.size} unique wind farm BMUs`);
    return windFarmIds;
  } catch (error) {
    console.error('Error loading BMU mappings:', error);
    throw error;
  }
}

/**
 * Make API request with retries
 */
async function makeRequest(url: string, date: string, period: number): Promise<any> {
  try {
    const response = await axios.get(url);
    return response.data;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(`API request failed for ${date} period ${period}: ${errorMessage}`);
    
    if (axios.isAxiosError(error) && error.response && error.response.status === 429) {
      // Rate limit hit, wait and retry
      log(`Rate limit hit, waiting 2 seconds before retry for period ${period}`);
      await new Promise(resolve => setTimeout(resolve, 2000));
      return makeRequest(url, date, period);
    }
    
    return [];
  }
}

/**
 * Fetch bids and offers for a specific period with correct filtering
 */
async function fetchBidsOffers(date: string, period: number): Promise<any[]> {
  const url = `${API_BASE_URL}/BOD/stream?from=${date}&to=${date}&settlementPeriodFrom=${period}&settlementPeriodTo=${period}`;
  log(`Fetching BOD data for ${date} period ${period}`);
  
  const data = await makeRequest(url, date, period);
  if (!Array.isArray(data)) {
    log(`No valid data returned for ${date} period ${period}`);
    return [];
  }
  
  log(`Retrieved ${data.length} BOD records for period ${period}`);
  return data;
}

/**
 * Process curtailment data for a specific date
 */
async function processCurtailmentData(date: string): Promise<number> {
  try {
    log(`Processing curtailment data for ${date}`);
    
    // Get a set of wind farm BMUs
    const windFarmIds = await getUnifiedWindFarmIds();
    log(`Using ${windFarmIds.size} wind farm BMUs from mapping file`);
    
    // Clear existing records for this date
    const deleteResult = await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    log(`Cleared ${deleteResult.rowCount} existing records for ${date}`);
    
    // Process all 48 settlement periods
    const periodPromises = [];
    const curtailmentResults: any[] = [];
    
    for (let period = 1; period <= 48; period++) {
      periodPromises.push(limit(async () => {
        // Get bids and offers data for the period
        const bodRecords = await fetchBidsOffers(date, period);
        
        if (bodRecords.length === 0) {
          log(`No BOD records for ${date} period ${period}`);
          return;
        }
        
        log(`Processing ${bodRecords.length} records for period ${period}`);
        
        // Filter curtailment records
        const curtailmentItems = bodRecords.filter((record: any) => {
          // Filter for wind farms
          if (!windFarmIds.has(record.bmUnit)) {
            return false;
          }
          
          // Apply curtailment criteria: 
          // 1. Volume must be negative AND
          // 2. Either soFlag OR cadlFlag must be true
          const isNegativeVolume = record.originalVolume < 0;
          const isCurtailed = record.soFlag || record.cadlFlag;
          
          return isNegativeVolume && isCurtailed;
        });
        
        log(`Found ${curtailmentItems.length} curtailment records for period ${period}`);
        
        // Process each curtailment record
        for (const item of curtailmentItems) {
          const avgPrice = await getAvgPrice(item.originalClearedPriceInGbp);
          const volume = Math.abs(item.originalVolume); // Convert to positive for our records
          const payment = volume * avgPrice;
          
          // Calculate potential Bitcoin mining
          const bitcoinData = await calculateBitcoinPotential(volume, date, 'S19J_PRO');
          
          const record = {
            settlementDate: date,
            settlementPeriod: period,
            farmId: item.bmUnit,
            leadPartyName: item.companyName || 'Unknown',
            volume,
            price: avgPrice,
            payment,
            soFlag: item.soFlag,
            cadlFlag: item.cadlFlag,
            bitcoinMined: bitcoinData.bitcoinMined,
            bitcoinValue: bitcoinData.valueAtCurrentPrice,
            bitcoinDifficulty: bitcoinData.difficulty,
            bitcoinPrice: bitcoinData.currentPrice,
            timeFrom: item.timeFrom || `Period ${period}`,
            timeTo: item.timeTo || `Period ${period}`,
            processingTime: new Date()
          };
          
          // Validate the record
          const validatedRecord = insertCurtailmentRecordSchema.parse(record);
          curtailmentResults.push(validatedRecord);
        }
      }));
    }
    
    // Wait for all periods to be processed
    await Promise.all(periodPromises);
    log(`Completed processing for all periods`);
    
    // Batch insert all records
    if (curtailmentResults.length > 0) {
      const insertResult = await db.insert(curtailmentRecords).values(curtailmentResults);
      log(`Inserted ${curtailmentResults.length} new curtailment records`);
      
      // Calculate summary
      const totalVolume = curtailmentResults.reduce((sum, record) => sum + record.volume, 0);
      const totalPayment = curtailmentResults.reduce((sum, record) => sum + record.payment, 0);
      const affectedPeriods = new Set(curtailmentResults.map(record => record.settlementPeriod)).size;
      
      log(`Summary for ${date}:`);
      log(`  Records: ${curtailmentResults.length}`);
      log(`  Affected Periods: ${affectedPeriods}`);
      log(`  Total Volume: ${totalVolume.toFixed(2)} MWh`);
      log(`  Total Payment: £${totalPayment.toFixed(2)}`);
      
      return curtailmentResults.length;
    } else {
      log(`No curtailment records found for ${date}`);
      return 0;
    }
    
  } catch (error) {
    console.error(`Error processing curtailment data for ${date}:`, error);
    throw error;
  }
}

/**
 * Main function to reprocess data for May 4th, 2025
 */
async function reprocessMayFourth() {
  try {
    console.log("=== Starting May 4th, 2025 Data Reprocessing ===");
    const startTime = new Date();
    
    // Process the data
    const recordCount = await processCurtailmentData(DATE_TO_PROCESS);
    
    const endTime = new Date();
    const duration = (endTime.getTime() - startTime.getTime()) / 1000;
    
    console.log(`\n=== Reprocessing Completed ===`);
    console.log(`Date: ${DATE_TO_PROCESS}`);
    console.log(`Records Processed: ${recordCount}`);
    console.log(`Duration: ${duration.toFixed(2)} seconds`);
    console.log(`Completed at: ${endTime.toISOString()}`);
    
  } catch (error) {
    console.error("Error during reprocessing:", error);
    process.exit(1);
  }
}

// Run the script
reprocessMayFourth();