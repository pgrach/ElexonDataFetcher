/**
 * 2025-03-18 Batch Data Processor
 * 
 * This script processes a specific batch of periods for 2025-03-18
 * to handle the API rate limits and timeout issues.
 * 
 * Usage:
 *   npx tsx process_2025_03_18_batch.ts [start_period] [end_period]
 * 
 * Example:
 *   npx tsx process_2025_03_18_batch.ts 1 10
 *   npx tsx process_2025_03_18_batch.ts 11 20
 */

import { db } from './db';
import { and, between, eq, sql } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

// Set up ES Module compatible dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const TARGET_DATE = '2025-03-18';
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BMU_MAPPING_PATH = path.join(__dirname, 'server', 'data', 'bmuMapping.json');
const MAX_RETRIES = 3;
const RETRY_DELAY = 3000; // 3 seconds

// Parse command line arguments
const [startPeriodArg, endPeriodArg] = process.argv.slice(2);
const startPeriod = startPeriodArg ? parseInt(startPeriodArg, 10) : 1;
const endPeriod = endPeriodArg ? parseInt(endPeriodArg, 10) : 10;

// Utility function to delay execution
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mappings
async function loadBmuMappings(): Promise<{
  windFarmIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}> {
  console.log(`Loading BMU mapping from: ${BMU_MAPPING_PATH}`);
  
  try {
    const data = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(data);
    
    const windFarmIds = new Set<string>();
    const bmuLeadPartyMap = new Map<string, string>();
    
    for (const bmu of bmuMapping) {
      if (bmu.fuelType === "WIND") {
        windFarmIds.add(bmu.elexonBmUnit);
        bmuLeadPartyMap.set(bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown');
      }
    }
    
    console.log(`Found ${windFarmIds.size} wind farm BMUs`);
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    console.error(`Error loading BMU mapping: ${error}`);
    throw error;
  }
}

// Fetch data from Elexon API
async function fetchBidsOffers(period: number, attempt = 1): Promise<any[]> {
  try {
    console.log(`Fetching data for ${TARGET_DATE} period ${period} (attempt ${attempt})`);
    
    // Make parallel requests for bids and offers
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${TARGET_DATE}/${period}`, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000 // 30 second timeout
      }),
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${TARGET_DATE}/${period}`, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000 // 30 second timeout
      })
    ]);
    
    if (!bidsResponse.data?.data || !offersResponse.data?.data) {
      console.error(`[${TARGET_DATE} P${period}] Invalid API response format`);
      return [];
    }
    
    return [...(bidsResponse.data.data || []), ...(offersResponse.data.data || [])];
  } catch (error) {
    console.error(`Error fetching data for period ${period}:`, error.message || error);
    
    if (attempt < MAX_RETRIES) {
      console.log(`Retrying in ${RETRY_DELAY/1000} seconds... (${attempt}/${MAX_RETRIES})`);
      await delay(RETRY_DELAY);
      return fetchBidsOffers(period, attempt + 1);
    }
    
    console.error(`Failed to fetch data after ${MAX_RETRIES} attempts.`);
    return [];
  }
}

// Process a single settlement period
async function processPeriod(period: number, windFarmIds: Set<string>, bmuLeadPartyMap: Map<string, string>): Promise<{
  success: boolean;
  records: number;
  volume: number;
  payment: number;
}> {
  try {
    // Fetch data from the Elexon API
    const allRecords = await fetchBidsOffers(period);
    
    // Filter to keep only valid wind farm records with negative volume (curtailment)
    const validRecords = allRecords.filter(record => 
      windFarmIds.has(record.id) && 
      record.volume < 0 && 
      (record.soFlag || record.cadlFlag)
    );
    
    const totalVolume = validRecords.reduce((sum, record) => sum + Math.abs(record.volume), 0);
    const totalPayment = validRecords.reduce((sum, record) => sum + (Math.abs(record.volume) * record.originalPrice), 0);
    
    console.log(`[${TARGET_DATE} P${period}] Records: ${validRecords.length} (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`);
    
    // Skip if no valid records
    if (validRecords.length === 0) {
      console.log(`[${TARGET_DATE} P${period}] No valid records found`);
      return { 
        success: true, 
        records: 0,
        volume: 0,
        payment: 0
      };
    }
    
    // Clear any existing records for this period to prevent duplicates
    await db.delete(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.settlementPeriod, period)
        )
      );
    
    console.log(`[${TARGET_DATE} P${period}] Processing ${validRecords.length} records`);
    
    // Insert records for this period
    for (const record of validRecords) {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice;
      
      try {
        await db.insert(curtailmentRecords).values({
          settlementDate: TARGET_DATE,
          settlementPeriod: period,
          farmId: record.id,
          leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
          volume: record.volume.toString(), // Keep the original negative value
          payment: payment.toString(),
          originalPrice: record.originalPrice.toString(),
          finalPrice: record.finalPrice.toString(),
          soFlag: record.soFlag,
          cadlFlag: record.cadlFlag
        });
        
        console.log(`[${TARGET_DATE} P${period}] Added record for ${record.id}: ${volume} MWh, £${payment}`);
      } catch (error) {
        console.error(`[${TARGET_DATE} P${period}] Error inserting record for ${record.id}:`, error);
      }
    }
    
    console.log(`[${TARGET_DATE} P${period}] Total: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    
    return { 
      success: true, 
      records: validRecords.length,
      volume: totalVolume,
      payment: totalPayment
    };
    
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return { 
      success: false, 
      records: 0,
      volume: 0,
      payment: 0
    };
  }
}

// Main function to process a batch of periods
async function processBatch(): Promise<void> {
  console.log(`=== Processing ${TARGET_DATE} periods ${startPeriod}-${endPeriod} ===`);
  
  try {
    // Load BMU mappings once
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    let processedPeriods = 0;
    
    // Process each period in sequence with delay to avoid rate limits
    for (let period = startPeriod; period <= endPeriod; period++) {
      const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
      
      if (result.success) {
        processedPeriods++;
        totalRecords += result.records;
        totalVolume += result.volume;
        totalPayment += result.payment;
      }
      
      // Add a delay between periods to avoid rate limiting
      if (period < endPeriod) {
        console.log(`Waiting 3 seconds before processing next period...`);
        await delay(3000);
      }
    }
    
    // Summary
    console.log(`\n=== Batch Summary ===`);
    console.log(`- Periods processed: ${processedPeriods}/${endPeriod - startPeriod + 1}`);
    console.log(`- Records added: ${totalRecords}`);
    console.log(`- Total volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`- Total payment: £${totalPayment.toFixed(2)}`);
    console.log(`Completed processing periods ${startPeriod}-${endPeriod} for ${TARGET_DATE}\n`);
    
  } catch (error) {
    console.error(`Error processing batch:`, error);
    process.exit(1);
  }
}

// Execute the batch process
processBatch().then(() => {
  console.log('Batch processing completed successfully');
  process.exit(0);
}).catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});