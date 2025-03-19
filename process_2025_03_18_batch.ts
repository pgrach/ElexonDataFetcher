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
import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { eq, and, sql } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';

// Set up ES Module compatible dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const TARGET_DATE = '2025-03-18';
const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/stack/all';
const DELAY_MS = 3000; // Delay between API requests to avoid rate limiting

let startPeriod = 1;
let endPeriod = 5;

// Parse command line arguments
if (process.argv.length >= 4) {
  startPeriod = parseInt(process.argv[2], 10);
  endPeriod = parseInt(process.argv[3], 10);
}

// Validate input
if (isNaN(startPeriod) || isNaN(endPeriod) || startPeriod < 1 || endPeriod > 48 || startPeriod > endPeriod) {
  console.error('Invalid period range. Please provide valid period numbers between 1 and 48.');
  process.exit(1);
}

console.log(`Processing ${TARGET_DATE} periods ${startPeriod} to ${endPeriod}`);

// Helper function for delays
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mappings to identify wind farms
async function loadBmuMappings(): Promise<{
  windFarmIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}> {
  try {
    const mappingPath = path.join(__dirname, 'data', 'bmu_mapping.json');
    const mappingData = await fs.readFile(mappingPath, 'utf-8');
    const mappings = JSON.parse(mappingData);
    
    // Extract wind farm BMUs
    const windFarmIds = new Set<string>();
    const bmuLeadPartyMap = new Map<string, string>();
    
    for (const mapping of mappings) {
      if (mapping.fuelType === 'WIND') {
        windFarmIds.add(mapping.elexonBmUnit);
        bmuLeadPartyMap.set(mapping.elexonBmUnit, mapping.leadPartyName);
      }
    }
    
    console.log(`Loaded ${windFarmIds.size} wind farm BMUs from mapping file`);
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    console.error('Error loading BMU mappings:', error);
    // Return empty sets as fallback
    return { windFarmIds: new Set(), bmuLeadPartyMap: new Map() };
  }
}

// Fetch bids and offers from the API for a specific period
async function fetchBidsOffers(period: number, attempt = 1): Promise<any[]> {
  try {
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get(`${API_BASE_URL}/bid/${TARGET_DATE}/${period}`, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000  // 30 second timeout
      }),
      axios.get(`${API_BASE_URL}/offer/${TARGET_DATE}/${period}`, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000  // 30 second timeout
      })
    ]);
    
    const bidsData = bidsResponse.data?.data || [];
    const offersData = offersResponse.data?.data || [];
    
    return [...bidsData, ...offersData];
  } catch (error) {
    if (attempt < 3) {
      console.log(`API request failed for period ${period}, attempt ${attempt}. Retrying in 5 seconds...`);
      await delay(5000);  // Longer delay for retries
      return fetchBidsOffers(period, attempt + 1);
    }
    
    console.error(`Error fetching data for period ${period}:`, error.message || error);
    return [];  // Return empty array after max retries
  }
}

// Process a single settlement period
async function processPeriod(period: number, windFarmIds: Set<string>, bmuLeadPartyMap: Map<string, string>): Promise<{
  recordsFound: number;
  recordsAdded: number;
  totalVolume: number;
}> {
  console.log(`Processing period ${period}...`);
  
  // Check if we already have data for this period
  const existingRecords = await db.execute(sql`
    SELECT COUNT(*) as count 
    FROM curtailment_records 
    WHERE settlement_date = ${TARGET_DATE} AND settlement_period = ${period}
  `);
  
  const existingCount = parseInt(existingRecords[0]?.count || '0', 10);
  console.log(`Found ${existingCount} existing records for period ${period}`);
  
  // Fetch data from API
  const bidsOffersData = await fetchBidsOffers(period);
  console.log(`Retrieved ${bidsOffersData.length} total records from API for period ${period}`);
  
  // Filter for records with curtailment criteria:
  // 1. Must be a wind farm BMU from our mapping
  // 2. Volume must be negative (curtailment)
  // 3. Must be flagged as system operator (SO) or Bid-Offer Acceptance Data Logging (CADL)
  const curtailmentRecordsData = bidsOffersData.filter(record => 
    windFarmIds.has(record.bmUnit) && 
    record.volume < 0 && 
    (record.soFlag || record.cadlFlag)
  );
  
  console.log(`Identified ${curtailmentRecordsData.length} curtailment records for period ${period}`);
  
  if (curtailmentRecordsData.length === 0) {
    return { recordsFound: 0, recordsAdded: 0, totalVolume: 0 };
  }
  
  // Clear any existing records for this period to avoid duplicates
  if (existingCount > 0) {
    await db.delete(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.settlementPeriod, period)
        )
      );
    console.log(`Cleared ${existingCount} existing records for period ${period}`);
  }
  
  // Insert new records
  let totalVolume = 0;
  
  for (const record of curtailmentRecordsData) {
    const volume = Math.abs(record.volume);
    totalVolume += volume;
    
    await db.execute(sql`
      INSERT INTO curtailment_records (
        farm_id, 
        settlement_date, 
        settlement_period, 
        volume, 
        payment,
        lead_party_name,
        original_price,
        final_price
      ) VALUES (
        ${record.bmUnit},
        ${TARGET_DATE},
        ${period},
        ${record.volume},
        ${record.finalPrice * record.volume},
        ${bmuLeadPartyMap.get(record.bmUnit) || record.leadPartyName || 'Unknown'},
        ${record.originalPrice || 0},
        ${record.finalPrice || 0}
      )
    `);
  }
  
  console.log(`Inserted ${curtailmentRecordsData.length} records for period ${period}`);
  
  return {
    recordsFound: curtailmentRecordsData.length,
    recordsAdded: curtailmentRecordsData.length,
    totalVolume
  };
}

// Process the batch
async function processBatch(): Promise<void> {
  console.log(`Starting batch processing for ${TARGET_DATE}, periods ${startPeriod}-${endPeriod}`);
  
  // Load BMU mappings
  const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
  if (windFarmIds.size === 0) {
    console.error('No wind farm IDs found in mappings. Cannot continue.');
    process.exit(1);
  }
  
  const periodResults: Record<number, any> = {};
  let totalRecordsAdded = 0;
  let totalVolume = 0;
  
  // Process each period in the range
  for (let period = startPeriod; period <= endPeriod; period++) {
    const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
    periodResults[period] = result;
    
    totalRecordsAdded += result.recordsAdded;
    totalVolume += result.totalVolume;
    
    // Add delay between periods to avoid API rate limits
    if (period < endPeriod) {
      console.log(`Waiting ${DELAY_MS}ms before next period...`);
      await delay(DELAY_MS);
    }
  }
  
  // Print summary
  console.log('\nBatch Processing Summary:');
  console.log(`Date: ${TARGET_DATE}`);
  console.log(`Periods: ${startPeriod}-${endPeriod}`);
  console.log(`Total records added: ${totalRecordsAdded}`);
  console.log(`Total curtailed volume: ${totalVolume.toFixed(2)} MWh`);
  
  for (let period = startPeriod; period <= endPeriod; period++) {
    const result = periodResults[period];
    if (result) {
      console.log(`Period ${period}: ${result.recordsAdded} records, ${result.totalVolume.toFixed(2)} MWh`);
    }
  }
  
  console.log('\nBatch processing complete');
}

// Run the processor
processBatch().catch(error => {
  console.error('Error during batch processing:', error);
  process.exit(1);
});