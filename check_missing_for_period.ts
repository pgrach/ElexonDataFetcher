/**
 * Check Missing Records for Specific Period
 * 
 * This script checks Elexon API data against database records for a specific date and period
 * and inserts any missing records.
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, and } from "drizzle-orm";
import { fetchBidsOffers } from "./server/services/elexon";
import { ElexonBidOffer } from "./server/types/elexon";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");

// Get command line arguments
const DATE_TO_CHECK = process.argv[2] || "2025-03-03";
const START_PERIOD = parseInt(process.argv[3] || "1", 10);
const END_PERIOD = parseInt(process.argv[4] || "48", 10);

// Load BMU mapping
let bmuLeadPartyMap: Map<string, string> | null = null;
let windFarmIds: Set<string> | null = null;

/**
 * Load BMU mapping from the mapping file
 */
async function loadBmuMapping(): Promise<{ bmuMap: Map<string, string>, farmIds: Set<string> }> {
  if (bmuLeadPartyMap !== null && windFarmIds !== null) {
    return { bmuMap: bmuLeadPartyMap, farmIds: windFarmIds };
  }

  try {
    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    console.log(`Loaded ${bmuMapping.length} BMU mappings`);

    windFarmIds = new Set(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => bmu.elexonBmUnit)
    );

    bmuLeadPartyMap = new Map(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => [bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown'])
    );

    console.log(`Found ${windFarmIds.size} wind farm BMUs`);
    return { bmuMap: bmuLeadPartyMap, farmIds: windFarmIds };
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

/**
 * Get existing records for the specified date and period
 */
async function getExistingRecords(date: string, period: number): Promise<Map<string, any>> {
  console.log(`Fetching existing records for ${date} period ${period}...`);
  try {
    const dbRecords = await db.select({
      settlementDate: curtailmentRecords.settlementDate,
      settlementPeriod: curtailmentRecords.settlementPeriod,
      farmId: curtailmentRecords.farmId,
      leadPartyName: curtailmentRecords.leadPartyName,
      volume: curtailmentRecords.volume,
      finalPrice: curtailmentRecords.finalPrice
    })
    .from(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, new Date(date)),
        eq(curtailmentRecords.settlementPeriod, period)
      )
    );

    console.log(`Found ${dbRecords.length} existing records in database for period ${period}`);
    
    // Create a map for quick lookup
    const recordMap = new Map<string, any>();
    dbRecords.forEach(record => {
      const key = `${record.farmId}_${record.volume}`;
      recordMap.set(key, record);
    });
    
    return recordMap;
  } catch (error) {
    console.error(`Error fetching existing records for period ${period}:`, error);
    throw error;
  }
}

/**
 * Process a specific period and add missing records
 */
async function processPeriod(date: string, period: number): Promise<void> {
  try {
    // Load BMU mapping
    const { bmuMap, farmIds } = await loadBmuMapping();
    
    // Get existing records for this period
    const existingRecords = await getExistingRecords(date, period);
    
    // Fetch data from Elexon API for this period
    console.log(`Fetching Elexon data for ${date} period ${period}...`);
    const elexonRecords = await fetchBidsOffers(date, period);
    console.log(`Retrieved ${elexonRecords.length} records from Elexon API for period ${period}`);
    
    // Find missing records
    const missingRecords: ElexonBidOffer[] = [];
    for (const record of elexonRecords) {
      const key = `${record.id}_${record.volume}`;
      if (!existingRecords.has(key) && farmIds.has(record.id)) {
        missingRecords.push(record);
      }
    }
    
    console.log(`Found ${missingRecords.length} missing records for period ${period}`);
    
    if (missingRecords.length === 0) {
      return;
    }
    
    // Transform to DB format and insert
    const recordsToInsert = missingRecords.map(record => ({
      settlementDate: new Date(record.settlementDate),
      settlementPeriod: record.settlementPeriod,
      farmId: record.id,
      leadPartyName: bmuMap.get(record.id) || record.leadPartyName || 'Unknown',
      volume: record.volume.toString(),
      payment: (record.volume * record.originalPrice * -1).toString(),
      originalPrice: record.originalPrice.toString(),
      finalPrice: record.finalPrice.toString(),
      soFlag: record.soFlag,
      cadlFlag: record.cadlFlag || false,
      createdAt: new Date()
    }));
    
    // Insert missing records
    await db.insert(curtailmentRecords).values(recordsToInsert);
    console.log(`Inserted ${recordsToInsert.length} missing records for period ${period}`);
    
    // Log some sample records for verification
    if (recordsToInsert.length > 0) {
      console.log('Sample of inserted records:');
      const sampleSize = Math.min(3, recordsToInsert.length);
      for (let i = 0; i < sampleSize; i++) {
        const record = recordsToInsert[i];
        console.log(`  - ${record.farmId}, volume: ${record.volume}, price: ${record.finalPrice}`);
      }
    }
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
  }
}

/**
 * Process multiple periods in sequence
 */
async function main(): Promise<void> {
  console.log(`Processing periods ${START_PERIOD} to ${END_PERIOD} for ${DATE_TO_CHECK}`);
  
  for (let period = START_PERIOD; period <= END_PERIOD; period++) {
    console.log(`\n=== Processing period ${period} ===`);
    await processPeriod(DATE_TO_CHECK, period);
  }
  
  console.log(`\nCompleted processing periods ${START_PERIOD} to ${END_PERIOD}`);
}

// Run the script
main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error('Unhandled error:', error);
    process.exit(1);
  });