/**
 * Fix Single Settlement Period
 * 
 * This script targets a single settlement period to add missing data by including
 * both soFlag and cadlFlag records.
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TARGET_DATE = '2025-03-28';
const TARGET_PERIOD = parseInt(process.argv[2] || '33', 10);
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");

// Utility function to delay between API calls
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mapping to get valid wind farm IDs
async function loadBmuMappings(): Promise<{
  windFarmIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}> {
  try {
    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    
    const windFarmIds = new Set<string>(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => bmu.elexonBmUnit)
    );
    
    const bmuLeadPartyMap = new Map<string, string>();
    for (const bmu of bmuMapping.filter((bmu: any) => bmu.fuelType === "WIND")) {
      bmuLeadPartyMap.set(bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown');
    }
    
    console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

// Make request to Elexon API
async function makeElexonRequest(url: string): Promise<any> {
  try {
    console.log(`Requesting ${url}`);
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000 // 30 second timeout
    });
    return response;
  } catch (error) {
    if (axios.isAxiosError(error) && error.response?.status === 429) {
      console.log(`Rate limited, retrying after delay...`);
      await delay(60000); // Wait 1 minute on rate limit
      return makeElexonRequest(url);
    }
    throw error;
  }
}

// Process the target period
async function processPeriod(): Promise<void> {
  console.log(`\nProcessing period ${TARGET_PERIOD} for ${TARGET_DATE}...`);
  
  try {
    // Load BMU mappings
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Make parallel requests for bids and offers
    const [bidsResponse, offersResponse] = await Promise.all([
      makeElexonRequest(
        `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${TARGET_DATE}/${TARGET_PERIOD}`
      ),
      makeElexonRequest(
        `${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${TARGET_DATE}/${TARGET_PERIOD}`
      )
    ]).catch(error => {
      console.error(`Error fetching data:`, error.message);
      return [{ data: { data: [] } }, { data: { data: [] } }];
    });

    if (!bidsResponse.data?.data || !offersResponse.data?.data) {
      console.error(`Invalid API response format`);
      return;
    }

    // Get raw records
    const allBids = bidsResponse.data.data || [];
    const allOffers = offersResponse.data.data || [];
    
    console.log(`Raw bids response: ${allBids.length} records`);
    console.log(`Raw offers response: ${allOffers.length} records`);
    
    // Filter for records that are:
    // 1. For wind farms (in the BMU mapping)
    // 2. Have negative volume (curtailment)
    // 3. Have either soFlag or cadlFlag true
    const validRecords = [...allBids, ...allOffers].filter(record =>
      record.volume < 0 && 
      (record.soFlag || record.cadlFlag) && 
      windFarmIds.has(record.id)
    );
    
    console.log(`Valid records: ${validRecords.length}`);
    
    // Get existing records for this period
    const existingRecords = await db
      .select({ farmId: curtailmentRecords.farmId })
      .from(curtailmentRecords)
      .where(and(
        eq(curtailmentRecords.settlementDate, TARGET_DATE),
        eq(curtailmentRecords.settlementPeriod, TARGET_PERIOD)
      ));
    
    const existingFarmIds = new Set(existingRecords.map(r => r.farmId));
    console.log(`Existing farm records: ${existingFarmIds.size}`);
    
    // Process records
    let totalVolume = 0;
    let totalPayment = 0;
    let addedCount = 0;
    
    for (const record of validRecords) {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice * -1;
      
      try {
        // Insert the record into the database
        // Use a custom query to check if the record already exists
        const existingRecord = await db
          .select({ id: curtailmentRecords.id })
          .from(curtailmentRecords)
          .where(and(
            eq(curtailmentRecords.settlementDate, TARGET_DATE),
            eq(curtailmentRecords.settlementPeriod, TARGET_PERIOD),
            eq(curtailmentRecords.farmId, record.id),
            eq(curtailmentRecords.soFlag, record.soFlag),
            eq(curtailmentRecords.cadlFlag, record.cadlFlag)
          ))
          .limit(1);
          
        if (existingRecord.length === 0) {
          // Record doesn't exist, insert it
          await db.insert(curtailmentRecords).values({
            settlementDate: TARGET_DATE,
            settlementPeriod: TARGET_PERIOD,
            farmId: record.id,
            leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
            volume: record.volume.toString(), // Keep original negative value
            payment: payment.toString(),
            originalPrice: record.originalPrice.toString(),
            finalPrice: record.finalPrice.toString(),
            soFlag: record.soFlag,
            cadlFlag: record.cadlFlag
          });
          
          // Track the added record
          totalVolume += volume;
          totalPayment += payment;
          addedCount++;
        }
      } catch (error) {
        console.error(`Error inserting record for ${record.id}:`, error);
      }
    }
    
    console.log(`Added ${addedCount} records`);
    console.log(`Total volume added: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment added: £${totalPayment.toFixed(2)}`);
    
    // Calculate period totals
    const periodTotals = await db
      .select({
        recordCount: sql`COUNT(*)`,
        totalVolume: sql`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(and(
        eq(curtailmentRecords.settlementDate, TARGET_DATE),
        eq(curtailmentRecords.settlementPeriod, TARGET_PERIOD)
      ));
    
    console.log(`\nFinal period ${TARGET_PERIOD} data:`);
    console.log(`- Records: ${periodTotals[0].recordCount}`);
    console.log(`- Volume: ${periodTotals[0].totalVolume} MWh`);
    console.log(`- Payment: £${periodTotals[0].totalPayment}`);
    
  } catch (error) {
    console.error('Error processing period:', error);
    throw error;
  }
}

// Main function
async function main(): Promise<void> {
  console.log(`=== Fixing Data for ${TARGET_DATE} Period ${TARGET_PERIOD} ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  
  try {
    await processPeriod();
    console.log(`\nCompleted at: ${new Date().toISOString()}`);
  } catch (error) {
    console.error('Unhandled error:', error);
    process.exit(1);
  }
}

// Execute main function
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});