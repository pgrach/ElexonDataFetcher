/**
 * Elexon Data Synchronization Script
 * 
 * This script checks Elexon API data against local database records for a specific date
 * and adds any missing curtailment records.
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, sql } from "drizzle-orm";
import { fetchBidsOffers } from "./server/services/elexon";
import { ElexonBidOffer } from "./server/types/elexon";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");

const DATE_TO_CHECK = "2025-03-03";
const TOTAL_PERIODS = 48; 

interface DatabaseRecord {
  settlementDate: Date;
  settlementPeriod: number;
  farmId: string;
  leadPartyName: string | null;
  volume: string;
  finalPrice: string;
}

// Cache for BMU mapping
let bmuLeadPartyMap: Map<string, string> | null = null;
let windFarmIds: Set<string> | null = null;

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

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

async function getExistingRecords(date: string): Promise<DatabaseRecord[]> {
  console.log(`Fetching existing records for ${date}...`);
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
    .where(eq(curtailmentRecords.settlementDate, new Date(date)));

    console.log(`Found ${dbRecords.length} existing records in database`);
    return dbRecords;
  } catch (error) {
    console.error('Error fetching existing records:', error);
    throw error;
  }
}

async function getElexonData(date: string): Promise<ElexonBidOffer[]> {
  console.log(`Fetching Elexon data for ${date} for all periods...`);
  
  const allData: ElexonBidOffer[] = [];
  
  // Process periods in batches to avoid overwhelming the API
  for (let period = 1; period <= TOTAL_PERIODS; period++) {
    console.log(`Fetching period ${period}...`);
    try {
      const periodData = await fetchBidsOffers(date, period);
      allData.push(...periodData);
      
      // Add a small delay between requests to be gentle on the API
      if (period < TOTAL_PERIODS) {
        await delay(500);
      }
    } catch (error) {
      console.error(`Error fetching period ${period}:`, error);
    }
  }
  
  console.log(`Retrieved ${allData.length} records from Elexon API`);
  return allData;
}

function createRecordKey(record: any, isElexon: boolean = false): string {
  if (isElexon) {
    // For Elexon records
    const date = typeof record.settlementDate === 'string' 
      ? record.settlementDate 
      : record.settlementDate.toISOString().split('T')[0];
    return `${date}_${record.settlementPeriod}_${record.id}_${record.volume}`;
  } else {
    // For database records
    const date = record.settlementDate.toISOString().split('T')[0];
    return `${date}_${record.settlementPeriod}_${record.farmId}_${record.volume}`;
  }
}

async function insertMissingRecords(elexonRecords: ElexonBidOffer[], dbRecords: DatabaseRecord[]): Promise<void> {
  // Load BMU mapping for lead party names
  const { bmuMap, farmIds } = await loadBmuMapping();
  
  // Create a map of existing DB records for easy lookup
  const dbRecordMap = new Map<string, DatabaseRecord>();
  dbRecords.forEach(record => {
    const key = createRecordKey(record);
    dbRecordMap.set(key, record);
  });
  
  // Find records in Elexon that don't exist in the DB
  const missingRecords: ElexonBidOffer[] = [];
  
  for (const elexonRecord of elexonRecords) {
    const key = createRecordKey(elexonRecord, true);
    if (!dbRecordMap.has(key) && farmIds.has(elexonRecord.id)) {
      missingRecords.push(elexonRecord);
    }
  }
  
  console.log(`Found ${missingRecords.length} records missing from database`);
  
  if (missingRecords.length === 0) {
    console.log("No missing records to insert.");
    return;
  }
  
  // Transform Elexon records to DB format
  const recordsToInsert = missingRecords.map(record => ({
    settlementDate: new Date(record.settlementDate),
    settlementPeriod: record.settlementPeriod,
    farmId: record.id, // BMU ID
    leadPartyName: bmuMap.get(record.id) || record.leadPartyName || 'Unknown',
    volume: record.volume.toString(),
    payment: (record.volume * record.originalPrice * -1).toString(), // Calculate payment based on volume and price
    originalPrice: record.originalPrice.toString(),
    finalPrice: record.finalPrice.toString(),
    soFlag: record.soFlag,
    cadlFlag: record.cadlFlag || false,
    createdAt: new Date()
  }));
  
  console.log("Inserting missing records...");
  
  // Insert records in batches to avoid overwhelming the DB
  const BATCH_SIZE = 50;
  for (let i = 0; i < recordsToInsert.length; i += BATCH_SIZE) {
    const batch = recordsToInsert.slice(i, i + BATCH_SIZE);
    try {
      await db.insert(curtailmentRecords).values(batch);
      console.log(`Inserted batch ${Math.floor(i / BATCH_SIZE) + 1} (${batch.length} records)`);
    } catch (error) {
      console.error(`Error inserting batch ${Math.floor(i / BATCH_SIZE) + 1}:`, error);
      console.error(error);
    }
  }
  
  console.log(`Inserted ${recordsToInsert.length} missing records`);
}

async function main(): Promise<void> {
  try {
    // Fetch existing records from the database
    const dbRecords = await getExistingRecords(DATE_TO_CHECK);
    
    // Fetch data from Elexon API
    const elexonRecords = await getElexonData(DATE_TO_CHECK);
    
    // Insert missing records
    await insertMissingRecords(elexonRecords, dbRecords);
    
    // Update daily summary
    const totalRecords = await db.select({
      totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, new Date(DATE_TO_CHECK)));

    console.log("Updated data summary for", DATE_TO_CHECK);
    console.log("Total volume:", totalRecords[0].totalVolume);
    console.log("Total payment:", totalRecords[0].totalPayment);
    
    console.log("Synchronization complete!");
  } catch (error) {
    console.error("Error during synchronization:", error);
  }
}

// Run the main function
main();