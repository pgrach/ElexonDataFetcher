/**
 * Reprocess March 28 Data - Batch Processor
 * 
 * This script reprocesses a batch of settlement periods for March 28, 2025.
 * Usage: npx tsx reprocess_march_28_batch.ts [start_period] [end_period]
 * Example: npx tsx reprocess_march_28_batch.ts 1 12
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, sql } from "drizzle-orm";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TARGET_DATE = '2025-03-28';
const START_PERIOD = parseInt(process.argv[2] || '1', 10);
const END_PERIOD = parseInt(process.argv[3] || '12', 10);
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

// Fetch data from Elexon API for a specific period
async function fetchElexonData(period: number): Promise<any[]> {
  try {
    console.log(`Fetching data for period ${period}...`);
    
    // Make parallel requests for bids and offers
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${TARGET_DATE}/${period}`, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000
      }),
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${TARGET_DATE}/${period}`, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000
      })
    ]).catch(error => {
      console.error(`Error fetching data for period ${period}:`, error.message);
      return [{ data: { data: [] } }, { data: { data: [] } }];
    });
    
    // Combine and return the raw data
    return [
      ...(bidsResponse.data?.data || []), 
      ...(offersResponse.data?.data || [])
    ];
  } catch (error) {
    console.error(`Error fetching data for period ${period}:`, error);
    return [];
  }
}

// Process data for a specific period
async function processPeriod(
  period: number,
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  recordCount: number;
  totalVolume: number;
  totalPayment: number;
}> {
  try {
    // Clear existing records for this period
    await db.delete(curtailmentRecords)
      .where(
        sql`settlement_date = ${TARGET_DATE} AND settlement_period = ${period}`
      );
    
    // Fetch raw data from Elexon
    const rawRecords = await fetchElexonData(period);
    console.log(`Retrieved ${rawRecords.length} raw records for period ${period}`);
    
    // Filter for valid wind farm records with curtailment (negative volume)
    // Include both soFlag and cadlFlag records
    const validRecords = rawRecords.filter(record => 
      record.volume < 0 && 
      (record.soFlag || record.cadlFlag) && 
      windFarmIds.has(record.id)
    );
    
    console.log(`Found ${validRecords.length} valid curtailment records for period ${period}`);
    
    // Insert the records
    let recordCount = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (const record of validRecords) {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice * -1;
      
      try {
        await db.insert(curtailmentRecords).values({
          settlementDate: TARGET_DATE,
          settlementPeriod: period,
          farmId: record.id,
          leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
          volume: record.volume.toString(), // Keep original negative value
          payment: payment.toString(),
          originalPrice: record.originalPrice.toString(),
          finalPrice: record.finalPrice.toString(),
          soFlag: record.soFlag,
          cadlFlag: record.cadlFlag
        });
        
        recordCount++;
        totalVolume += volume;
        totalPayment += payment;
      } catch (error) {
        console.error(`Error inserting record for ${record.id}:`, error);
      }
    }
    
    console.log(`Processed ${recordCount} records for period ${period} (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`);
    
    return {
      recordCount,
      totalVolume,
      totalPayment
    };
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return {
      recordCount: 0,
      totalVolume: 0,
      totalPayment: 0
    };
  }
}

// Process batch of periods
async function processBatch(): Promise<{
  totalRecords: number;
  totalVolume: number;
  totalPayment: number;
}> {
  console.log(`\nProcessing periods ${START_PERIOD} to ${END_PERIOD} for ${TARGET_DATE}...`);
  
  try {
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process each period in the batch
    for (let period = START_PERIOD; period <= END_PERIOD; period++) {
      const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
      
      totalRecords += result.recordCount;
      totalVolume += result.totalVolume;
      totalPayment += result.totalPayment;
      
      // Add a short delay to avoid rate limits
      await delay(500);
    }
    
    console.log(`\nProcessed periods ${START_PERIOD}-${END_PERIOD}:`);
    console.log(`- Total Records: ${totalRecords}`);
    console.log(`- Total Volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`- Total Payment: £${totalPayment.toFixed(2)}`);
    
    return {
      totalRecords,
      totalVolume,
      totalPayment
    };
  } catch (error) {
    console.error('Error processing batch:', error);
    throw error;
  }
}

// Verify batch results
async function verifyBatch(): Promise<void> {
  try {
    // Check records for the batch periods
    const recordCount = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(curtailmentRecords)
      .where(
        sql`settlement_date = ${TARGET_DATE} AND settlement_period >= ${START_PERIOD} AND settlement_period <= ${END_PERIOD}`
      );
    
    // Check totals for the batch periods
    const totals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(
        sql`settlement_date = ${TARGET_DATE} AND settlement_period >= ${START_PERIOD} AND settlement_period <= ${END_PERIOD}`
      );
    
    console.log(`\nVerification for periods ${START_PERIOD}-${END_PERIOD}:`);
    console.log(`- Total Records: ${recordCount[0].count}`);
    console.log(`- Total Energy: ${totals[0].totalCurtailedEnergy || '0'} MWh`);
    console.log(`- Total Payment: £${totals[0].totalPayment || '0'}`);
    
    // Get overall totals for the date
    const overallTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nCurrent Overall Totals for ${TARGET_DATE}:`);
    console.log(`- Total Energy: ${overallTotals[0].totalCurtailedEnergy || '0'} MWh`);
    console.log(`- Total Payment: £${overallTotals[0].totalPayment || '0'}`);
  } catch (error) {
    console.error('Error verifying batch:', error);
    throw error;
  }
}

// Main function
async function main(): Promise<void> {
  console.log(`=== Reprocessing Batch for ${TARGET_DATE} (Periods ${START_PERIOD}-${END_PERIOD}) ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  
  try {
    // Process the batch
    await processBatch();
    
    // Verify the batch
    await verifyBatch();
    
    console.log(`\nBatch processing completed successfully at ${new Date().toISOString()}`);
  } catch (error) {
    console.error('Error during batch processing:', error);
    process.exit(1);
  }
}

// Execute main function
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});