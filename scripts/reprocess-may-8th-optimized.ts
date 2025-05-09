/**
 * Optimized Reprocessing Script for 2025-05-08
 * 
 * This script focuses on efficiently processing May 8th data, with targeted
 * period checks to reduce processing time.
 * 
 * Based on our analysis, periods 27-29 contain curtailment records.
 * 
 * Usage:
 *   npx tsx scripts/reprocess-may-8th-optimized.ts
 */

import { db } from "../db";
import { 
  curtailmentRecords, 
  historicalBitcoinCalculations,
  dailySummaries
} from "../db/schema";
import { processWindDataForDate } from "../server/services/windDataUpdater";
import { processSingleDay } from "../server/services/bitcoinService";
import { eq, and, sql } from "drizzle-orm";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Constants
const TARGET_DATE = '2025-05-08';
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const SERVER_BMU_MAPPING_PATH = path.join(__dirname, "../server/data/bmuMapping.json");
const DATA_BMU_MAPPING_PATH = path.join(__dirname, "../server/data/bmuMapping.json");

// Based on our debug findings, we'll focus on these periods
const TARGETED_PERIODS = [27, 28, 29, 30];  // Add a few adjacent periods to be safe

// Create a unified set of wind farm BMU IDs from both mapping files
async function getUnifiedWindFarmIds(): Promise<Set<string>> {
  console.log("Loading BMU mappings from multiple sources...");
  
  try {
    // Load server BMU mapping
    console.log(`Reading from ${SERVER_BMU_MAPPING_PATH}`);
    const serverMappingContent = await fs.readFile(SERVER_BMU_MAPPING_PATH, 'utf8');
    const serverBmuMapping = JSON.parse(serverMappingContent);
    const serverWindFarmIds = new Set(
      serverBmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => bmu.elexonBmUnit)
    );
    console.log(`Found ${serverWindFarmIds.size} wind farm BMUs in server mapping`);
    
    // Load data BMU mapping (if exists)
    let dataWindFarmIds = new Set<string>();
    try {
      console.log(`Reading from ${DATA_BMU_MAPPING_PATH}`);
      const dataMappingContent = await fs.readFile(DATA_BMU_MAPPING_PATH, 'utf8');
      const dataBmuMapping = JSON.parse(dataMappingContent);
      dataWindFarmIds = new Set(
        dataBmuMapping
          .filter((bmu: any) => bmu.fuelType === "WIND")
          .map((bmu: any) => bmu.elexonBmUnit)
      );
      console.log(`Found ${dataWindFarmIds.size} wind farm BMUs in data mapping`);
    } catch (error) {
      console.log(`Data BMU mapping not found or invalid, using only server mapping`);
    }
    
    // Combine both sets
    const unifiedWindFarmIds = new Set([...serverWindFarmIds, ...dataWindFarmIds]);
    console.log(`Created unified set with ${unifiedWindFarmIds.size} unique wind farm BMUs`);
    
    return unifiedWindFarmIds;
  } catch (error) {
    console.error(`Error loading BMU mappings:`, error);
    throw new Error(`Failed to load BMU mappings: ${error.message}`);
  }
}

// Also create a mapping of BMU IDs to lead party names
async function getLeadPartyMapping(): Promise<Map<string, string>> {
  try {
    const leadPartyMap = new Map<string, string>();
    
    // Load from server BMU mapping
    const serverMappingContent = await fs.readFile(SERVER_BMU_MAPPING_PATH, 'utf8');
    const serverBmuMapping = JSON.parse(serverMappingContent);
    
    serverBmuMapping
      .filter((bmu: any) => bmu.fuelType === "WIND" && bmu.leadPartyName)
      .forEach((bmu: any) => {
        leadPartyMap.set(bmu.elexonBmUnit, bmu.leadPartyName);
      });
    
    // Try to supplement with data BMU mapping if it exists
    try {
      const dataMappingContent = await fs.readFile(DATA_BMU_MAPPING_PATH, 'utf8');
      const dataBmuMapping = JSON.parse(dataMappingContent);
      
      dataBmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND" && bmu.leadPartyName)
        .forEach((bmu: any) => {
          if (!leadPartyMap.has(bmu.elexonBmUnit)) {
            leadPartyMap.set(bmu.elexonBmUnit, bmu.leadPartyName);
          }
        });
    } catch (error) {
      // Just continue with server mapping
    }
    
    return leadPartyMap;
  } catch (error) {
    console.error(`Error creating lead party mapping:`, error);
    return new Map(); // Return empty map as fallback
  }
}

/**
 * Make API request with retries and rate limiting
 */
async function makeRequest(url: string, date: string, period: number): Promise<any> {
  const MAX_RETRIES = 3;
  let retries = 0;
  
  while (retries < MAX_RETRIES) {
    try {
      console.log(`[${date} P${period}] Making API request to: ${url}`);
      const response = await axios.get(url, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000 // 30 second timeout
      });
      
      return response;
    } catch (error) {
      retries++;
      if (axios.isAxiosError(error) && error.response?.status === 429) {
        console.log(`[${date} P${period}] Rate limited, retrying after delay... (${retries}/${MAX_RETRIES})`);
        await new Promise(resolve => setTimeout(resolve, 60000)); // Wait 1 minute on rate limit
      } else if (retries < MAX_RETRIES) {
        console.log(`[${date} P${period}] Request failed, retrying... (${retries}/${MAX_RETRIES})`);
        await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5 seconds between retries
      } else {
        throw error;
      }
    }
  }
}

/**
 * Improved version of fetchBidsOffers that checks for both soFlag and cadlFlag
 */
async function customFetchBidsOffers(date: string, period: number): Promise<any[]> {
  try {
    const validWindFarmIds = await getUnifiedWindFarmIds();
    const leadPartyMap = await getLeadPartyMapping();
    
    // Make parallel requests for bids and offers
    const [bidsResponse, offersResponse] = await Promise.all([
      makeRequest(
        `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`,
        date,
        period
      ),
      makeRequest(
        `${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`,
        date,
        period
      )
    ]).catch(error => {
      console.error(`[${date} P${period}] Error fetching data:`, error.message);
      return [{ data: { data: [] } }, { data: { data: [] } }];
    });
    
    if (!bidsResponse.data?.data || !offersResponse.data?.data) {
      console.error(`[${date} P${period}] Invalid API response format`);
      return [];
    }
    
    // Filter with correct logic
    const validBids = bidsResponse.data.data.filter((record: any) => 
      record.volume < 0 && 
      (record.soFlag || record.cadlFlag) && 
      validWindFarmIds.has(record.id)
    );
    
    const validOffers = offersResponse.data.data.filter((record: any) => 
      record.volume < 0 && 
      (record.soFlag || record.cadlFlag) && 
      validWindFarmIds.has(record.id)
    );
    
    // Add lead party names to records if missing
    const allRecords = [...validBids, ...validOffers].map(record => {
      if (!record.leadPartyName && leadPartyMap.has(record.id)) {
        record.leadPartyName = leadPartyMap.get(record.id);
      }
      return record;
    });
    
    if (allRecords.length > 0) {
      const periodTotal = allRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
      const periodPayment = allRecords.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice * -1), 0);
      console.log(`[${date} P${period}] Records: ${allRecords.length} (${periodTotal.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`);
    } else {
      console.log(`[${date} P${period}] No valid curtailment records found`);
    }
    
    return allRecords;
  } catch (error) {
    console.error(`[${date} P${period}] Error fetching bids/offers:`, error);
    return [];
  }
}

/**
 * Optimized process curtailment focusing only on targeted periods
 */
async function optimizedProcessCurtailment(date: string): Promise<void> {
  console.log(`Optimized processing of curtailment data for ${date}`);
  
  // Clear existing records for the date
  await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  let totalVolume = 0;
  let totalPayment = 0;
  let recordsProcessed = 0;
  
  // Process only targeted periods
  for (const period of TARGETED_PERIODS) {
    try {
      const records = await customFetchBidsOffers(date, period);
      
      // Insert valid records into the database
      const validRecords = records.filter(record => 
        record.volume < 0 && 
        (record.soFlag || record.cadlFlag)
      );
      
      if (validRecords.length > 0) {
        console.log(`[${date} P${period}] Inserting ${validRecords.length} records`);
      }
      
      const periodResults = await Promise.all(
        validRecords.map(async record => {
          try {
            const volume = Math.abs(record.volume);
            const payment = volume * record.originalPrice;
            
            await db.insert(curtailmentRecords).values({
              settlementDate: date,
              settlementPeriod: period,
              farmId: record.id,
              leadPartyName: record.leadPartyName || 'Unknown',
              volume: record.volume.toString(), // Keep the original negative value
              payment: payment.toString(),
              originalPrice: record.originalPrice.toString(),
              finalPrice: record.finalPrice.toString(),
              soFlag: record.soFlag,
              cadlFlag: record.cadlFlag
            });
            
            recordsProcessed++;
            totalVolume += volume;
            totalPayment += payment;
            
            return { volume, payment };
          } catch (error) {
            console.error(`[${date} P${period}] Error inserting record for ${record.id}:`, error);
            return { volume: 0, payment: 0 };
          }
        })
      );
    } catch (error) {
      console.error(`Error processing period ${period} for date ${date}:`, error);
    }
    
    // Add a small delay between periods to avoid rate limiting
    await new Promise(resolve => setTimeout(resolve, 200));
  }
  
  // Update daily summary
  await db.insert(dailySummaries).values({
    summaryDate: date,
    totalCurtailedEnergy: totalVolume.toString(),
    totalPayment: totalPayment.toString()
  }).onConflictDoUpdate({
    target: [dailySummaries.summaryDate],
    set: {
      totalCurtailedEnergy: totalVolume.toString(),
      totalPayment: totalPayment.toString()
    }
  });
  
  console.log(`=== Curtailment processing summary for ${date} ===`);
  console.log(`Records processed: ${recordsProcessed}`);
  console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`Total payment: £${totalPayment.toFixed(2)}`);
}

/**
 * Main function to reprocess data for 2025-05-08
 */
async function reprocessMay8th() {
  console.log(`\n=== Starting Optimized Reprocessing for ${TARGET_DATE} ===\n`);
  
  try {
    // Step 1: Clear existing data for the target date
    console.log(`Clearing existing curtailment records for ${TARGET_DATE}...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Clearing existing Bitcoin calculations for ${TARGET_DATE}...`);
    await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
    // Step 2: Reprocess curtailment data using our optimized method
    console.log(`\nReprocessing curtailment data for ${TARGET_DATE}...`);
    try {
      await optimizedProcessCurtailment(TARGET_DATE);
      
      // Verify curtailment data was processed
      const curtailmentStats = await db
        .select({
          count: sql<number>`COUNT(*)`,
          periodCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
          totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
          totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
      console.log(`Successfully reprocessed curtailment data for ${TARGET_DATE}:`, {
        records: curtailmentStats[0].count,
        periods: curtailmentStats[0].periodCount,
        volume: Number(curtailmentStats[0].totalVolume || 0).toFixed(2) + ' MWh',
        payment: '£' + Number(curtailmentStats[0].totalPayment || 0).toFixed(2)
      });
    } catch (error) {
      console.error(`Error processing curtailment data:`, error);
      throw error;
    }
    
    // Step 3: Process wind generation data
    console.log(`\nProcessing wind generation data for ${TARGET_DATE}...`);
    try {
      const windDataProcessed = await processWindDataForDate(TARGET_DATE);
      if (windDataProcessed) {
        console.log(`Successfully processed wind generation data for ${TARGET_DATE}`);
      } else {
        console.log(`No wind generation data found for ${TARGET_DATE}`);
      }
    } catch (error) {
      console.error(`Error processing wind generation data:`, error);
      // Continue even if wind data processing fails
      console.log(`Continuing with Bitcoin calculations despite wind data error`);
    }
    
    // Step 4: Process Bitcoin calculations for each miner model
    console.log(`\nProcessing Bitcoin calculations for ${TARGET_DATE}...`);
    for (const minerModel of MINER_MODELS) {
      try {
        console.log(`Processing Bitcoin calculations for ${minerModel}...`);
        await processSingleDay(TARGET_DATE, minerModel);
        
        // Verify Bitcoin calculations were processed
        const bitcoinStats = await db
          .select({
            count: sql<number>`COUNT(*)`,
            totalBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
          })
          .from(historicalBitcoinCalculations)
          .where(and(
            eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          ));
        
        console.log(`Successfully processed Bitcoin calculations for ${minerModel}:`, {
          records: bitcoinStats[0].count,
          bitcoinMined: Number(bitcoinStats[0].totalBitcoin || 0).toFixed(8) + ' BTC'
        });
      } catch (error) {
        console.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
        // Continue with other miner models even if one fails
      }
    }
    
    // Step 5: Verify daily summary was updated
    const dailySummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, TARGET_DATE)
    });
    
    if (dailySummary) {
      console.log(`\nVerified daily summary for ${TARGET_DATE}:`, {
        energy: Number(dailySummary.totalCurtailedEnergy || 0).toFixed(2) + ' MWh',
        payment: '£' + Number(dailySummary.totalPayment || 0).toFixed(2),
        windGeneration: Number(dailySummary.totalWindGeneration || 0).toFixed(2) + ' MWh'
      });
    } else {
      console.log(`\nWarning: No daily summary found for ${TARGET_DATE}`);
    }
    
    console.log(`\n=== Reprocessing Complete for ${TARGET_DATE} ===`);
    console.log(`All data has been successfully reprocessed.`);
    
  } catch (error) {
    console.error(`\nError during reprocessing:`, error);
    process.exit(1);
  }
}

// Run the reprocessing function
reprocessMay8th();