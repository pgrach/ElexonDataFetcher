/**
 * Improved Reprocessing Script for Date Range
 * 
 * This script allows reprocessing data for a range of dates with improved
 * BMU mapping and API filtering to ensure all curtailment records are captured.
 * 
 * Usage:
 *   npx tsx scripts/reprocess-date-range-fixed.ts --start 2025-05-01 --end 2025-05-08
 *   npx tsx scripts/reprocess-date-range-fixed.ts --start 2025-05-01 --end 2025-05-08 --skip-wind
 *   npx tsx scripts/reprocess-date-range-fixed.ts --start 2025-05-01 --end 2025-05-08 --miners S19J_PRO
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
import { format, parse, addDays, isBefore, isValid, parseISO } from "date-fns";
import pLimit from "p-limit";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Constants
const DEFAULT_MINER_MODELS = ["S19J_PRO", "S9", "M20S"];
const MAX_CONCURRENT_DATES = 3; // Process max 3 dates in parallel to avoid rate limits
const DATE_FORMAT = "yyyy-MM-dd";
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const SERVER_BMU_MAPPING_PATH = path.join(__dirname, "../server/data/bmuMapping.json");
const DATA_BMU_MAPPING_PATH = path.join(__dirname, "../data/bmu_mapping.json");

// Command line arguments
const args = process.argv.slice(2);
let startDate: string | null = null;
let endDate: string | null = null;
let skipWind = false;
let minerModels = DEFAULT_MINER_MODELS;

// Parse command line arguments
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--start" && i + 1 < args.length) {
    startDate = args[i + 1];
    i++;
  } else if (args[i] === "--end" && i + 1 < args.length) {
    endDate = args[i + 1];
    i++;
  } else if (args[i] === "--skip-wind") {
    skipWind = true;
  } else if (args[i] === "--miners" && i + 1 < args.length) {
    minerModels = args[i + 1].split(",");
    i++;
  }
}

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
    
    // FIXED: Changed filter to match the logic in curtailment_enhanced.ts
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
 * Get all dates in a range as formatted strings
 */
function getDatesInRange(start: string, end: string): string[] {
  const dates: string[] = [];
  
  const startDate = parseISO(start);
  const endDate = parseISO(end);
  
  let currentDate = startDate;
  
  while (isBefore(currentDate, endDate) || format(currentDate, DATE_FORMAT) === end) {
    dates.push(format(currentDate, DATE_FORMAT));
    currentDate = addDays(currentDate, 1);
  }
  
  return dates;
}

/**
 * Manually process curtailment for a specific date with improved filtering
 */
async function manualProcessCurtailment(date: string): Promise<void> {
  console.log(`[${date}] Manually processing curtailment data`);
  
  // Clear existing records for the date
  await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  let totalVolume = 0;
  let totalPayment = 0;
  let recordsProcessed = 0;
  
  // Process all 48 periods
  for (let period = 1; period <= 48; period++) {
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
  
  console.log(`[${date}] Curtailment processing summary:`);
  console.log(`[${date}] Records processed: ${recordsProcessed}`);
  console.log(`[${date}] Total volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`[${date}] Total payment: £${totalPayment.toFixed(2)}`);
}

/**
 * Process a single date's worth of data
 */
async function processDate(date: string): Promise<void> {
  console.log(`\n=== Processing data for ${date} ===`);
  
  try {
    // Step 1: Clear existing data for the target date
    console.log(`[${date}] Clearing existing curtailment records...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    console.log(`[${date}] Clearing existing Bitcoin calculations...`);
    await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, date));
    
    // Step 2: Reprocess curtailment data using our improved method
    console.log(`[${date}] Reprocessing curtailment data...`);
    try {
      await manualProcessCurtailment(date);
      
      // Verify curtailment data was processed
      const curtailmentStats = await db
        .select({
          count: sql<number>`COUNT(*)`,
          periodCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
          totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
          totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, date));
      
      console.log(`[${date}] Curtailment data processed:`, {
        records: curtailmentStats[0].count,
        periods: curtailmentStats[0].periodCount,
        volume: Number(curtailmentStats[0].totalVolume || 0).toFixed(2) + ' MWh',
        payment: '£' + Number(curtailmentStats[0].totalPayment || 0).toFixed(2)
      });
    } catch (error) {
      console.error(`[${date}] Error processing curtailment data:`, error);
      throw error;
    }
    
    // Step 3: Process wind generation data (if not skipped)
    if (skipWind) {
      console.log(`[${date}] Skipping wind data processing (--skip-wind)`);
    } else {
      console.log(`[${date}] Processing wind generation data...`);
      try {
        const windDataProcessed = await processWindDataForDate(date);
        if (windDataProcessed) {
          console.log(`[${date}] Wind generation data processed successfully`);
        } else {
          console.log(`[${date}] No wind generation data found`);
        }
      } catch (error) {
        console.error(`[${date}] Error processing wind generation data:`, error);
        // Continue even if wind data processing fails
        console.log(`[${date}] Continuing with Bitcoin calculations despite wind data error`);
      }
    }
    
    // Step 4: Process Bitcoin calculations for each miner model
    console.log(`[${date}] Processing Bitcoin calculations...`);
    for (const minerModel of minerModels) {
      try {
        console.log(`[${date}] Processing Bitcoin calculations for ${minerModel}...`);
        await processSingleDay(date, minerModel);
        
        // Verify Bitcoin calculations were processed
        const bitcoinStats = await db
          .select({
            count: sql<number>`COUNT(*)`,
            totalBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
          })
          .from(historicalBitcoinCalculations)
          .where(and(
            eq(historicalBitcoinCalculations.settlementDate, date),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          ));
        
        console.log(`[${date}] Bitcoin calculations for ${minerModel}:`, {
          records: bitcoinStats[0].count,
          bitcoinMined: Number(bitcoinStats[0].totalBitcoin || 0).toFixed(8) + ' BTC'
        });
      } catch (error) {
        console.error(`[${date}] Error processing Bitcoin calculations for ${minerModel}:`, error);
        // Continue with other miner models even if one fails
      }
    }
    
    // Step 5: Verify daily summary was updated
    const dailySummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, date)
    });
    
    if (dailySummary) {
      console.log(`[${date}] Daily summary updated:`, {
        energy: Number(dailySummary.totalCurtailedEnergy || 0).toFixed(2) + ' MWh',
        payment: '£' + Number(dailySummary.totalPayment || 0).toFixed(2),
        windGeneration: Number(dailySummary.totalWindGeneration || 0).toFixed(2) + ' MWh'
      });
    } else {
      console.log(`[${date}] Warning: No daily summary found`);
    }
    
    console.log(`=== Completed processing for ${date} ===`);
  } catch (error) {
    console.error(`[${date}] Error during processing:`, error);
    throw error;
  }
}

/**
 * Main function to run the reprocessing
 */
async function runDateRangeReprocessing() {
  console.log("Bitcoin Mining Analytics - Improved Date Range Reprocessing Tool");
  console.log("=========================================================\n");
  
  try {
    // Validate inputs
    if (!startDate || !endDate) {
      console.error("Please provide both start and end dates.");
      console.error("Example: npx tsx scripts/reprocess-date-range-fixed.ts --start 2025-05-01 --end 2025-05-08");
      process.exit(1);
    }
    
    // Validate date formats
    const startDateObj = parseISO(startDate);
    const endDateObj = parseISO(endDate);
    
    if (!isValid(startDateObj) || !isValid(endDateObj)) {
      console.error("Invalid date format. Use YYYY-MM-DD format for --start and --end parameters.");
      process.exit(1);
    }
    
    if (startDateObj > endDateObj) {
      console.error("Start date must be before or equal to end date.");
      process.exit(1);
    }
    
    // Get all dates in the range
    const dates = getDatesInRange(startDate, endDate);
    
    console.log(`Processing date range: ${startDate} to ${endDate}`);
    console.log(`Total dates to process: ${dates.length}`);
    console.log(`Skip wind processing: ${skipWind ? 'Yes' : 'No'}`);
    console.log(`Miner models: ${minerModels.join(', ')}`);
    console.log(`Max concurrent dates: ${MAX_CONCURRENT_DATES}`);
    
    // Set up concurrency limit to avoid overwhelming the APIs
    const limit = pLimit(MAX_CONCURRENT_DATES);
    const processingPromises = dates.map(date => 
      limit(() => processDate(date))
    );
    
    console.log("\nStarting data processing...");
    console.log("This may take some time depending on the number of dates and API rate limits.");
    
    const startTime = Date.now();
    await Promise.all(processingPromises);
    const endTime = Date.now();
    
    const duration = (endTime - startTime) / 1000 / 60; // Convert to minutes
    
    console.log("\n=== Reprocessing Complete ===");
    console.log(`Processed ${dates.length} dates from ${startDate} to ${endDate}`);
    console.log(`Total execution time: ${duration.toFixed(1)} minutes`);
    console.log("All data has been processed successfully.");
    
  } catch (error) {
    console.error("Error during reprocessing:", error);
    process.exit(1);
  }
}

// Run the reprocessing
runDateRangeReprocessing();