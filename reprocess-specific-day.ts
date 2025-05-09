/**
 * Reprocess Specific Day Script
 * 
 * This script reprocesses all data for a specific date with improved
 * BMU mapping and filtering logic.
 * 
 * Usage:
 *   npx tsx reprocess-specific-day.ts 2025-05-08
 * 
 * (Pass a date as the first argument, defaults to yesterday if none provided)
 */

import { db } from "./db";
import { 
  curtailmentRecords, 
  historicalBitcoinCalculations,
  dailySummaries
} from "./db/schema";
import { processWindDataForDate } from "./server/services/windDataUpdater";
import { processSingleDay } from "./server/services/bitcoinService";
import { eq, and, sql } from "drizzle-orm";
import { format, subDays } from "date-fns";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Constants
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const SERVER_BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");
const DATA_BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");

// Get the date to process
const args = process.argv.slice(2);
const specificDate = args[0] || format(subDays(new Date(), 1), 'yyyy-MM-dd');

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

/**
 * Make API request with retries
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
 * Fetch bids and offers for a specific period with correct filtering
 */
async function fetchBidsOffers(date: string, period: number): Promise<any[]> {
  try {
    const validWindFarmIds = await getUnifiedWindFarmIds();
    
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
    // Filter with correct logic: volume < 0 AND (soFlag OR cadlFlag)
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
    
    const allRecords = [...validBids, ...validOffers];
    
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
 * Process curtailment data for a specific date
 */
async function processCurtailmentData(date: string): Promise<number> {
  console.log(`Processing curtailment data for ${date}`);
  
  // Clear existing records for the date
  await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  let totalVolume = 0;
  let totalPayment = 0;
  let recordsProcessed = 0;
  
  // Sample specific periods first to check if there's data
  const samplePeriods = [1, 12, 24, 36, 48];
  let hasCurtailmentData = false;
  
  for (const period of samplePeriods) {
    const records = await fetchBidsOffers(date, period);
    if (records.length > 0) {
      hasCurtailmentData = true;
    }
  }
  
  // If samples found data, process all 48 periods
  // If no data found in samples, only process periods 25-30 as that's where curtailment often occurs
  const periodsToProcess = hasCurtailmentData 
    ? Array.from({ length: 48 }, (_, i) => i + 1)
    : [25, 26, 27, 28, 29, 30];
  
  console.log(`${hasCurtailmentData ? 'Data found in samples - processing all periods' : 'No data in samples - processing targeted periods only'}`);
  
  for (const period of periodsToProcess) {
    try {
      const records = await fetchBidsOffers(date, period);
      
      // Insert valid records into the database
      for (const record of records) {
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
      }
    } catch (error) {
      console.error(`Error processing period ${period} for date ${date}:`, error);
    }
    
    // Add a small delay between periods to avoid rate limiting
    await new Promise(resolve => setTimeout(resolve, 200));
  }
  
  // Update daily summary
  if (recordsProcessed > 0) {
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
  }
  
  console.log(`=== Curtailment processing summary for ${date} ===`);
  console.log(`Records processed: ${recordsProcessed}`);
  console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`Total payment: £${totalPayment.toFixed(2)}`);
  
  return recordsProcessed;
}

/**
 * Main function to reprocess data for a specific date
 */
async function reprocessSpecificDay() {
  console.log(`\n=== Starting Data Reprocessing for ${specificDate} ===\n`);
  const startTime = Date.now();
  
  try {
    // Step 1: Clear existing data for the target date
    console.log(`Clearing existing curtailment records for ${specificDate}...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, specificDate));
    
    console.log(`Clearing existing Bitcoin calculations for ${specificDate}...`);
    await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, specificDate));
    
    // Step 2: Reprocess curtailment data
    console.log(`\nReprocessing curtailment data for ${specificDate}...`);
    const recordsProcessed = await processCurtailmentData(specificDate);
    
    // Verify curtailment data was processed
    const curtailmentStats = await db
      .select({
        count: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, specificDate));
    
    console.log(`Curtailment data results for ${specificDate}:`, {
      records: curtailmentStats[0].count,
      periods: curtailmentStats[0].periodCount,
      volume: Number(curtailmentStats[0].totalVolume || 0).toFixed(2) + ' MWh',
      payment: '£' + Number(curtailmentStats[0].totalPayment || 0).toFixed(2)
    });
    
    // Step 3: Process wind generation data 
    console.log(`\nProcessing wind generation data for ${specificDate}...`);
    try {
      const windDataProcessed = await processWindDataForDate(specificDate);
      if (windDataProcessed) {
        console.log(`Successfully processed wind generation data for ${specificDate}`);
      } else {
        console.log(`No wind generation data found for ${specificDate}`);
      }
    } catch (error) {
      console.error(`Error processing wind generation data:`, error);
      // Continue even if wind data processing fails
      console.log(`Continuing with Bitcoin calculations despite wind data error`);
    }
    
    // Step 4: Process Bitcoin calculations for each miner model
    if (recordsProcessed > 0) {
      console.log(`\nProcessing Bitcoin calculations for ${specificDate}...`);
      for (const minerModel of MINER_MODELS) {
        try {
          console.log(`Processing Bitcoin calculations for ${minerModel}...`);
          await processSingleDay(specificDate, minerModel);
          
          // Verify Bitcoin calculations were processed
          const bitcoinStats = await db
            .select({
              count: sql<number>`COUNT(*)`,
              totalBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
            })
            .from(historicalBitcoinCalculations)
            .where(and(
              eq(historicalBitcoinCalculations.settlementDate, specificDate),
              eq(historicalBitcoinCalculations.minerModel, minerModel)
            ));
          
          console.log(`Bitcoin calculations for ${minerModel}:`, {
            records: bitcoinStats[0].count,
            bitcoinMined: Number(bitcoinStats[0].totalBitcoin || 0).toFixed(8) + ' BTC'
          });
        } catch (error) {
          console.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
          // Continue with other miner models even if one fails
        }
      }
    } else {
      console.log(`\nSkipping Bitcoin calculations as no curtailment records were found.`);
    }
    
    // Step 5: Verify daily summary was updated
    const dailySummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, specificDate)
    });
    
    if (dailySummary) {
      console.log(`\nVerified daily summary for ${specificDate}:`, {
        energy: Number(dailySummary.totalCurtailedEnergy || 0).toFixed(2) + ' MWh',
        payment: '£' + Number(dailySummary.totalPayment || 0).toFixed(2),
        windGeneration: Number(dailySummary.totalWindGeneration || 0).toFixed(2) + ' MWh'
      });
    } else {
      console.log(`\nNo daily summary found for ${specificDate}`);
    }
    
    const endTime = Date.now();
    const durationMinutes = ((endTime - startTime) / 1000 / 60).toFixed(1);
    
    console.log(`\n=== Reprocessing Complete for ${specificDate} ===`);
    console.log(`Total execution time: ${durationMinutes} minutes`);
    
  } catch (error) {
    console.error(`\nError during reprocessing:`, error);
    process.exit(1);
  }
}

// Run the reprocessing
reprocessSpecificDay();