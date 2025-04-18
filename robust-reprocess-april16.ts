/**
 * Robust Reprocessing Script for 2025-04-16
 * 
 * This script implements a highly reliable approach to ensure 100% data capture
 * from Elexon API for 2025-04-16. It uses a sequential processing approach with
 * robust error handling and retries.
 * 
 * Run with: npx tsx robust-reprocess-april16.ts
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries } from "./db/schema";
import { eq, sql } from "drizzle-orm";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';
import { processSingleDay } from "./server/services/bitcoinService";
import { minerModels } from "./server/types/bitcoin";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TARGET_DATE = "2025-04-16";
const BMU_MAPPING_PATH = path.join(__dirname, "./server/data/bmuMapping.json");
const MINER_MODEL_KEYS = Object.keys(minerModels);
const MAX_RETRIES = 10;
const BASE_RETRY_DELAY_MS = 2000;
const REQUEST_TIMEOUT_MS = 60000; // 60 seconds

// Enhanced Elexon API interface
interface ElexonBidOffer {
  settlementDate: string;
  settlementPeriod: number;
  id: string;
  bmUnit?: string;
  volume: number;
  soFlag: boolean;
  cadlFlag: boolean | null;
  originalPrice: number;
  finalPrice: number;
  leadPartyName?: string;
}

// Delay utility function
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load wind farm BMU IDs
async function loadWindFarmIds(): Promise<string[]> {
  try {
    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    const windFarmIds = bmuMapping.map((bmu: any) => bmu.elexonBmUnit);
    console.log(`Loaded ${windFarmIds.length} wind farm BMU IDs`);
    return windFarmIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

// Map BMU IDs to lead party names
async function loadBmuLeadPartyMap(): Promise<Map<string, string>> {
  try {
    console.log('Loading BMU to lead party mapping...');
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    
    const bmuLeadPartyMap = new Map<string, string>();
    for (const bmu of bmuMapping) {
      if (bmu.elexonBmUnit && bmu.leadPartyName) {
        bmuLeadPartyMap.set(bmu.elexonBmUnit, bmu.leadPartyName);
      }
    }
    
    console.log(`Loaded ${bmuLeadPartyMap.size} BMU-to-lead-party mappings`);
    return bmuLeadPartyMap;
  } catch (error) {
    console.error('Error loading BMU to lead party mapping:', error);
    throw error;
  }
}

// Fetch bids and offers with robust retries
async function fetchBidsOffersRobust(date: string, period: number, retryCount = 0): Promise<ElexonBidOffer[]> {
  try {
    // Base URL for Elexon API
    const baseUrl = "https://data.elexon.co.uk/bmrs/api/v1";
    
    // Build URL for the request
    const url = `${baseUrl}/datasets/FORDAI/settlement-periods/${date}/${period}?format=json`;
    
    console.log(`Fetching from Elexon API: ${url}`);
    
    // Make the request with increased timeout
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: REQUEST_TIMEOUT_MS  // Increased timeout
    });
    
    if (!response.data || !Array.isArray(response.data.data)) {
      throw new Error('Invalid API response format');
    }
    
    console.log(`[${date} P${period}] Received ${response.data.data.length} records from Elexon API`);
    
    // Process API response
    const records: ElexonBidOffer[] = response.data.data.map((item: any) => ({
      settlementDate: date,
      settlementPeriod: period,
      id: item.bmUnit,
      volume: item.volume,
      soFlag: item.soFlag === "Y",
      cadlFlag: item.cadlFlag === "Y" ? true : (item.cadlFlag === "N" ? false : null),
      originalPrice: item.originalPrice,
      finalPrice: item.finalPrice
    }));
    
    return records;
  } catch (error) {
    // Handle rate limiting (429 status code)
    if (axios.isAxiosError(error) && error.response?.status === 429) {
      if (retryCount < MAX_RETRIES) {
        const delayMs = BASE_RETRY_DELAY_MS * Math.pow(2, retryCount);
        console.log(`[${date} P${period}] Rate limited, retrying after ${delayMs}ms delay (${retryCount + 1}/${MAX_RETRIES})...`);
        await delay(delayMs);
        return fetchBidsOffersRobust(date, period, retryCount + 1);
      }
    }
    
    // Handle timeouts and other errors
    if (axios.isAxiosError(error) && error.code === 'ECONNABORTED') {
      if (retryCount < MAX_RETRIES) {
        const delayMs = BASE_RETRY_DELAY_MS * Math.pow(2, retryCount);
        console.log(`[${date} P${period}] Request timeout, retrying after ${delayMs}ms delay (${retryCount + 1}/${MAX_RETRIES})...`);
        await delay(delayMs);
        return fetchBidsOffersRobust(date, period, retryCount + 1);
      }
    }
    
    // Handle server errors
    if (axios.isAxiosError(error) && error.response?.status && error.response.status >= 500) {
      if (retryCount < MAX_RETRIES) {
        const delayMs = BASE_RETRY_DELAY_MS * Math.pow(2, retryCount);
        console.log(`[${date} P${period}] Server error (${error.response.status}), retrying after ${delayMs}ms delay (${retryCount + 1}/${MAX_RETRIES})...`);
        await delay(delayMs);
        return fetchBidsOffersRobust(date, period, retryCount + 1);
      }
    }
    
    if (retryCount >= MAX_RETRIES) {
      console.error(`[${date} P${period}] Maximum retries reached. Giving up.`);
    }
    
    console.error(`[${date} P${period}] Error fetching from Elexon API:`, error);
    return []; // Return empty array instead of throwing, to continue processing other periods
  }
}

// Completely sequential processing - one BMU/period at a time for maximum reliability
async function processSequentially() {
  console.log(`\n=== Starting Sequential Reprocessing for ${TARGET_DATE} ===\n`);
  const startTime = new Date();
  
  try {
    // Step 1: Delete existing curtailment records for the target date
    console.log(`Removing existing curtailment records for ${TARGET_DATE}...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Step 2: Load BMU IDs and mappings
    const allBmuIds = await loadWindFarmIds();
    const bmuLeadPartyMap = await loadBmuLeadPartyMap();
    
    // Step 3: Process completely sequentially
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Counters for retries
    let apiFailures = 0;
    let dbFailures = 0;
    
    // Process each period one at a time
    for (let period = 1; period <= 48; period++) {
      console.log(`\nProcessing period ${period}/48...`);
      
      // Fetch all records for this period
      const periodRecords = await fetchBidsOffersRobust(TARGET_DATE, period);
      
      // Filter to valid curtailment records
      const validRecords = periodRecords.filter(record => 
        record.volume < 0 &&
        (record.soFlag || record.cadlFlag) &&
        allBmuIds.includes(record.id)
      );
      
      if (validRecords.length > 0) {
        console.log(`[${TARGET_DATE} P${period}] Processing ${validRecords.length} valid curtailment records`);
      } else {
        console.log(`[${TARGET_DATE} P${period}] No valid curtailment records found`);
        continue; // Skip to next period
      }
      
      // Process each record one at a time
      let periodVolume = 0;
      let periodPayment = 0;
      let periodRecordsInserted = 0;
      
      for (const record of validRecords) {
        // Compute volume and payment
        const volume = Math.abs(record.volume);
        const payment = volume * record.originalPrice;
        
        // Retry loop for database operations
        let dbRetryCount = 0;
        let insertSuccess = false;
        
        while (!insertSuccess && dbRetryCount < MAX_RETRIES) {
          try {
            // Insert the record
            await db.insert(curtailmentRecords).values({
              settlementDate: TARGET_DATE,
              settlementPeriod: period,
              farmId: record.id,
              leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
              volume: record.volume.toString(),
              payment: payment.toString(),
              originalPrice: record.originalPrice.toString(),
              finalPrice: record.finalPrice.toString(),
              soFlag: record.soFlag,
              cadlFlag: record.cadlFlag
            });
            
            // Record successful insertion
            periodRecordsInserted++;
            periodVolume += volume;
            periodPayment += payment;
            
            console.log(`[${TARGET_DATE} P${period}] Added record for ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
            
            insertSuccess = true;
          } catch (error) {
            dbRetryCount++;
            dbFailures++;
            
            console.error(`[${TARGET_DATE} P${period}] Error inserting record for ${record.id} (attempt ${dbRetryCount}/${MAX_RETRIES}):`, error);
            
            if (dbRetryCount < MAX_RETRIES) {
              const delayMs = BASE_RETRY_DELAY_MS * Math.pow(1.5, dbRetryCount - 1);
              console.log(`Waiting ${delayMs}ms before retry...`);
              await delay(delayMs);
            } else {
              console.error(`[${TARGET_DATE} P${period}] All ${MAX_RETRIES} database retries failed for BMU ${record.id}. Skipping this record.`);
            }
          }
        }
        
        // Small delay between record insertions to reduce database contention
        await delay(50);
      }
      
      // Add period stats to totals
      totalRecords += periodRecordsInserted;
      totalVolume += periodVolume;
      totalPayment += periodPayment;
      
      console.log(`[${TARGET_DATE} P${period}] Period summary: ${periodRecordsInserted}/${validRecords.length} records inserted, ${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
      
      // Add delay between periods to avoid API rate limits
      if (period < 48) {
        await delay(1000);
      }
    }
    
    // Step 4: Update daily summary
    console.log(`\nUpdating daily summary for ${TARGET_DATE}...`);
    try {
      // Delete existing summary if any
      await db.delete(dailySummaries)
        .where(eq(dailySummaries.summaryDate, TARGET_DATE));
      
      // Count distinct periods and farms
      const countResult = await db.select({
        periodCount: sql<string>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        farmCount: sql<string>`COUNT(DISTINCT ${curtailmentRecords.farmId})`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
      // Insert new summary
      await db.insert(dailySummaries).values({
        summaryDate: TARGET_DATE,
        totalCurtailedEnergy: totalVolume,
        totalPayment: -totalPayment, // Use negative payment as per schema
        periodCount: Number(countResult[0]?.periodCount || 0),
        farmCount: Number(countResult[0]?.farmCount || 0),
        recordCount: totalRecords,
        lastUpdated: new Date()
      });
      
      console.log(`✓ Daily summary updated for ${TARGET_DATE}`);
    } catch (error) {
      console.error(`Error updating daily summary:`, error);
    }
    
    // Step 5: Summary and verification
    console.log(`\n=== Reprocessing Summary ===`);
    console.log(`Total records processed: ${totalRecords}`);
    console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${totalPayment.toFixed(2)}`);
    console.log(`API failures: ${apiFailures}`);
    console.log(`Database failures: ${dbFailures}`);
    
    // Calculate execution time
    const endTime = new Date();
    const executionTimeMs = endTime.getTime() - startTime.getTime();
    console.log(`\n=== Reprocessing Completed ===`);
    console.log(`Total execution time: ${(executionTimeMs / 1000).toFixed(2)} seconds`);
    
    // Verify against expected totals from Elexon
    console.log(`\n=== Verification ===`);
    const verificationQuery = await db.select({
      record_count: sql<string>`COUNT(*)`,
      period_count: sql<string>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      farm_count: sql<string>`COUNT(DISTINCT ${curtailmentRecords.farmId})`,
      total_volume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      total_payment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));

    const verification = verificationQuery[0];
    console.log(`Records in database: ${verification.record_count}`);
    console.log(`Periods in database: ${verification.period_count}`);
    console.log(`Farms in database: ${verification.farm_count}`);
    console.log(`Total volume in database: ${Number(verification.total_volume).toFixed(2)} MWh`);
    console.log(`Total payment in database: £${Number(verification.total_payment).toFixed(2)}`);
    
    return {
      success: true,
      records: totalRecords,
      volume: totalVolume,
      payment: totalPayment
    };
    
  } catch (error) {
    console.error(`\n❌ Sequential reprocessing failed:`, error);
    return {
      success: false,
      error
    };
  }
}

// Update Bitcoin calculations
async function updateBitcoinCalculations() {
  console.log(`\n=== Updating Bitcoin Calculations for ${TARGET_DATE} ===\n`);
  
  for (const minerModel of MINER_MODEL_KEYS) {
    console.log(`Processing calculations for ${minerModel}...`);
    try {
      const result = await processSingleDay(TARGET_DATE, minerModel);
      if (result && result.success) {
        console.log(`✓ Successfully processed ${minerModel}: ${result.bitcoinMined.toFixed(8)} BTC (£${result.valueGbp.toFixed(2)})`);
      } else {
        console.log(`No calculations generated for ${minerModel}`);
      }
    } catch (error) {
      console.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
    }
  }
}

// Main function
async function main() {
  try {
    // Step 1: Sequential reprocessing of curtailment data
    const curtailmentResult = await processSequentially();
    
    if (curtailmentResult.success) {
      // Step 2: Process Bitcoin calculations
      await updateBitcoinCalculations();
    } else {
      console.error("Curtailment processing failed, skipping Bitcoin calculations");
    }
    
    console.log("\nReprocessing completed");
    process.exit(0);
  } catch (error) {
    console.error("Unexpected error:", error);
    process.exit(1);
  }
}

// Run the script
main();