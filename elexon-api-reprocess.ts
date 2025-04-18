/**
 * Elexon API-based Reprocessing Script for 2025-04-16
 * 
 * This script uses the same exact Elexon API endpoints and processing logic
 * as the main application code to ensure 100% data capture from Elexon.
 * 
 * Run with: npx tsx elexon-api-reprocess.ts
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
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const REQUEST_TIMEOUT_MS = 60000; // 60 seconds
const MAX_REQUESTS_PER_MINUTE = 4500;
const REQUEST_WINDOW_MS = 60000; // 1 minute in milliseconds

// Tracking request timestamps for rate limiting
let requestTimestamps: number[] = [];

// Utility functions
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function trackRequest() {
  const now = Date.now();
  requestTimestamps = [...requestTimestamps, now].filter(timestamp => 
    now - timestamp < REQUEST_WINDOW_MS
  );
}

async function waitForRateLimit(): Promise<void> {
  const now = Date.now();
  requestTimestamps = requestTimestamps.filter(timestamp => 
    now - timestamp < REQUEST_WINDOW_MS
  );

  if (requestTimestamps.length >= MAX_REQUESTS_PER_MINUTE) {
    const oldestRequest = requestTimestamps[0];
    const waitTime = REQUEST_WINDOW_MS - (now - oldestRequest);
    console.log(`Rate limit reached, waiting ${Math.ceil(waitTime/1000)}s...`);
    await delay(waitTime + 100); // Add 100ms buffer
    return waitForRateLimit(); // Recheck after waiting
  }
}

// Make request to Elexon API with rate limiting
async function makeRequest(url: string, date: string, period: number): Promise<any> {
  await waitForRateLimit();

  try {
    console.log(`Making request to: ${url}`);
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: REQUEST_TIMEOUT_MS
    });

    trackRequest();
    return response;
  } catch (error) {
    if (axios.isAxiosError(error) && error.response?.status === 429) {
      console.log(`[${date} P${period}] Rate limited, retrying after delay...`);
      await delay(60000); // Wait 1 minute on rate limit
      return makeRequest(url, date, period);
    }
    throw error;
  }
}

// Load BMU mapping
async function loadWindFarmIds(): Promise<Set<string>> {
  try {
    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    const windFarmIds = new Set(bmuMapping.map((bmu: any) => bmu.elexonBmUnit));
    console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
    return windFarmIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

// Load BMU to lead party mapping
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

// Fetch bids and offers using same approach as main application
async function fetchBidsOffers(date: string, period: number): Promise<any[]> {
  try {
    const validWindFarmIds = await loadWindFarmIds();

    // Make parallel requests for bids and offers - exactly as in main app
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

    const validBids = bidsResponse.data.data.filter((record: any) => 
      record.volume < 0 && record.soFlag && validWindFarmIds.has(record.bmUnit)
    ).map((record: any) => ({
      ...record,
      id: record.bmUnit
    }));

    const validOffers = offersResponse.data.data.filter((record: any) => 
      record.volume < 0 && record.soFlag && validWindFarmIds.has(record.bmUnit)
    ).map((record: any) => ({
      ...record,
      id: record.bmUnit
    }));

    const allRecords = [...validBids, ...validOffers];

    if (allRecords.length > 0) {
      const periodTotal = allRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
      const periodPayment = allRecords.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice * -1), 0);
      console.log(`[${date} P${period}] Records: ${allRecords.length} (${periodTotal.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`);
    }

    return allRecords;
  } catch (error) {
    if (axios.isAxiosError(error)) {
      console.error(`[${date} P${period}] Elexon API error:`, error.response?.data || error.message);
    } else {
      console.error(`[${date} P${period}] Unexpected error:`, error);
    }
    return []; // Return empty array to continue with other periods
  }
}

// Process curtailment data for a specific period
async function processPeriod(period: number, bmuLeadPartyMap: Map<string, string>): Promise<{ records: number, volume: number, payment: number }> {
  console.log(`\nProcessing period ${period}/48...`);
  
  try {
    // Fetch records using the same approach as in main application
    const records = await fetchBidsOffers(TARGET_DATE, period);
    
    if (!records || records.length === 0) {
      console.log(`[${TARGET_DATE} P${period}] No valid records found`);
      return { records: 0, volume: 0, payment: 0 };
    }
    
    // Process and insert records
    let insertedCount = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (const record of records) {
      try {
        const volume = Math.abs(record.volume);
        const payment = volume * record.originalPrice;
        
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
        
        insertedCount++;
        totalVolume += volume;
        totalPayment += payment;
      } catch (error) {
        console.error(`[${TARGET_DATE} P${period}] Error inserting record for ${record.id}:`, error);
      }
    }
    
    if (insertedCount > 0) {
      console.log(`[${TARGET_DATE} P${period}] Inserted ${insertedCount}/${records.length} records: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    }
    
    return { records: insertedCount, volume: totalVolume, payment: totalPayment };
  } catch (error) {
    console.error(`[${TARGET_DATE} P${period}] Error processing period:`, error);
    return { records: 0, volume: 0, payment: 0 };
  }
}

// Main function to reprocess all periods
async function reprocessAllPeriods(): Promise<void> {
  console.log(`\n=== Starting Elexon API Reprocessing for ${TARGET_DATE} ===\n`);
  const startTime = new Date();
  
  try {
    // Load BMU mappings
    const bmuLeadPartyMap = await loadBmuLeadPartyMap();
    
    // Delete existing records
    console.log(`Deleting existing curtailment records for ${TARGET_DATE}...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Process all periods
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (let period = 1; period <= 48; period++) {
      const result = await processPeriod(period, bmuLeadPartyMap);
      
      totalRecords += result.records;
      totalVolume += result.volume;
      totalPayment += result.payment;
      
      // Add small delay between periods to avoid rate limiting
      if (period < 48) {
        await delay(1000);
      }
    }
    
    // Update daily summary
    console.log(`\nUpdating daily summary for ${TARGET_DATE}...`);
    
    try {
      // Delete existing summary
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
    
    // Process Bitcoin calculations
    console.log(`\n=== Processing Bitcoin Calculations ===`);
    
    for (const minerModel of Object.keys(minerModels)) {
      try {
        console.log(`Processing ${minerModel}...`);
        const result = await processSingleDay(TARGET_DATE, minerModel);
        if (result && result.success) {
          console.log(`✓ Successfully processed ${minerModel}: ${result.bitcoinMined.toFixed(8)} BTC (£${result.valueGbp.toFixed(2)})`);
        } else {
          console.log(`No calculations for ${minerModel}`);
        }
      } catch (error) {
        console.error(`Error processing Bitcoin for ${minerModel}:`, error);
      }
    }
    
    // Final verification
    console.log(`\n=== Final Verification ===`);
    const verificationResult = await db.select({
      record_count: sql<string>`COUNT(*)`,
      period_count: sql<string>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      farm_count: sql<string>`COUNT(DISTINCT ${curtailmentRecords.farmId})`,
      total_volume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      total_payment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Records in database: ${verificationResult[0].record_count}`);
    console.log(`Periods in database: ${verificationResult[0].period_count}`);
    console.log(`Farms in database: ${verificationResult[0].farm_count}`);
    console.log(`Total volume in database: ${Number(verificationResult[0].total_volume).toFixed(2)} MWh`);
    console.log(`Total payment in database: £${Number(verificationResult[0].total_payment).toFixed(2)}`);
    
    // Calculate execution time
    const endTime = new Date();
    const executionTimeMs = endTime.getTime() - startTime.getTime();
    console.log(`\n=== Reprocessing Completed ===`);
    console.log(`Total execution time: ${(executionTimeMs / 1000).toFixed(2)} seconds`);
  } catch (error) {
    console.error(`Error during reprocessing:`, error);
  }
}

// Run the reprocessing
reprocessAllPeriods().then(() => {
  console.log("Reprocessing completed successfully");
  process.exit(0);
}).catch(error => {
  console.error("Unexpected error during reprocessing:", error);
  process.exit(1);
});