/**
 * Fetch Elexon BOA Data for 2025-04-16
 * 
 * This script fetches bid-offer acceptance data from Elexon API using
 * the correct endpoint and processes it to update curtailment records.
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
const REQUEST_TIMEOUT_MS = 60000; // 60 seconds
const MAX_RETRIES = 3;

// Delay function
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mapping to get valid wind farm IDs
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

// Fetch bid-offer data with retry logic
async function fetchBidOfferData(date: string, period: number, retryCount = 0): Promise<any[]> {
  try {
    // Use the correct endpoint for bid-offer data
    const url = `https://data.elexon.co.uk/bmrs/api/v1/balancing/bid-offer/all/settlement-period-all-bids-offers/${date}/${period}?format=json`;
    
    console.log(`Fetching from Elexon API: ${url}`);
    
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: REQUEST_TIMEOUT_MS
    });
    
    if (!response.data || !Array.isArray(response.data.data)) {
      throw new Error('Invalid API response format');
    }
    
    console.log(`[${date} P${period}] Received ${response.data.data.length} records`);
    
    return response.data.data;
  } catch (error) {
    if (axios.isAxiosError(error)) {
      console.error(`API error for period ${period}: ${error.message}`);
      
      if (retryCount < MAX_RETRIES) {
        const delayMs = 5000 * Math.pow(2, retryCount);
        console.log(`Retrying after ${delayMs}ms... (${retryCount + 1}/${MAX_RETRIES})`);
        await delay(delayMs);
        return fetchBidOfferData(date, period, retryCount + 1);
      }
    }
    
    console.error('Maximum retries reached or non-axios error');
    return []; // Return empty array after all retries
  }
}

// Process data for a specific settlement period
async function processPeriod(
  period: number, 
  validWindFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{ records: number, volume: number, payment: number }> {
  console.log(`\nProcessing period ${period}/48...`);
  
  try {
    // Fetch data from Elexon API
    const rawData = await fetchBidOfferData(TARGET_DATE, period);
    
    // Transform to format used by the application
    const records = rawData.map(item => ({
      settlementDate: TARGET_DATE,
      settlementPeriod: period,
      id: item.bmUnit,
      volume: item.volume,
      soFlag: item.soFlag === "Y" || item.soFlag === true,
      cadlFlag: item.cadlFlag === "Y" || item.cadlFlag === true ? true : 
               (item.cadlFlag === "N" || item.cadlFlag === false ? false : null),
      originalPrice: item.originalPrice,
      finalPrice: item.finalPrice
    }));
    
    // Filter for valid curtailment records (negative volume, SO or CADL flagged, valid wind farm)
    const validRecords = records.filter(record => 
      record.volume < 0 &&
      (record.soFlag || record.cadlFlag) &&
      validWindFarmIds.has(record.id)
    );
    
    console.log(`[P${period}] Found ${validRecords.length} valid curtailment records`);
    
    // Process and insert valid records
    let insertedCount = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (const record of validRecords) {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice;
      
      try {
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
        
        console.log(`[P${period}] Inserted ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
      } catch (error) {
        console.error(`[P${period}] Error inserting record for ${record.id}:`, error);
      }
    }
    
    // Period summary
    if (insertedCount > 0) {
      console.log(`[P${period}] Summary: ${insertedCount} records, ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    } else {
      console.log(`[P${period}] No valid records inserted`);
    }
    
    return { records: insertedCount, volume: totalVolume, payment: totalPayment };
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return { records: 0, volume: 0, payment: 0 };
  }
}

// Main function
async function processAllPeriods() {
  console.log(`\n=== Starting Reprocessing for ${TARGET_DATE} ===\n`);
  const startTime = new Date();
  
  try {
    // Load required mappings
    const validWindFarmIds = await loadWindFarmIds();
    const bmuLeadPartyMap = await loadBmuLeadPartyMap();
    
    // Delete existing records
    console.log(`Deleting existing curtailment records for ${TARGET_DATE}...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Process each settlement period
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (let period = 1; period <= 48; period++) {
      const result = await processPeriod(period, validWindFarmIds, bmuLeadPartyMap);
      
      totalRecords += result.records;
      totalVolume += result.volume;
      totalPayment += result.payment;
      
      // Add short delay between periods to avoid rate limiting
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
    const verificationQuery = await db.select({
      record_count: sql<string>`COUNT(*)`,
      period_count: sql<string>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      farm_count: sql<string>`COUNT(DISTINCT ${curtailmentRecords.farmId})`,
      total_volume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      total_payment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\n=== Data Verification ===`);
    console.log(`Records: ${verificationQuery[0].record_count}`);
    console.log(`Periods: ${verificationQuery[0].period_count}`);
    console.log(`Farms: ${verificationQuery[0].farm_count}`);
    console.log(`Total Volume: ${Number(verificationQuery[0].total_volume || 0).toFixed(2)} MWh`);
    console.log(`Total Payment: £${Number(verificationQuery[0].total_payment || 0).toFixed(2)}`);
    
    // Calculate execution time
    const endTime = new Date();
    const executionTimeMs = endTime.getTime() - startTime.getTime();
    console.log(`\n=== Reprocessing Completed ===`);
    console.log(`Total execution time: ${(executionTimeMs / 1000).toFixed(2)} seconds`);
  } catch (error) {
    console.error(`Reprocessing failed:`, error);
    process.exit(1);
  }
}

// Run the script
processAllPeriods().then(() => {
  console.log("Processing completed successfully");
  process.exit(0);
}).catch(error => {
  console.error("Unexpected error:", error);
  process.exit(1);
});