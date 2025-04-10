/**
 * Focused Curtailment Data Reingestion Script for April 2, 2025
 * 
 * This script focuses solely on fetching and storing curtailment records from Elexon API
 * for all 48 settlement periods on April 2, 2025.
 * 
 * It handles:
 * - Batch processing to avoid timeouts
 * - Duplicate prevention
 * - Validation and error handling
 * 
 * Usage: npx tsx reingest_april2_curtailment.ts
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';
import { format } from "date-fns";
import { eq, sql, and } from "drizzle-orm";
import pLimit from "p-limit";

// Constants
const TARGET_DATE = "2025-04-02";
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BATCH_SIZE = 5; // Process 5 periods at a time to avoid timeouts
const LOG_FILE_PATH = `./logs/reingest_april2_curtailment_${format(new Date(), "yyyy-MM-dd'T'HH-mm-ss")}.log`;
const MAX_RETRIES = 3;

// Configure logger
const logger = {
  log: (message: string) => {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] ${message}`;
    console.log(logMessage);
    fs.appendFile(LOG_FILE_PATH, logMessage + "\n").catch(console.error);
  },
  error: (message: string, error: any) => {
    const timestamp = new Date().toISOString();
    const errorMessage = `[${timestamp}] ERROR: ${message}: ${error?.message || error}`;
    console.error(errorMessage);
    fs.appendFile(LOG_FILE_PATH, errorMessage + "\n").catch(console.error);
    if (error?.stack) {
      fs.appendFile(LOG_FILE_PATH, `[${timestamp}] STACK: ${error.stack}\n`).catch(console.error);
    }
  }
};

// Initialize logger file
async function initLogger() {
  try {
    const logDir = path.dirname(LOG_FILE_PATH);
    try {
      await fs.mkdir(logDir, { recursive: true });
    } catch (err) {
      // Ignore if directory already exists
    }
    
    await fs.writeFile(LOG_FILE_PATH, `===== CURTAILMENT REINGESTION LOG FOR ${TARGET_DATE} =====\n\n`);
    logger.log(`Log file initialized at ${LOG_FILE_PATH}`);
  } catch (error) {
    console.error("Failed to initialize log file:", error);
    // Continue even if log file creation fails
  }
}

// BMU mapping for wind farms
let windFarmIds: Set<string> | null = null;

// Delay utility function
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mapping for wind farms
async function loadWindFarmIds(): Promise<Set<string>> {
  if (windFarmIds !== null) {
    return windFarmIds;
  }

  try {
    const __filename = fileURLToPath(import.meta.url);
    const __dirname = path.dirname(__filename);
    const bmuMappingPath = path.join(__dirname, "data/bmu_mapping.json");
    
    logger.log(`Loading BMU mapping from: ${bmuMappingPath}`);
    try {
      const mappingContent = await fs.readFile(bmuMappingPath, 'utf8');
      const bmuMapping = JSON.parse(mappingContent);
      windFarmIds = new Set(bmuMapping.map((bmu: any) => bmu.elexonBmUnit));
      logger.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
      return windFarmIds;
    } catch (err) {
      // If the bmu_mapping.json file doesn't exist, try with server/data/bmuMapping.json
      const serverBmuMappingPath = path.join(__dirname, "server/data/bmuMapping.json");
      logger.log(`First BMU mapping path not found, trying: ${serverBmuMappingPath}`);
      const mappingContent = await fs.readFile(serverBmuMappingPath, 'utf8');
      const bmuMapping = JSON.parse(mappingContent);
      windFarmIds = new Set(bmuMapping.map((bmu: any) => bmu.elexonBmUnit));
      logger.log(`Loaded ${windFarmIds.size} wind farm BMU IDs from server path`);
      return windFarmIds;
    }
  } catch (error) {
    logger.error('Error loading BMU mapping', error);
    throw error;
  }
}

// Interface for Elexon API response
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

// Fetch data from Elexon API for a specific date and period
async function fetchBidsOffers(date: string, period: number, retryCount = 0): Promise<ElexonBidOffer[]> {
  try {
    const validWindFarmIds = await loadWindFarmIds();
    logger.log(`Fetching data for ${date} Period ${period}...`);

    // Get bids and offers separately and merge them
    let allData: ElexonBidOffer[] = [];
    
    // Fetch bids
    const bidUrl = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`;
    const bidsResponse = await axios.get(bidUrl, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000
    });
    
    if (bidsResponse.data && Array.isArray(bidsResponse.data.data)) {
      allData = [...allData, ...bidsResponse.data.data];
    }
    
    // Wait briefly between calls to avoid rate limiting
    await delay(500);
    
    // Fetch offers
    const offerUrl = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`;
    const offersResponse = await axios.get(offerUrl, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000
    });
    
    if (offersResponse.data && Array.isArray(offersResponse.data.data)) {
      allData = [...allData, ...offersResponse.data.data];
    }

    // Filter to only include wind farm BMUs
    const windFarmData = allData.filter((item: ElexonBidOffer) => 
      item.bmUnit && validWindFarmIds.has(item.bmUnit)
    );

    logger.log(`Retrieved ${windFarmData.length} wind farm records for ${date} Period ${period}`);
    return windFarmData;
  } catch (error) {
    if (axios.isAxiosError(error)) {
      if (error.response?.status === 429) {
        // Rate limited - retry after a delay
        if (retryCount < MAX_RETRIES) {
          const delayTime = (retryCount + 1) * 60000; // Exponential backoff
          logger.log(`Rate limited for ${date} P${period}, retrying after ${delayTime/1000}s... (Attempt ${retryCount + 1}/${MAX_RETRIES})`);
          await delay(delayTime);
          return fetchBidsOffers(date, period, retryCount + 1);
        } else {
          logger.error(`Rate limit retry exhausted for ${date} P${period}`, error);
        }
      } else if (error.response?.status === 404) {
        logger.log(`No data available for ${date} P${period} (404 Not Found)`);
        return [];
      } else {
        logger.error(`API error for ${date} P${period}`, error);
      }
    } else {
      logger.error(`Unexpected error fetching data for ${date} P${period}`, error);
    }
    
    // Return empty array on failure after retries
    return [];
  }
}

// Process and store curtailment records for a specific date and period
async function processPeriodCurtailment(date: string, period: number): Promise<number> {
  try {
    const bidsOffers = await fetchBidsOffers(date, period);
    
    // Filter for valid curtailment records:
    // 1. Negative volume (curtailment)
    // 2. soFlag or cadlFlag is true
    const curtailmentData = bidsOffers.filter(record => 
      record.volume < 0 && (record.soFlag === true || record.cadlFlag === true)
    );
    
    if (curtailmentData.length === 0) {
      logger.log(`No valid curtailment records for ${date} Period ${period}`);
      return 0;
    }
    
    // Delete existing records for this date and period
    await db.delete(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          eq(curtailmentRecords.settlementPeriod, period)
        )
      );
    
    // Process and insert curtailment records
    const insertData = curtailmentData.map(record => ({
      settlementDate: date,
      settlementPeriod: period,
      farmId: record.bmUnit || "",
      leadPartyName: record.leadPartyName || "Unknown",
      volume: Math.abs(record.volume).toString(), // Store as positive number
      price: record.originalPrice.toString(),
      payment: (Math.abs(record.volume) * record.originalPrice).toString(),
      soFlag: record.soFlag,
      cadlFlag: record.cadlFlag || false,
      createdAt: new Date(),
      updatedAt: new Date()
    }));
    
    await db.insert(curtailmentRecords).values(insertData);
    logger.log(`Inserted ${insertData.length} curtailment records for ${date} Period ${period}`);
    
    return insertData.length;
  } catch (error) {
    logger.error(`Error processing period ${period} for ${date}`, error);
    return 0;
  }
}

// Process a batch of settlement periods
async function processBatch(date: string, periods: number[]): Promise<number> {
  const limit = pLimit(5); // Maximum 5 concurrent requests to avoid overloading
  
  const results = await Promise.all(
    periods.map(period => limit(() => processPeriodCurtailment(date, period)))
  );
  
  return results.reduce((sum, count) => sum + count, 0);
}

// Main function
async function main() {
  try {
    await initLogger();
    
    logger.log(`===== STARTING CURTAILMENT REINGESTION FOR ${TARGET_DATE} =====`);
    const startTime = Date.now();
    
    // First, check current state of data
    const existingData = await db
      .select({
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        recordCount: sql<number>`COUNT(*)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    logger.log(`Current state for ${TARGET_DATE}: ${existingData[0]?.periodCount || 0}/48 periods, ${existingData[0]?.recordCount || 0} records`);
    
    // Process all 48 settlement periods in batches to avoid timeouts
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1); // 1-48
    const batches = [];
    
    for (let i = 0; i < allPeriods.length; i += BATCH_SIZE) {
      batches.push(allPeriods.slice(i, i + BATCH_SIZE));
    }
    
    logger.log(`Processing ${batches.length} batches of settlement periods`);
    
    let totalRecords = 0;
    let batchNumber = 1;
    
    for (const batch of batches) {
      logger.log(`Processing batch ${batchNumber}/${batches.length}: Periods ${batch.join(', ')}`);
      const batchRecords = await processBatch(TARGET_DATE, batch);
      totalRecords += batchRecords;
      logger.log(`Completed batch ${batchNumber}/${batches.length}: Added ${batchRecords} records`);
      
      // Pause between batches to avoid rate limiting issues
      if (batchNumber < batches.length) {
        logger.log(`Pausing for 5 seconds before next batch...`);
        await delay(5000);
      }
      
      batchNumber++;
    }
    
    // Verify final results
    const finalData = await db
      .select({
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        recordCount: sql<number>`COUNT(*)`,
        totalEnergy: sql<string>`SUM(volume::numeric)`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const endTime = Date.now();
    const duration = ((endTime - startTime) / 1000 / 60).toFixed(2);
    
    logger.log(`
===== CURTAILMENT REINGESTION COMPLETED =====
Date: ${TARGET_DATE}
Duration: ${duration} minutes
Final state: ${finalData[0]?.periodCount || 0}/48 periods processed
Total records: ${finalData[0]?.recordCount || 0}
Total energy: ${parseFloat(finalData[0]?.totalEnergy || '0').toFixed(2)} MWh
Total payment: £${parseFloat(finalData[0]?.totalPayment || '0').toFixed(2)}
    `);
    
    process.exit(0);
  } catch (error) {
    logger.error(`Fatal error during reingestion process`, error);
    process.exit(1);
  }
}

// Execute main function
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});