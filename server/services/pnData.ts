/**
 * Physical Notification (PN) Data Service
 * 
 * This service fetches Physical Notification data from the Elexon API,
 * which represents the expected generation level for BMUs before curtailment
 * actions are taken.
 */

import { db } from "@db";
import { curtailmentRecords, physicalNotifications } from "@db/schema";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';
import { ElexonPhysicalNotification, ElexonPNResponse } from "../types/elexon";
import { eq, distinct, and, sql, desc, asc } from "drizzle-orm";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BMU_MAPPING_PATH = path.join(__dirname, "../data/bmuMapping.json");
const MAX_REQUESTS_PER_MINUTE = 4000; // Keep buffer below 5000 limit
const REQUEST_WINDOW_MS = 60000; // 1 minute in milliseconds
const MAX_BATCH_SIZE = 5; // Number of BMUs to fetch in each batch

// Track API requests to prevent rate limiting
let requestTimestamps: number[] = [];

/**
 * Wait if we're approaching the API rate limit
 */
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

/**
 * Track an API request for rate limiting
 */
function trackRequest() {
  const now = Date.now();
  requestTimestamps = [...requestTimestamps, now].filter(timestamp => 
    now - timestamp < REQUEST_WINDOW_MS
  );
}

/**
 * Get all unique BMU IDs from curtailment records
 */
export async function getUniqueBmuIds(): Promise<string[]> {
  try {
    const results = await db
      .select({ farmId: curtailmentRecords.farmId })
      .from(curtailmentRecords)
      .groupBy(curtailmentRecords.farmId);
    
    return results.map(record => record.farmId);
  } catch (error) {
    console.error('Error fetching unique BMU IDs:', error);
    throw error;
  }
}

/**
 * Make a request to the Elexon API with rate limiting
 */
async function makeRequest(url: string): Promise<any> {
  await waitForRateLimit();

  try {
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000 // 30 second timeout
    });

    trackRequest();
    return response.data;
  } catch (error) {
    if (axios.isAxiosError(error) && error.response?.status === 429) {
      console.log(`Rate limited, retrying after delay...`);
      await delay(60000); // Wait 1 minute on rate limit
      return makeRequest(url);
    }
    throw error;
  }
}

/**
 * Fetch Physical Notification data for a specific date range and BMU
 */
export async function fetchPhysicalNotifications(
  fromDate: string,
  toDate: string,
  settlementPeriodFrom: number,
  settlementPeriodTo: number,
  bmuId: string
): Promise<ElexonPhysicalNotification[]> {
  try {
    const url = `${ELEXON_BASE_URL}/datasets/PN/stream?from=${fromDate}&to=${toDate}&settlementPeriodFrom=${settlementPeriodFrom}&settlementPeriodTo=${settlementPeriodTo}&bmUnit=${bmuId}`;
    
    console.log(`Fetching PN data for ${bmuId} from ${fromDate} to ${toDate}`);
    const response = await makeRequest(url);
    
    // Return empty array if no data
    if (!response || !Array.isArray(response)) {
      console.log(`No PN data found for ${bmuId}`);
      return [];
    }

    console.log(`Received ${response.length} PN records for ${bmuId}`);
    return response;
  } catch (error) {
    console.error(`Error fetching PN data for ${bmuId}:`, error);
    throw error;
  }
}

/**
 * Store Physical Notification data in the database
 */
export async function storePhysicalNotifications(pnData: ElexonPhysicalNotification[]): Promise<number> {
  if (!pnData || pnData.length === 0) {
    return 0;
  }

  try {
    // Map to database fields
    const records = pnData.map(pn => ({
      settlementDate: pn.settlementDate,
      settlementPeriod: pn.settlementPeriod,
      timeFrom: new Date(pn.timeFrom),
      timeTo: new Date(pn.timeTo),
      levelFrom: String(pn.levelFrom),
      levelTo: String(pn.levelTo),
      nationalGridBmUnit: pn.nationalGridBmUnit,
      bmUnit: pn.bmUnit,
      leadPartyName: null // Will be populated separately
    }));

    // Batch insert the records
    const result = await db.insert(physicalNotifications).values(records)
      .onConflictDoUpdate({
        target: [
          physicalNotifications.settlementDate,
          physicalNotifications.settlementPeriod,
          physicalNotifications.bmUnit
        ],
        set: {
          levelFrom: sql`EXCLUDED.level_from`,
          levelTo: sql`EXCLUDED.level_to`,
          timeFrom: sql`EXCLUDED.time_from`,
          timeTo: sql`EXCLUDED.time_to`
        }
      });

    return records.length;
  } catch (error) {
    console.error('Error storing PN data:', error);
    throw error;
  }
}

/**
 * Process a batch of BMUs for a date range
 */
export async function processPNDataBatch(
  fromDate: string,
  toDate: string,
  bmuIds: string[]
): Promise<{
  totalFetched: number;
  totalStored: number;
  failedBmus: string[];
}> {
  let totalFetched = 0;
  let totalStored = 0;
  const failedBmus: string[] = [];

  for (const bmuId of bmuIds) {
    try {
      // Fetch data for each BMU
      const pnData = await fetchPhysicalNotifications(
        fromDate,
        toDate,
        1, // First settlement period
        48, // Last settlement period
        bmuId
      );

      totalFetched += pnData.length;

      // Store the data
      if (pnData.length > 0) {
        const stored = await storePhysicalNotifications(pnData);
        totalStored += stored;
        console.log(`Stored ${stored} PN records for ${bmuId}`);
      }
    } catch (error) {
      console.error(`Failed to process PN data for ${bmuId}:`, error);
      failedBmus.push(bmuId);
    }

    // Add a small delay between BMUs to avoid overwhelming the API
    await delay(1000);
  }

  return { totalFetched, totalStored, failedBmus };
}

/**
 * Process all BMUs for a month
 */
export async function processMonthData(yearMonth: string): Promise<{
  totalBmus: number;
  totalFetched: number;
  totalStored: number;
  failedBmus: string[];
}> {
  // Get all unique BMU IDs from curtailment records
  const bmuIds = await getUniqueBmuIds();
  
  // Determine the date range
  const year = parseInt(yearMonth.split('-')[0]);
  const month = parseInt(yearMonth.split('-')[1]);
  const fromDate = `${year}-${month.toString().padStart(2, '0')}-01`;
  
  // Calculate the last day of the month
  const lastDay = new Date(year, month, 0).getDate();
  const toDate = `${year}-${month.toString().padStart(2, '0')}-${lastDay}`;
  
  console.log(`Processing PN data for ${yearMonth} (${fromDate} to ${toDate})`);
  console.log(`Found ${bmuIds.length} BMUs to process`);

  let totalFetched = 0;
  let totalStored = 0;
  let failedBmus: string[] = [];

  // Process BMUs in batches
  for (let i = 0; i < bmuIds.length; i += MAX_BATCH_SIZE) {
    const batchBmuIds = bmuIds.slice(i, i + MAX_BATCH_SIZE);
    console.log(`Processing batch ${Math.floor(i / MAX_BATCH_SIZE) + 1} of ${Math.ceil(bmuIds.length / MAX_BATCH_SIZE)}: ${batchBmuIds.join(', ')}`);
    
    const batchResult = await processPNDataBatch(fromDate, toDate, batchBmuIds);
    
    totalFetched += batchResult.totalFetched;
    totalStored += batchResult.totalStored;
    failedBmus = [...failedBmus, ...batchResult.failedBmus];
    
    console.log(`Batch complete: Fetched ${batchResult.totalFetched}, Stored ${batchResult.totalStored}, Failed ${batchResult.failedBmus.length}`);
    
    // Add a small delay between batches
    await delay(2000);
  }

  return {
    totalBmus: bmuIds.length,
    totalFetched,
    totalStored,
    failedBmus
  };
}

/**
 * Update lead party names for Physical Notifications
 */
export async function updatePNLeadPartyNames(): Promise<number> {
  try {
    // Get lead party names from curtailment records
    const leadPartyMappings = await db
      .select({
        farmId: curtailmentRecords.farmId,
        leadPartyName: curtailmentRecords.leadPartyName
      })
      .from(curtailmentRecords)
      .groupBy(curtailmentRecords.farmId, curtailmentRecords.leadPartyName);
    
    let updatedCount = 0;
    
    // Update each BMU's lead party name
    for (const mapping of leadPartyMappings) {
      if (!mapping.leadPartyName) continue;
      
      const result = await db
        .update(physicalNotifications)
        .set({ leadPartyName: mapping.leadPartyName })
        .where(
          and(
            eq(physicalNotifications.bmUnit, mapping.farmId),
            sql`${physicalNotifications.leadPartyName} IS NULL`
          )
        );
      
      updatedCount += 1; // Assuming one update per BMU
    }
    
    return updatedCount;
  } catch (error) {
    console.error('Error updating PN lead party names:', error);
    throw error;
  }
}

/**
 * Simple delay function
 */
export async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}