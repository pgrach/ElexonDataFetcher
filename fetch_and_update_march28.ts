/**
 * Fetch and Update March 28, 2025 Data
 * 
 * This script:
 * 1. Fetches all data from Elexon API for 2025-03-28
 * 2. Uses both bid and offer endpoints to ensure complete data
 * 3. Stores the data in the curtailment_records table
 * 4. Updates all summaries with the correct values
 */

import { db } from './db';
import { and, eq, sql } from 'drizzle-orm';
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from './db/schema';
import axios from 'axios';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';

// Configuration
const DATE_TO_FETCH = '2025-03-28';
const LOG_FILE = `fetch_and_update_${DATE_TO_FETCH}.log`;
const ELEXON_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'bmuMapping.json');

// Rate limiting for Elexon API
const MAX_REQUESTS_PER_MINUTE = 25;
const REQUEST_WINDOW_MS = 60000; // 1 minute
let requestTimestamps: number[] = [];

// Helper function to log to file
function log(message: string, level: "info" | "error" | "warning" | "success" = "info"): void {
  const timestamp = new Date().toISOString().replace('T', ' ').split('.')[0];
  const logMessage = `[${timestamp}] ${message}\n`;
  fs.appendFileSync(LOG_FILE, logMessage);
  
  // Also log to console with colors for better visibility
  const colors = {
    info: '\x1b[36m', // Cyan
    error: '\x1b[31m', // Red
    warning: '\x1b[33m', // Yellow
    success: '\x1b[32m', // Green
    reset: '\x1b[0m' // Reset
  };
  
  console.log(`${colors[level]}${message}${colors.reset}`);
}

// Helper function for delays
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Rate limiting function
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
    log(`Rate limit reached, waiting ${Math.ceil(waitTime/1000)}s...`, "warning");
    await delay(waitTime + 100); // Add 100ms buffer
    return waitForRateLimit(); // Recheck after waiting
  }
}

async function makeRequest(url: string, period: number, retryCount = 0): Promise<any> {
  try {
    await waitForRateLimit();
    
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000 // 30 second timeout
    });
    
    trackRequest();
    return response;
  } catch (error) {
    if (axios.isAxiosError(error)) {
      if (error.response?.status === 429 && retryCount < 3) {
        log(`[P${period}] Rate limited, retrying after delay...`, "warning");
        await delay(60000); // Wait 1 minute on rate limit
        return makeRequest(url, period, retryCount + 1);
      } else if (error.code === 'ECONNABORTED' && retryCount < 3) {
        log(`[P${period}] Request timeout, retrying...`, "warning");
        await delay(5000); // Wait 5 seconds
        return makeRequest(url, period, retryCount + 1);
      } else if (retryCount < 3) {
        log(`[P${period}] API error (${error.response?.status}), retrying...`, "warning");
        await delay(5000);
        return makeRequest(url, period, retryCount + 1);
      }
      
      log(`[P${period}] API error after retries: ${error.message}`, "error");
      throw error;
    }
    
    log(`[P${period}] Unexpected error: ${error}`, "error");
    throw error;
  }
}

async function loadBmuMappings(): Promise<{
  windFarmIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}> {
  try {
    log(`Loading BMU mapping from: ${BMU_MAPPING_PATH}`, "info");
    const mappingContent = fs.readFileSync(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    
    const windFarmIds = new Set(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => bmu.elexonBmUnit)
    );
    
    const bmuLeadPartyMap = new Map(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => [bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown'])
    );
    
    log(`Found ${windFarmIds.size} wind farm BMUs`, "success");
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    log(`Error loading BMU mapping: ${error}`, "error");
    throw error;
  }
}

async function fetchPeriodData(period: number, windFarmIds: Set<string>): Promise<any[]> {
  try {
    // Make parallel requests for both bids and offers to ensure complete data
    const bidUrl = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${DATE_TO_FETCH}/${period}`;
    const offerUrl = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${DATE_TO_FETCH}/${period}`;
    
    log(`[P${period}] Fetching data from Elexon API...`, "info");
    
    const [bidsResponse, offersResponse] = await Promise.all([
      makeRequest(bidUrl, period).catch(error => {
        log(`[P${period}] Bid endpoint error: ${error.message}`, "warning");
        return { data: { data: [] } };
      }),
      makeRequest(offerUrl, period).catch(error => {
        log(`[P${period}] Offer endpoint error: ${error.message}`, "warning");
        return { data: { data: [] } };
      })
    ]);
    
    const bidsData = bidsResponse.data?.data || [];
    const offersData = offersResponse.data?.data || [];
    
    log(`[P${period}] Retrieved ${bidsData.length} bids and ${offersData.length} offers`, "info");
    
    // Filter to keep only valid wind farm records
    const validBids = bidsData.filter((record: any) => 
      record.volume < 0 && 
      (record.soFlag || record.cadlFlag) && 
      windFarmIds.has(record.id)
    );
    
    const validOffers = offersData.filter((record: any) => 
      record.volume < 0 && 
      (record.soFlag || record.cadlFlag) && 
      windFarmIds.has(record.id)
    );
    
    // Combine all valid records
    const validRecords = [...validBids, ...validOffers];
    
    // Log period stats
    const periodVolume = validRecords.reduce((sum: number, record: any) => sum + Math.abs(record.volume), 0);
    const periodPayment = validRecords.reduce((sum: number, record: any) => sum + (Math.abs(record.volume) * record.originalPrice), 0);
    
    log(`[P${period}] Valid records: ${validRecords.length} (${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`, 
      validRecords.length > 0 ? "success" : "warning");
    
    return validRecords;
  } catch (error) {
    log(`[P${period}] Error fetching data: ${error}`, "error");
    throw error;
  }
}

async function processPeriod(
  period: number, 
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  success: boolean;
  records: number;
  volume: number;
  payment: number;
}> {
  try {
    // Fetch data for this period
    const validRecords = await fetchPeriodData(period, windFarmIds);
    
    // Clear existing records for this period to avoid duplicates
    try {
      await db.delete(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, DATE_TO_FETCH),
            eq(curtailmentRecords.settlementPeriod, period)
          )
        );
      
      log(`[P${period}] Cleared existing records before insertion`, "info");
    } catch (error) {
      log(`[P${period}] Error clearing existing records: ${error}`, "error");
      return { success: false, records: 0, volume: 0, payment: 0 };
    }
    
    // Track totals for reporting
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Create records for insertion
    const recordsToInsert = validRecords.map((record: any) => {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice;
      
      // Track totals
      totalVolume += volume;
      totalPayment += payment;
      
      return {
        settlementDate: DATE_TO_FETCH,
        settlementPeriod: period,
        farmId: record.id,
        leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
        volume: record.volume.toString(), // Keep negative value
        payment: payment.toString(),
        originalPrice: record.originalPrice.toString(),
        finalPrice: record.finalPrice.toString(),
        soFlag: record.soFlag,
        cadlFlag: record.cadlFlag
      };
    });
    
    // Insert records into the database
    if (recordsToInsert.length > 0) {
      try {
        await db.insert(curtailmentRecords).values(recordsToInsert);
        
        // Log individual record details for visibility
        for (const record of validRecords) {
          const volume = Math.abs(record.volume);
          const payment = volume * record.originalPrice;
          log(`[P${period}] Added record for ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
        }
        
        log(`[P${period}] Successfully inserted ${recordsToInsert.length} records`, "success");
      } catch (error) {
        log(`[P${period}] Error inserting records: ${error}`, "error");
        return { success: false, records: 0, volume: 0, payment: 0 };
      }
    } else {
      log(`[P${period}] No valid records to insert`, "warning");
    }
    
    return {
      success: true,
      records: recordsToInsert.length,
      volume: totalVolume,
      payment: totalPayment
    };
  } catch (error) {
    log(`[P${period}] Error processing period: ${error}`, "error");
    return { success: false, records: 0, volume: 0, payment: 0 };
  }
}

async function processDate(): Promise<void> {
  try {
    // Load wind farm mappings
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    log(`Starting fetch and update process for ${DATE_TO_FETCH}`, "info");
    
    // Process all 48 settlement periods
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    let failedPeriods = 0;
    
    // Process periods in batches of 6 to balance speed and rate limiting
    const BATCH_SIZE = 6;
    for (let startPeriod = 1; startPeriod <= 48; startPeriod += BATCH_SIZE) {
      const endPeriod = Math.min(startPeriod + BATCH_SIZE - 1, 48);
      log(`Processing batch of periods ${startPeriod}-${endPeriod}`, "info");
      
      const periodPromises = [];
      for (let period = startPeriod; period <= endPeriod; period++) {
        // Slightly stagger requests to avoid bunching
        await delay(500);
        periodPromises.push(processPeriod(period, windFarmIds, bmuLeadPartyMap));
      }
      
      const results = await Promise.all(periodPromises);
      
      // Tally results
      for (const result of results) {
        if (result.success) {
          totalRecords += result.records;
          totalVolume += result.volume;
          totalPayment += result.payment;
        } else {
          failedPeriods++;
        }
      }
      
      // Brief pause between batches
      await delay(2000);
    }
    
    log(`\nFetch and process complete:`, "success");
    log(`Total records: ${totalRecords}`, "success");
    log(`Total volume: ${totalVolume.toFixed(2)} MWh`, "success");
    log(`Total payment: £${totalPayment.toFixed(2)}`, "success");
    
    if (failedPeriods > 0) {
      log(`Failed periods: ${failedPeriods}`, "warning");
    }
    
    // Update daily summary
    await db.insert(dailySummaries)
      .values({
        summaryDate: DATE_TO_FETCH,
        totalCurtailedEnergy: totalVolume.toString(),
        totalPayment: (-totalPayment).toString(), // Store as negative value per convention
        lastUpdated: new Date()
      })
      .onConflictDoUpdate({
        target: [dailySummaries.summaryDate],
        set: {
          totalCurtailedEnergy: totalVolume.toString(),
          totalPayment: (-totalPayment).toString(),
          lastUpdated: new Date()
        }
      });
    
    log(`Updated daily summary for ${DATE_TO_FETCH}`, "success");
    
    // Update monthly summary for March 2025
    const yearMonth = DATE_TO_FETCH.substring(0, 7); // '2025-03'
    
    // Recalculate monthly totals
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${DATE_TO_FETCH}::date)`);
    
    if (monthlyTotals[0].totalCurtailedEnergy && monthlyTotals[0].totalPayment) {
      // Update monthly summary
      await db.insert(monthlySummaries)
        .values({
          yearMonth,
          totalCurtailedEnergy: String(monthlyTotals[0].totalCurtailedEnergy),
          totalPayment: String(monthlyTotals[0].totalPayment),
          updatedAt: new Date()
        })
        .onConflictDoUpdate({
          target: [monthlySummaries.yearMonth],
          set: {
            totalCurtailedEnergy: String(monthlyTotals[0].totalCurtailedEnergy),
            totalPayment: String(monthlyTotals[0].totalPayment),
            updatedAt: new Date()
          }
        });
      
      log(`Updated monthly summary for ${yearMonth}`, "success");
    }
    
    // Update yearly summary for 2025
    const year = DATE_TO_FETCH.substring(0, 4); // '2025'
    
    // Recalculate yearly totals
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${DATE_TO_FETCH}::date)`);
    
    if (yearlyTotals[0].totalCurtailedEnergy && yearlyTotals[0].totalPayment) {
      // Update yearly summary
      await db.insert(yearlySummaries)
        .values({
          year,
          totalCurtailedEnergy: String(yearlyTotals[0].totalCurtailedEnergy),
          totalPayment: String(yearlyTotals[0].totalPayment),
          updatedAt: new Date()
        })
        .onConflictDoUpdate({
          target: [yearlySummaries.year],
          set: {
            totalCurtailedEnergy: String(yearlyTotals[0].totalCurtailedEnergy),
            totalPayment: String(yearlyTotals[0].totalPayment),
            updatedAt: new Date()
          }
        });
      
      log(`Updated yearly summary for ${year}`, "success");
    }
    
    // Verify results match expected Elexon API total
    const expectedElexonTotal = 3784089.62;
    const percentageOfExpected = (totalPayment / expectedElexonTotal) * 100;
    
    log(`\nExpected Elexon API payment: £${expectedElexonTotal.toFixed(2)}`, "info");
    log(`Actual payment: £${totalPayment.toFixed(2)} (${percentageOfExpected.toFixed(2)}% of expected)`, 
      percentageOfExpected >= 95 ? "success" : "warning");
    
    if (percentageOfExpected < 95) {
      log(`NOTE: There's still a discrepancy with the expected Elexon API total.`, "warning");
      log(`This may be due to:`, "warning");
      log(`- Different calculation methodology`, "warning");
      log(`- Changes in API data since the expected value was calculated`, "warning");
      log(`- Some data not being available through the API anymore`, "warning");
    }
    
  } catch (error) {
    log(`Error processing date: ${error}`, "error");
    throw error;
  }
}

// Run the process
(async () => {
  log(`Starting fetch and update script for ${DATE_TO_FETCH}\n`, "info");
  
  try {
    await processDate();
    log(`\nScript completed successfully!`, "success");
  } catch (error) {
    log(`\nScript failed: ${error}`, "error");
    process.exit(1);
  }
})();