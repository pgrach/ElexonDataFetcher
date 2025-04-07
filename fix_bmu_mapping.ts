/**
 * BMU Mapping Fix Script
 * 
 * This script fixes the BMU mapping inconsistency by ensuring both Elexon API calls
 * and data processing use the same valid BMU IDs. It uses the server mapping file
 * which has 208 entries rather than the root mapping file with 32 entries.
 */

import { db } from './db';
import { curtailmentRecords, dailySummaries } from './db/schema';
import { eq, sql } from 'drizzle-orm';
import fs from 'fs/promises';
import path from 'path';
import { format, addMinutes } from 'date-fns';
import axios from 'axios';

// Configuration
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 2000;
const API_RATE_LIMIT_DELAY_MS = 500;
const SMALL_BATCH_SIZE = 4; // Number of periods to process in parallel

// Constants
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const SERVER_BMU_MAPPING_PATH = path.join('server', 'data', 'bmuMapping.json');
const MAX_REQUESTS_PER_MINUTE = 4500; // Keep buffer below 5000 limit
const REQUEST_WINDOW_MS = 60000; // 1 minute in milliseconds

// Cache for BMU mapping
let bmuMapping: any[] = [];
let windFarmIds: Set<string> | null = null;
let requestTimestamps: number[] = [];

/**
 * Simple delay function
 */
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Track API request to manage rate limiting
 */
function trackRequest() {
  const now = Date.now();
  requestTimestamps = [...requestTimestamps, now].filter(timestamp => 
    now - timestamp < REQUEST_WINDOW_MS
  );
}

/**
 * Wait if we're approaching rate limits
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
 * Make API request with rate limiting
 */
async function makeRequest(url: string, date: string, period: number): Promise<any> {
  await waitForRateLimit();

  try {
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000 // 30 second timeout
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

/**
 * Load the BMU mapping file from the server directory
 */
async function loadBmuMapping(): Promise<any[]> {
  if (bmuMapping.length > 0) return bmuMapping;
  
  try {
    console.log(`Loading BMU mapping from ${SERVER_BMU_MAPPING_PATH}...`);
    const mappingFile = await fs.readFile(SERVER_BMU_MAPPING_PATH, 'utf-8');
    const mappingData = JSON.parse(mappingFile);
    bmuMapping = mappingData;
    console.log(`Loaded ${mappingData.length} BMU mappings from server data`);
    return mappingData;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

/**
 * Load valid wind farm IDs
 */
async function loadWindFarmIds(): Promise<Set<string>> {
  if (windFarmIds !== null) {
    return windFarmIds;
  }

  try {
    const bmuMapping = await loadBmuMapping();
    windFarmIds = new Set(bmuMapping.map((bmu: any) => bmu.elexonBmUnit));
    console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
    return windFarmIds;
  } catch (error) {
    console.error('Error loading wind farm IDs:', error);
    throw error;
  }
}

/**
 * Fetch bids and offers from Elexon API
 */
async function fetchBidsOffers(date: string, period: number): Promise<any[]> {
  try {
    const validWindFarmIds = await loadWindFarmIds();

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

    const bids = bidsResponse.data.data || [];
    const offers = offersResponse.data.data || [];
    
    console.log(`[${date} P${period}] Raw records - Bids: ${bids.length}, Offers: ${offers.length}`);
    
    // Extract just wind farm records without filtering for negative volume/flags yet
    const windFarmBids = bids.filter((record: any) => validWindFarmIds.has(record.id));
    const windFarmOffers = offers.filter((record: any) => validWindFarmIds.has(record.id));
    
    console.log(`[${date} P${period}] Wind farm records - Bids: ${windFarmBids.length}, Offers: ${windFarmOffers.length}`);

    // Now filter for curtailment conditions
    const validBids = windFarmBids.filter((record: any) => record.volume < 0 && record.soFlag);
    const validOffers = windFarmOffers.filter((record: any) => record.volume < 0 && record.soFlag);

    console.log(`[${date} P${period}] Valid curtailment records - Bids: ${validBids.length}, Offers: ${validOffers.length}`);

    const allRecords = [...validBids, ...validOffers];

    // Add additional info from BMU mapping
    if (allRecords.length > 0) {
      // Add BMU details from mapping
      const bmuDetails = await loadBmuMapping();
      const bmuMap = new Map();
      
      for (const bmu of bmuDetails) {
        bmuMap.set(bmu.elexonBmUnit, {
          name: bmu.bmUnitName,
          leadParty: bmu.leadPartyName
        });
      }
      
      // Enrich records with BMU details
      for (const record of allRecords) {
        const details = bmuMap.get(record.id);
        if (details) {
          record.bmUnitName = details.name;
          record.leadPartyName = details.leadParty;
        }
      }
      
      // Calculate totals
      const periodTotal = allRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
      // Ensure payment exists and is a number
      const periodPayment = allRecords.reduce((sum, r) => {
        const payment = typeof r.payment === 'number' ? r.payment : 
                      (r.originalPrice ? r.originalPrice * Math.abs(r.volume) : 0);
        return sum + Math.abs(payment);
      }, 0);
      console.log(`[${date} P${period}] Final records: ${allRecords.length} (${periodTotal.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`);
    }

    return allRecords;
  } catch (error) {
    if (axios.isAxiosError(error)) {
      console.error(`[${date} P${period}] Elexon API error:`, error.response?.data || error.message);
      throw new Error(`Elexon API error: ${error.response?.data?.error || error.message}`);
    }
    console.error(`[${date} P${period}] Unexpected error:`, error);
    throw error;
  }
}

/**
 * Process a single settlement period with retries
 */
async function processSettlementPeriod(
  date: string, 
  period: number,
  retryCount: number = 0
): Promise<{
  records: number;
  volume: number;
  payment: number;
}> {
  try {
    const records = await fetchBidsOffers(date, period);
    
    if (!records || records.length === 0) {
      console.log(`[${date} P${period}] No valid curtailment records found`);
      return { records: 0, volume: 0, payment: 0 };
    }
    
    // Log the records we're about to process
    const totalVolume = records.reduce((sum, r) => sum + Math.abs(r.volume), 0);
    // Ensure payment exists and is a number
    const totalPayment = records.reduce((sum, r) => {
      const payment = typeof r.payment === 'number' ? r.payment : 
                    (r.originalPrice ? r.originalPrice * Math.abs(r.volume) : 0);
      return sum + Math.abs(payment);
    }, 0);
    console.log(`[${date} P${period}] Processing: ${records.length} records (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`);
    
    // Insert all records in a single batch
    const batchInserts = records.map(record => {
      // Ensure payment exists and is a number
      const payment = typeof record.payment === 'number' ? record.payment : 
                    (record.originalPrice ? record.originalPrice * Math.abs(record.volume) : 0);
      
      // Store payment values as negative in the database (they're positive in API)
      const paymentValue = -Math.abs(payment);
      
      // Calculate price per MWh (£/MWh)
      const originalPrice = Math.abs(payment) / Math.abs(record.volume);
      const finalPrice = originalPrice;
      
      return {
        settlementDate: date,
        settlementPeriod: period,
        farmId: record.id, // This matches the farmId in the schema
        volume: record.volume.toString(),
        payment: paymentValue.toString(),
        originalPrice: originalPrice.toString(),
        finalPrice: finalPrice.toString(),
        leadPartyName: record.leadPartyName || null,
        soFlag: record.soFlag || false,
        cadlFlag: record.cadlFlag || false
      };
    });
    
    // Insert all records
    await db.insert(curtailmentRecords).values(batchInserts);
    
    return {
      records: records.length,
      volume: totalVolume,
      payment: totalPayment
    };
  } catch (error) {
    console.error(`Error processing period ${period} for ${date}:`, error);
    
    // Retry logic
    if (retryCount < MAX_RETRIES) {
      console.log(`Retrying period ${period} for ${date} (attempt ${retryCount + 1} of ${MAX_RETRIES})...`);
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
      return processSettlementPeriod(date, period, retryCount + 1);
    }
    
    console.error(`Failed to process period ${period} for ${date} after ${MAX_RETRIES} attempts`);
    return { records: 0, volume: 0, payment: 0 };
  }
}

/**
 * Process all 48 settlement periods for a specific date
 */
export async function processAllPeriods(date: string): Promise<{
  totalRecords: number;
  totalPeriods: number;
  totalVolume: number;
  totalPayment: number;
}> {
  console.log(`\n=== Processing All Periods for ${date} Using Server BMU Mapping ===\n`);
  
  // First, clear existing records for the date to avoid duplicates
  console.log(`Clearing existing records for ${date}...`);
  await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  // Process in small batches to avoid hitting API rate limits
  let totalRecords = 0;
  let periodsProcessed = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  
  // Create a queue of all 48 periods
  const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
  
  // Process in small batches
  for (let i = 0; i < allPeriods.length; i += SMALL_BATCH_SIZE) {
    const batchPeriods = allPeriods.slice(i, i + SMALL_BATCH_SIZE);
    console.log(`Processing periods ${batchPeriods.join(', ')}...`);
    
    // Process each period in the batch concurrently
    const batchResults = await Promise.all(
      batchPeriods.map(async period => {
        // Add a small delay between API calls to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, API_RATE_LIMIT_DELAY_MS));
        return processSettlementPeriod(date, period);
      })
    );
    
    // Aggregate results from this batch
    for (const result of batchResults) {
      if (result.records > 0) {
        totalRecords += result.records;
        periodsProcessed++;
        totalVolume += result.volume;
        totalPayment += result.payment;
      }
    }
    
    console.log(`Progress: ${periodsProcessed}/48 periods processed (${totalRecords} records)`);
    
    // Add a delay between batches to avoid API rate limits
    if (i + SMALL_BATCH_SIZE < allPeriods.length) {
      console.log(`Waiting ${API_RATE_LIMIT_DELAY_MS * 3}ms before next batch...`);
      await new Promise(resolve => setTimeout(resolve, API_RATE_LIMIT_DELAY_MS * 3));
    }
  }
  
  // Update daily summary
  if (totalRecords > 0) {
    console.log(`\nUpdating daily summary for ${date}...`);
    
    // Payment values are stored as negative in the database, but displayed as positive in logs
    await db.insert(dailySummaries).values({
      summaryDate: date,
      totalCurtailedEnergy: totalVolume.toString(),
      totalPayment: (-totalPayment).toString(), // Store as negative in the database
      totalWindGeneration: '0',
      windOnshoreGeneration: '0',
      windOffshoreGeneration: '0',
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totalVolume.toString(),
        totalPayment: (-totalPayment).toString(), // Store as negative in the database
        lastUpdated: new Date()
      }
    });
    
    console.log(`Daily summary updated for ${date}:`);
    console.log(`- Energy: ${totalVolume.toFixed(2)} MWh`);
    console.log(`- Payment: £${totalPayment.toFixed(2)}`);
  } else {
    console.log(`\nNo valid curtailment records found for ${date}`);
    console.log(`Checking if daily summary exists...`);
    
    // Check if daily summary exists for this date
    const existingSummary = await db.select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date))
      .limit(1);
    
    if (existingSummary.length > 0) {
      console.log(`Daily summary exists for ${date} but no curtailment records found.`);
      console.log(`This suggests there might be data inconsistency.`);
      console.log(`Consider updating the summary or investigating further.`);
    }
  }
  
  console.log(`\n=== Processing Summary for ${date} ===`);
  console.log(`Total Records: ${totalRecords}`);
  console.log(`Periods Processed: ${periodsProcessed}/48`);
  console.log(`Total Volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`Total Payment: £${totalPayment.toFixed(2)}`);
  
  return {
    totalRecords,
    totalPeriods: periodsProcessed,
    totalVolume,
    totalPayment
  };
}

/**
 * Main function
 */
async function main() {
  try {
    // Get the date from command-line arguments or use default
    const dateToProcess = process.argv[2] || format(new Date(), 'yyyy-MM-dd');
    
    console.log(`\n=== Starting BMU Mapping Fix for ${dateToProcess} ===\n`);
    console.log(`This script will use the server BMU mapping file with 208 entries\n`);
    
    const result = await processAllPeriods(dateToProcess);
    
    console.log(`\n=== Processing Complete for ${dateToProcess} ===\n`);
    
    if (result.totalPeriods > 0) {
      console.log(`Next steps:`);
      console.log(`1. Process Bitcoin calculations with optimized DynamoDB access:`);
      console.log(`   npx tsx process_bitcoin_optimized.ts ${dateToProcess}`);
      console.log(`2. Or update the full cascade (Bitcoin, monthly, yearly summaries):`);
      console.log(`   npx tsx process_complete_cascade.ts ${dateToProcess}`);
    } else {
      console.log(`No curtailment data found for ${dateToProcess}`);
      console.log(`Please check the Elexon API and BMU mapping files for issues.`);
    }
  } catch (error) {
    console.error('Error processing all periods:', error);
    process.exit(1);
  }
}

// Run the script
main();