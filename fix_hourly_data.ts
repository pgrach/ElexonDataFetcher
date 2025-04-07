/**
 * Fix Hourly Data
 * 
 * This script processes all 48 periods for a specific date to ensure complete hourly data
 * in the database. It avoids using DynamoDB for difficulty data, using a default value instead.
 */

import { db } from './db';
import { curtailmentRecords, dailySummaries } from './db/schema';
import { eq, and, sql } from 'drizzle-orm';
import { format } from 'date-fns';
import fs from 'fs';
import path from 'path';
import axios from 'axios';

// Elexon API config
const ELEXON_BASE_URL = 'https://api.bmreports.com/BMRS';
const ELEXON_API_KEY = process.env.ELEXON_API_KEY || 'live';
const API_VERSION = 'v1';

// Constants
const DEFAULT_DIFFICULTY = 71e12; // Default difficulty if none found

/**
 * Load the BMU mapping file
 */
async function loadBmuMapping(): Promise<any[]> {
  try {
    const bmuMappingPath = path.join(process.cwd(), 'server/data/bmuMapping.json');
    console.log(`Loading BMU mapping from ${bmuMappingPath}...`);
    
    if (!fs.existsSync(bmuMappingPath)) {
      console.error(`BMU mapping file not found at: ${bmuMappingPath}`);
      return [];
    }
    
    const bmuMappingData = fs.readFileSync(bmuMappingPath, 'utf8');
    const bmuMapping = JSON.parse(bmuMappingData);
    
    console.log(`Loaded ${Object.keys(bmuMapping).length} BMU mappings`);
    return bmuMapping;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    return [];
  }
}

/**
 * Filter for valid wind farm BMUs
 */
async function loadWindFarmIds(): Promise<Set<string>> {
  try {
    const bmuMapping = await loadBmuMapping();
    if (!bmuMapping || Object.keys(bmuMapping).length === 0) {
      console.error('Failed to load valid BMU IDs');
      return new Set();
    }
    
    // The mapping includes BMU IDs as keys
    const validWindFarmIds = new Set(Object.keys(bmuMapping));
    console.log(`Loaded ${validWindFarmIds.size} wind farm BMU IDs`);
    return validWindFarmIds;
  } catch (error) {
    console.error('Error loading wind farm IDs:', error);
    return new Set();
  }
}

/**
 * Fetch curtailment data from Elexon API
 */
async function fetchCurtailmentData(date: string, period: number): Promise<any[]> {
  try {
    const url = `${ELEXON_BASE_URL}/DISBSAD/${API_VERSION}`;
    
    const params = {
      APIKey: ELEXON_API_KEY,
      SettlementDate: date,
      SettlementPeriod: period,
      ServiceType: 'xml'
    };
    
    // Add a small delay to avoid API rate limiting
    await new Promise(resolve => setTimeout(resolve, 500));
    
    const response = await axios.get(url, { params });
    
    if (!response.data || !response.data.response || !response.data.response.responseBody) {
      console.error(`No data returned from Elexon API for ${date} P${period}`);
      return [];
    }
    
    const records = response.data.response.responseBody.responseList?.item || [];
    return Array.isArray(records) ? records : [records];
  } catch (error) {
    console.error(`Error fetching data from Elexon API for ${date} P${period}:`, error);
    return [];
  }
}

/**
 * Process a single settlement period
 */
async function processSettlementPeriod(date: string, period: number, validWindFarmIds: Set<string>): Promise<{
  recordsProcessed: number;
  totalVolume: number;
  totalPayment: number;
}> {
  try {
    // Fetch data from Elexon API
    const records = await fetchCurtailmentData(date, period);
    
    console.log(`[${date} P${period}] API records: ${records.length}`);
    
    if (records.length === 0) {
      return { recordsProcessed: 0, totalVolume: 0, totalPayment: 0 };
    }
    
    let recordsProcessed = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Filter and process wind farm records
    for (const record of records) {
      // Extract data from the record
      const bmUnitId = record.bmUnitID;
      const leadPartyName = record.leadPartyName;
      const volume = parseFloat(record.volume);
      const originalPrice = parseFloat(record.originalPrice);
      
      // Only process wind farm records with negative volume and flag indicators
      if (
        validWindFarmIds.has(bmUnitId) && 
        volume < 0 && 
        (record.soFlag === 'Y' || record.cadlFlag === 'Y')
      ) {
        // Calculate payment as volume * originalPrice * -1
        const payment = volume * originalPrice * -1;
        
        // Insert into curtailment_records
        await db.insert(curtailmentRecords).values({
          settlementDate: date,
          settlementPeriod: period,
          bmUnitId: bmUnitId,
          leadPartyName: leadPartyName,
          volume: Math.abs(volume).toString(),
          payment: payment.toString(),
          originalPrice: originalPrice.toString(),
          finalPrice: record.finalPrice || originalPrice.toString(),
          soFlag: record.soFlag === 'Y',
          cadlFlag: record.cadlFlag === 'Y'
        });
        
        recordsProcessed++;
        totalVolume += Math.abs(volume);
        totalPayment += payment;
      }
    }
    
    console.log(`[${date} P${period}] Processed ${recordsProcessed} records`);
    console.log(`[${date} P${period}] Total Volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`[${date} P${period}] Total Payment: £${totalPayment.toFixed(2)}`);
    
    return {
      recordsProcessed,
      totalVolume,
      totalPayment
    };
  } catch (error) {
    console.error(`Error processing settlement period ${date} P${period}:`, error);
    return { recordsProcessed: 0, totalVolume: 0, totalPayment: 0 };
  }
}

/**
 * Update daily summary record
 */
async function updateDailySummary(date: string, totalVolume: number, totalPayment: number): Promise<void> {
  try {
    // Check if daily summary exists
    const existingSummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, date)
    });
    
    if (existingSummary) {
      // Update existing summary
      await db
        .update(dailySummaries)
        .set({
          totalCurtailedEnergy: totalVolume.toString(),
          totalPayment: totalPayment.toString(),
          lastUpdated: new Date()
        })
        .where(eq(dailySummaries.summaryDate, date));
      
      console.log(`Updated daily summary for ${date}`);
    } else {
      // Create new summary
      await db.insert(dailySummaries).values({
        summaryDate: date,
        totalCurtailedEnergy: totalVolume.toString(),
        totalPayment: totalPayment.toString(),
        lastUpdated: new Date()
      });
      
      console.log(`Created new daily summary for ${date}`);
    }
  } catch (error) {
    console.error(`Error updating daily summary for ${date}:`, error);
  }
}

/**
 * Process specific settlement periods for a date
 */
async function processSpecificPeriods(date: string, startPeriod = 1, endPeriod = 12): Promise<{
  recordsProcessed: number;
  periodsProcessed: number;
  totalVolume: number;
  totalPayment: number;
}> {
  console.log(`\n=== Processing Periods ${startPeriod}-${endPeriod} for ${date} ===\n`);
  
  // Load valid wind farm IDs
  const validWindFarmIds = await loadWindFarmIds();
  
  if (validWindFarmIds.size === 0) {
    console.error('No valid wind farm IDs found, aborting');
    return { recordsProcessed: 0, periodsProcessed: 0, totalVolume: 0, totalPayment: 0 };
  }
  
  // Clear existing records for this date and periods before reprocessing
  await db.delete(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, date),
        sql`${curtailmentRecords.settlementPeriod} >= ${startPeriod} AND ${curtailmentRecords.settlementPeriod} <= ${endPeriod}`
      )
    );
  
  console.log(`Cleared existing records for ${date} periods ${startPeriod}-${endPeriod}...`);
  
  // Process selected settlement periods
  let totalRecordsProcessed = 0;
  let totalPeriodsProcessed = 0;
  let totalVolumeAll = 0;
  let totalPaymentAll = 0;
  
  // Process periods in small batches to avoid API rate limiting
  const batchSize = 4;
  for (let batchStart = startPeriod; batchStart <= endPeriod; batchStart += batchSize) {
    const batchEnd = Math.min(batchStart + batchSize - 1, endPeriod);
    console.log(`Processing periods ${batchStart}-${batchEnd}...`);
    
    const batchPromises = [];
    for (let period = batchStart; period <= batchEnd; period++) {
      batchPromises.push(processSettlementPeriod(date, period, validWindFarmIds));
    }
    
    const batchResults = await Promise.all(batchPromises);
    
    for (const result of batchResults) {
      if (result.recordsProcessed > 0) {
        totalRecordsProcessed += result.recordsProcessed;
        totalPeriodsProcessed++;
        totalVolumeAll += result.totalVolume;
        totalPaymentAll += result.totalPayment;
      }
    }
    
    // Add delay between batches to avoid API rate limiting
    if (batchEnd < endPeriod) {
      console.log(`Waiting before processing next batch...`);
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }
  
  // Get existing daily summary data
  const existingSummary = await db.query.dailySummaries.findFirst({
    where: eq(dailySummaries.summaryDate, date)
  });
  
  // Calculate total volume and payment including existing data
  let finalTotalVolume = totalVolumeAll;
  let finalTotalPayment = totalPaymentAll;
  
  if (existingSummary) {
    // We're only updating some periods, so we need to add our new data to the existing totals
    // excluding what we just reprocessed
    const existingRecords = await db.query.curtailmentRecords.findMany({
      where: 
        and(
          eq(curtailmentRecords.settlementDate, date),
          sql`${curtailmentRecords.settlementPeriod} < ${startPeriod} OR ${curtailmentRecords.settlementPeriod} > ${endPeriod}`
        )
    });
    
    const existingVolume = existingRecords.reduce((sum, r) => sum + parseFloat(r.volume.toString()), 0);
    const existingPayment = existingRecords.reduce((sum, r) => sum + parseFloat(r.payment.toString()), 0);
    
    finalTotalVolume += existingVolume;
    finalTotalPayment += existingPayment;
  }
  
  // Update daily summary
  await updateDailySummary(date, finalTotalVolume, finalTotalPayment);
  
  console.log(`\n=== Processing Summary for ${date} ===`);
  console.log(`Records Processed: ${totalRecordsProcessed}`);
  console.log(`Periods Processed: ${totalPeriodsProcessed}/${endPeriod - startPeriod + 1}`);
  console.log(`Volume for Periods ${startPeriod}-${endPeriod}: ${totalVolumeAll.toFixed(2)} MWh`);
  console.log(`Payment for Periods ${startPeriod}-${endPeriod}: £${totalPaymentAll.toFixed(2)}`);
  console.log(`Total Volume in Daily Summary: ${finalTotalVolume.toFixed(2)} MWh`);
  console.log(`Total Payment in Daily Summary: £${finalTotalPayment.toFixed(2)}`);
  
  return {
    recordsProcessed: totalRecordsProcessed,
    periodsProcessed: totalPeriodsProcessed,
    totalVolume: totalVolumeAll,
    totalPayment: totalPaymentAll
  };
}

/**
 * Main function
 */
async function main() {
  try {
    // Get the date from command-line arguments or use default
    const dateToProcess = process.argv[2] || format(new Date(), 'yyyy-MM-dd');
    
    // Get optional period range from command line arguments
    const startPeriod = parseInt(process.argv[3], 10) || 1;
    const endPeriod = parseInt(process.argv[4], 10) || 12; // Default to 12 periods
    
    // Process specified periods for the date
    await processSpecificPeriods(dateToProcess, startPeriod, endPeriod);
    
    console.log(`\n=== Processing Complete for ${dateToProcess} Periods ${startPeriod}-${endPeriod} ===\n`);
  } catch (error) {
    console.error(`Error in main process:`, error);
    process.exit(1);
  }
}

main();