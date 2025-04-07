/**
 * Complete March 27 Data
 * 
 * This script is specifically designed to finish processing data for March 27, 2025
 * by processing the remaining periods (17-48) in small batches with appropriate delays 
 * to avoid API rate limits. It maintains the existing daily summary and augments it
 * with the additional data.
 */

import { processFullCascade } from './process_bitcoin_optimized';
import fs from 'fs';
import path from 'path';
import { db } from './db';
import { dailySummaries, curtailmentRecords } from './db/schema';
import { eq, sql } from 'drizzle-orm';

// Configuration
const TARGET_DATE = '2025-03-27';
const BATCH_SIZE = 4; // Number of periods to process in each batch
const DELAY_BETWEEN_BATCHES = 5000; // Milliseconds to wait between batches
const START_PERIOD = 17; // Start from period 17 (we already have 1-16)

/**
 * Sleep function for delays
 */
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Load the BMU mapping file
 */
async function loadBmuMapping(): Promise<any[]> {
  try {
    // Read from server's BMU mapping file
    const data = await fs.promises.readFile('./data/bmu_mapping.json', 'utf8');
    return JSON.parse(data);
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    return [];
  }
}

/**
 * Filter for valid wind farm BMUs
 */
async function loadWindFarmIds(): Promise<Set<string>> {
  const bmuMappingData = await loadBmuMapping();
  
  if (!bmuMappingData || !Array.isArray(bmuMappingData)) {
    console.error('BMU mapping data is not valid');
    return new Set<string>();
  }
  
  // Create a Set for faster lookups
  const validIds = new Set<string>();
  
  // Extract all valid elexonBmUnit IDs
  for (const entry of bmuMappingData) {
    if (entry && entry.elexonBmUnit && typeof entry.elexonBmUnit === 'string') {
      validIds.add(entry.elexonBmUnit);
    }
  }
  
  console.log(`Loaded ${validIds.size} valid wind farm BMU IDs`);
  return validIds;
}

/**
 * Fetch curtailment data from Elexon API
 */
async function fetchCurtailmentData(date: string, period: number): Promise<any[]> {
  console.log(`Fetching curtailment data for ${date}, period ${period}...`);
  
  try {
    // Simulate an API call to Elexon
    // In a real implementation, this would be a fetch to the Elexon API
    const response = await fetch(
      `https://api.elexon.co.uk/bmrs/api/v1/datasets/BM_BOD_PRICES?SettlementDate=${date}&SettlementPeriod=${period}`,
      {
        method: 'GET',
        headers: {
          'Accept': 'application/json',
          'Cache-Control': 'no-cache'
        }
      }
    );
    
    if (!response.ok) {
      console.error(`API error: ${response.status} ${response.statusText}`);
      return [];
    }
    
    const data = await response.json();
    
    // Extract the relevant records (in a real implementation, this would parse the API response)
    const records = data.data || [];
    
    console.log(`Fetched ${records.length} records for period ${period}`);
    return records;
  } catch (error) {
    console.error(`Error fetching curtailment data for period ${period}:`, error);
    return [];
  }
}

/**
 * Process a single settlement period
 */
async function processSettlementPeriod(
  date: string, 
  period: number, 
  validWindFarmIds: Set<string>,
  retryCount = 0
): Promise<{
  records: number;
  volume: number;
  payment: number;
}> {
  try {
    console.log(`Processing settlement period ${period} for ${date}...`);
    
    // Fetch curtailment data from Elexon API
    const records = await fetchCurtailmentData(date, period);
    
    // Filter records for valid wind farms
    const validRecords = records.filter(
      record => record.bmUnitId && validWindFarmIds.has(record.bmUnitId)
    );
    
    // Insert records into database
    let recordsInserted = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (const record of validRecords) {
      try {
        // Extract values from record
        const volume = parseFloat(record.volume || '0');
        const payment = parseFloat(record.payment || '0');
        
        if (isNaN(volume) || isNaN(payment)) {
          console.warn(`Invalid volume or payment value in record:`, record);
          continue;
        }
        
        // Insert the record into the database
        await db.insert(curtailmentRecords).values({
          volume: volume.toString(),
          payment: payment.toString(),
          settlementDate: date,
          settlementPeriod: period,
          farmId: record.bmUnitId,
          farmName: record.name || 'Unknown',
          leadParty: record.leadParty || 'Unknown',
          notificationTime: new Date(record.notificationTime || Date.now()),
          cadlFlag: record.cadlFlag === 'true',
          createdAt: new Date()
        });
        
        recordsInserted++;
        totalVolume += Math.abs(volume);
        totalPayment += Math.abs(payment);
      } catch (error) {
        console.error(`Error inserting record:`, error, record);
      }
    }
    
    console.log(`Processed period ${period}: ${recordsInserted} records inserted`);
    console.log(`Volume: ${totalVolume.toFixed(2)} MWh, Payment: £${totalPayment.toFixed(2)}`);
    
    return {
      records: recordsInserted,
      volume: totalVolume,
      payment: totalPayment
    };
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    
    // Retry logic
    const MAX_RETRIES = 3;
    const RETRY_DELAY_MS = 5000;
    
    if (retryCount < MAX_RETRIES) {
      console.log(`Retrying period ${period} for ${date} (attempt ${retryCount + 1} of ${MAX_RETRIES})...`);
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
      return processSettlementPeriod(date, period, validWindFarmIds, retryCount + 1);
    }
    
    console.error(`Failed to process period ${period} for ${date} after ${MAX_RETRIES} attempts`);
    return { records: 0, volume: 0, payment: 0 };
  }
}

/**
 * Process specific periods for a date
 */
async function processSpecificPeriods(date: string, startPeriod: number, endPeriod: number): Promise<{
  totalRecords: number;
  totalVolume: number;
  totalPayment: number;
}> {
  console.log(`\n=== Processing Periods ${startPeriod}-${endPeriod} for ${date} ===\n`);
  
  // Load wind farm IDs
  const validWindFarmIds = await loadWindFarmIds();
  
  // Process in small batches to avoid hitting API rate limits
  let totalRecords = 0;
  let periodsProcessed = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  
  // Create a queue of the specified periods
  const periodsToProcess = Array.from(
    { length: endPeriod - startPeriod + 1 }, 
    (_, i) => startPeriod + i
  );
  
  const SMALL_BATCH_SIZE = 4;
  const API_RATE_LIMIT_DELAY_MS = 1000;
  
  // Process in small batches
  for (let i = 0; i < periodsToProcess.length; i += SMALL_BATCH_SIZE) {
    const batchPeriods = periodsToProcess.slice(i, i + SMALL_BATCH_SIZE);
    console.log(`Processing periods ${batchPeriods.join(', ')}...`);
    
    // Process each period in the batch concurrently
    const batchResults = await Promise.all(
      batchPeriods.map(async period => {
        // Add a small delay between API calls to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, API_RATE_LIMIT_DELAY_MS));
        return processSettlementPeriod(date, period, validWindFarmIds);
      })
    );
    
    // Aggregate results from this batch
    for (const result of batchResults) {
      if (result && result.records > 0) {
        totalRecords += result.records;
        periodsProcessed++;
        totalVolume += result.volume;
        totalPayment += result.payment;
      }
    }
    
    console.log(`Progress: ${periodsProcessed}/${periodsToProcess.length} periods processed (${totalRecords} records)`);
    
    // Add a delay between batches to avoid API rate limits
    if (i + SMALL_BATCH_SIZE < periodsToProcess.length) {
      console.log(`Waiting ${API_RATE_LIMIT_DELAY_MS * 3}ms before next batch...`);
      await new Promise(resolve => setTimeout(resolve, API_RATE_LIMIT_DELAY_MS * 3));
    }
  }
  
  console.log(`\n=== Processing Summary for Periods ${startPeriod}-${endPeriod} ===`);
  console.log(`Total Records: ${totalRecords}`);
  console.log(`Periods Processed: ${periodsProcessed}`);
  console.log(`Total Volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`Total Payment: £${totalPayment.toFixed(2)}`);
  
  return {
    totalRecords,
    totalVolume,
    totalPayment
  };
}

/**
 * Process a batch of periods
 */
async function processBatch(startPeriod: number, endPeriod: number): Promise<{ 
  totalRecords: number; 
  totalVolume: number; 
  totalPayment: number;
}> {
  console.log(`\nProcessing periods ${startPeriod}-${endPeriod}...`);
  
  try {
    // Process the periods
    const result = await processSpecificPeriods(TARGET_DATE, startPeriod, endPeriod);
    console.log(`Processed periods ${startPeriod}-${endPeriod}: ${result.totalRecords} records`);
    return {
      totalRecords: result.totalRecords,
      totalVolume: result.totalVolume,
      totalPayment: result.totalPayment
    };
  } catch (error) {
    console.error(`Error processing periods ${startPeriod}-${endPeriod}:`, error);
    return { totalRecords: 0, totalVolume: 0, totalPayment: 0 };
  }
}

/**
 * Update the daily summary with the total values
 */
async function updateDailySummary(totalVolume: number, totalPayment: number): Promise<void> {
  console.log(`\nUpdating daily summary for ${TARGET_DATE}...`);
  console.log(`Additional Volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`Additional Payment: £${totalPayment.toFixed(2)}`);
  
  try {
    // Get existing daily summary 
    const existingSummary = await db.select().from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE))
      .limit(1);
    
    if (existingSummary.length === 0) {
      console.log('No existing summary found, creating new one');
      await db.insert(dailySummaries).values({
        summaryDate: TARGET_DATE,
        totalCurtailedEnergy: totalVolume.toString(),
        totalPayment: totalPayment.toString(),
        createdAt: new Date(),
        lastUpdated: new Date()
      });
    } else {
      // Update the existing summary by adding the new totals
      const newTotalVolume = Number(existingSummary[0].totalCurtailedEnergy) + totalVolume;
      const newTotalPayment = Number(existingSummary[0].totalPayment) + totalPayment;
      
      await db.update(dailySummaries)
        .set({
          totalCurtailedEnergy: newTotalVolume.toString(),
          totalPayment: newTotalPayment.toString(),
          lastUpdated: new Date()
        })
        .where(eq(dailySummaries.summaryDate, TARGET_DATE));
      
      console.log(`Updated daily summary: ${newTotalVolume.toFixed(2)} MWh, £${newTotalPayment.toFixed(2)}`);
    }
    
    console.log('Daily summary updated successfully');
  } catch (error) {
    console.error('Error updating daily summary:', error);
  }
}

/**
 * Process Bitcoin calculations after all curtailment data is processed
 */
async function processBitcoinData(): Promise<void> {
  console.log(`\nProcessing Bitcoin calculations for ${TARGET_DATE}...`);
  try {
    await processFullCascade(TARGET_DATE);
    console.log('Bitcoin calculations processed successfully');
  } catch (error) {
    console.error('Error processing Bitcoin calculations:', error);
  }
}

/**
 * Main function to complete the data for March 27, 2025
 */
async function main() {
  console.log(`===== Completing Data for ${TARGET_DATE} (Periods ${START_PERIOD}-48) =====`);
  
  // Create a log file
  const logFile = path.join('logs', `complete_march_27_${new Date().toISOString().replace(/:/g, '-')}.log`);
  const logStream = fs.createWriteStream(logFile, { flags: 'a' });
  
  // Redirect console output to log file
  const originalConsoleLog = console.log;
  const originalConsoleError = console.error;
  console.log = function(message: any, ...args: any[]) {
    originalConsoleLog(message, ...args);
    logStream.write(`${message}\n`);
  };
  console.error = function(message: any, ...args: any[]) {
    originalConsoleError(message, ...args);
    logStream.write(`ERROR: ${message}\n`);
  };
  
  try {
    // Process the remaining periods in small batches
    let totalRecordCount = 0;
    let overallTotalVolume = 0;
    let overallTotalPayment = 0;
    
    for (let period = START_PERIOD; period <= 48; period += BATCH_SIZE) {
      const endPeriod = Math.min(period + BATCH_SIZE - 1, 48);
      const batchResult = await processBatch(period, endPeriod);
      
      totalRecordCount += batchResult.totalRecords;
      overallTotalVolume += batchResult.totalVolume;
      overallTotalPayment += batchResult.totalPayment;
      
      // Add delay between batches to avoid API rate limits
      if (endPeriod < 48) {
        console.log(`Waiting ${DELAY_BETWEEN_BATCHES}ms before next batch...`);
        await sleep(DELAY_BETWEEN_BATCHES);
      }
    }
    
    // Update the daily summary with the additional data
    await updateDailySummary(overallTotalVolume, overallTotalPayment);
    
    // Process Bitcoin calculations for the complete dataset
    await processBitcoinData();
    
    console.log(`\n===== Data Completion Finished for ${TARGET_DATE} =====`);
    console.log(`Additional Records: ${totalRecordCount}`);
    console.log(`Additional Volume: ${overallTotalVolume.toFixed(2)} MWh`);
    console.log(`Additional Payment: £${overallTotalPayment.toFixed(2)}`);
    console.log(`Log file: ${logFile}`);
  } catch (error) {
    console.error('Error in main process:', error);
  } finally {
    // Restore console functions
    console.log = originalConsoleLog;
    console.error = originalConsoleError;
    
    // Close log stream
    logStream.end();
  }
}

// Run the script
main().catch(console.error);