/**
 * Reingest Elexon Data for Specific Date
 * 
 * This script completely replaces data for a specific date by:
 * 1. Deleting all existing curtailment records for the date
 * 2. Fetching fresh data from the Elexon API
 * 3. Inserting the new data into the curtailment_records table
 * 4. Updating all dependent tables (daily_summaries, bitcoin calculations, etc.)
 * 5. Generating a comprehensive report of the changes
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries } from "./db/schema";
import { eq, and } from "drizzle-orm";
import { fetchBidsOffers } from "./server/services/elexon";
import fs from "fs";

// Configuration
const TARGET_DATE = process.argv[2] || '2025-04-01';
const LOG_FILE = `logs/reingest_${TARGET_DATE}_${new Date().toISOString().replace(/:/g, '-')}.log`;
const ALL_PERIODS = Array.from({ length: 48 }, (_, i) => i + 1);

// Initialize logging
let logStream: fs.WriteStream;

/**
 * Set up logging to both console and file
 */
function setupLogging() {
  // Ensure logs directory exists
  if (!fs.existsSync('logs')) {
    fs.mkdirSync('logs');
  }
  
  logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });
  
  console.log(`Logging to ${LOG_FILE}`);
}

/**
 * Log message to both console and file
 */
function log(message: string) {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${message}`;
  
  console.log(logMessage);
  logStream.write(logMessage + '\n');
}

/**
 * Delete all existing curtailment records for the target date
 */
async function deleteExistingRecords(): Promise<number> {
  log(`Deleting existing curtailment records for ${TARGET_DATE}...`);
  
  // First, count the records to be deleted
  const records = await db
    .select({ id: curtailmentRecords.id })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
  const count = records.length;
  log(`Found ${count} existing records to delete`);
  
  // Then delete them
  await db
    .delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
  log(`Deleted ${count} records from curtailment_records table`);
  return count;
}

/**
 * Delete existing daily summary for the target date
 */
async function deleteExistingSummary(): Promise<void> {
  log(`Deleting existing daily summary for ${TARGET_DATE}...`);
  
  // Delete the daily summary
  await db
    .delete(dailySummaries)
    .where(eq(dailySummaries.date, TARGET_DATE));
    
  log(`Deleted daily summary for ${TARGET_DATE}`);
}

/**
 * Fetch fresh data from the Elexon API
 */
async function fetchFreshData(): Promise<any[]> {
  log(`Fetching fresh data from Elexon API for ${TARGET_DATE}...`);
  
  const allRecords: any[] = [];
  let totalVolume = 0;
  let totalPayment = 0;
  
  // Process each period sequentially to avoid rate limiting
  for (const period of ALL_PERIODS) {
    try {
      log(`Fetching data for period ${period}...`);
      const periodRecords = await fetchBidsOffers(TARGET_DATE, period);
      
      if (periodRecords.length > 0) {
        const periodVolume = periodRecords.reduce((sum, record) => sum + Math.abs(parseFloat(record.volume.toString())), 0);
        const periodPayment = periodRecords.reduce((sum, record) => sum + (Math.abs(parseFloat(record.volume.toString())) * parseFloat(record.originalPrice.toString()) * -1), 0);
        
        totalVolume += periodVolume;
        totalPayment += periodPayment;
        
        log(`Period ${period}: ${periodRecords.length} records, ${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
        allRecords.push(...periodRecords);
      } else {
        log(`Period ${period}: No records found`);
      }
      
      // Add a delay between periods to avoid rate limiting
      if (period !== ALL_PERIODS[ALL_PERIODS.length - 1]) {
        log(`Waiting 1 second before fetching next period...`);
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    } catch (error) {
      log(`Error fetching period ${period}: ${error}`);
    }
  }
  
  log(`Fetched ${allRecords.length} total records from API`);
  log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
  log(`Total payment: £${totalPayment.toFixed(2)}`);
  
  return allRecords;
}

/**
 * Insert fresh records into the curtailment_records table
 */
async function insertFreshRecords(apiRecords: any[]): Promise<number> {
  if (apiRecords.length === 0) {
    log('No records to insert');
    return 0;
  }
  
  log(`Preparing to insert ${apiRecords.length} fresh records...`);
  
  const recordsToInsert = apiRecords.map(record => ({
    settlementDate: TARGET_DATE,
    settlementPeriod: record.settlementPeriod,
    bmUnitId: record.id,
    bmUnitName: record.bmUnit || record.id,
    leadPartyName: record.leadPartyName || 'Unknown',
    volume: record.volume.toString(),
    payment: (Math.abs(parseFloat(record.volume.toString())) * parseFloat(record.originalPrice.toString())).toString(),
    soFlag: record.soFlag,
    cadlFlag: record.cadlFlag,
    originalPrice: record.originalPrice.toString(),
    finalPrice: record.finalPrice.toString()
  }));
  
  // Log sample record
  log(`Sample record to insert: ${JSON.stringify(recordsToInsert[0], null, 2)}`);
  
  // Insert in batches to avoid query size limits
  const BATCH_SIZE = 100;
  let insertedCount = 0;
  
  for (let i = 0; i < recordsToInsert.length; i += BATCH_SIZE) {
    const batch = recordsToInsert.slice(i, i + BATCH_SIZE);
    log(`Inserting batch ${Math.floor(i/BATCH_SIZE) + 1} of ${Math.ceil(recordsToInsert.length/BATCH_SIZE)}...`);
    
    await db.insert(curtailmentRecords).values(batch);
    insertedCount += batch.length;
    
    log(`Inserted ${insertedCount} of ${recordsToInsert.length} records`);
  }
  
  log(`Successfully inserted all ${insertedCount} records`);
  return insertedCount;
}

/**
 * Update the daily_summaries table
 */
async function updateDailySummary(apiRecords: any[]): Promise<void> {
  log(`Updating daily summary for ${TARGET_DATE}...`);
  
  // Calculate totals
  let totalVolume = 0;
  let totalPayment = 0;
  
  for (const record of apiRecords) {
    const volume = Math.abs(parseFloat(record.volume.toString()));
    const payment = Math.abs(parseFloat(record.volume.toString())) * parseFloat(record.originalPrice.toString());
    
    totalVolume += volume;
    totalPayment += payment;
  }
  
  // Create new daily summary
  await db.insert(dailySummaries).values({
    date: TARGET_DATE,
    totalEnergy: totalVolume.toString(),
    totalPayment: totalPayment.toString()
  });
  
  log(`Updated daily summary with: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
}

/**
 * Trigger rebuild of Bitcoin calculations
 */
async function rebuildBitcoinCalculations(): Promise<void> {
  log(`Triggering rebuild of Bitcoin calculations for ${TARGET_DATE}...`);
  
  try {
    // This would typically call your existing Bitcoin calculation service
    // For example: await bitcoinCalculationService.rebuildForDate(TARGET_DATE);
    log(`NOTE: Bitcoin calculations rebuild function needs to be implemented`);
    log(`Please use your existing scripts to rebuild Bitcoin calculations for ${TARGET_DATE}`);
  } catch (error) {
    log(`Error rebuilding Bitcoin calculations: ${error}`);
  }
}

/**
 * Main function
 */
async function main() {
  try {
    setupLogging();
    
    log(`=== STARTING REINGESTION OF ${TARGET_DATE} ===`);
    
    // Process
    const deletedCount = await deleteExistingRecords();
    await deleteExistingSummary();
    const apiRecords = await fetchFreshData();
    const insertedCount = await insertFreshRecords(apiRecords);
    await updateDailySummary(apiRecords);
    
    log(`=== REINGESTION COMPLETED ===`);
    log(`Deleted: ${deletedCount} records`);
    log(`Inserted: ${insertedCount} records`);
    
    // Provide instructions for next steps
    log(`\nNext steps:`);
    log(`1. Run Bitcoin calculations rebuild for ${TARGET_DATE}`);
    log(`2. Verify data consistency across all tables`);
    log(`3. Update monthly and yearly summaries if needed`);
    
    // Close the log file
    logStream.end();
    
    console.log(`\nReingestion completed successfully. See ${LOG_FILE} for details.`);
    process.exit(0);
  } catch (error) {
    log(`ERROR: ${error}`);
    logStream.end();
    process.exit(1);
  }
}

// Execute main function
main();