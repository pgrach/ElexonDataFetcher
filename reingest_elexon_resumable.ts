/**
 * Resumable Elexon Data Reingestion for Specific Date
 * 
 * This script completely replaces data for a specific date by:
 * 1. Deleting all existing curtailment records for the date (if not resuming)
 * 2. Fetching fresh data from the Elexon API
 * 3. Inserting the new data into the curtailment_records table
 * 4. Updating all dependent tables (daily_summaries, bitcoin calculations, etc.)
 * 
 * The script supports resuming a previously interrupted reingestion
 * by saving progress to a state file.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries } from "./db/schema";
import { eq, and } from "drizzle-orm";
import { fetchBidsOffers } from "./server/services/elexon";
import fs from "fs";

// Configuration
const TARGET_DATE = process.argv[2] || '2025-04-01';
const RESUME_MODE = process.argv.includes('--resume');
const FORCE_DELETE = process.argv.includes('--force-delete');
const BATCH_SIZE = 5; // Process periods in batches of 5 for better performance

const LOG_FILE = `logs/reingest_resumable_${TARGET_DATE}_${new Date().toISOString().replace(/:/g, '-')}.log`;
const STATE_FILE = `temp/reingest_state_${TARGET_DATE}.json`;

// Define all 48 settlement periods
const ALL_PERIODS = Array.from({ length: 48 }, (_, i) => i + 1);

// Initialize logging and state
let logStream: fs.WriteStream;
let processedPeriods: number[] = [];
let allRecords: any[] = [];
let totalVolume = 0;
let totalPayment = 0;

/**
 * Set up logging to both console and file
 */
function setupLogging() {
  // Ensure logs directory exists
  if (!fs.existsSync('logs')) {
    fs.mkdirSync('logs');
  }
  
  // Ensure temp directory exists for state file
  if (!fs.existsSync('temp')) {
    fs.mkdirSync('temp');
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
 * Save current state to file
 */
function saveState() {
  const state = {
    targetDate: TARGET_DATE,
    processedPeriods,
    insertedRecords: allRecords.length,
    totalVolume,
    totalPayment,
    lastUpdated: new Date().toISOString()
  };
  
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
  log(`State saved to ${STATE_FILE}`);
}

/**
 * Load state from file if it exists
 */
function loadState(): boolean {
  if (fs.existsSync(STATE_FILE)) {
    try {
      const state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
      
      if (state.targetDate === TARGET_DATE) {
        processedPeriods = state.processedPeriods || [];
        totalVolume = state.totalVolume || 0;
        totalPayment = state.totalPayment || 0;
        
        log(`Loaded state from ${STATE_FILE}`);
        log(`Previously processed periods: ${processedPeriods.join(', ')}`);
        log(`Remaining periods: ${ALL_PERIODS.filter(p => !processedPeriods.includes(p)).join(', ')}`);
        return true;
      }
    } catch (error) {
      log(`Error loading state: ${error}`);
    }
  }
  
  return false;
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
    .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
  log(`Deleted daily summary for ${TARGET_DATE}`);
}

/**
 * Fetch fresh data from the Elexon API for a batch of periods
 */
async function fetchBatchData(periods: number[]): Promise<any[]> {
  const batchRecords: any[] = [];
  
  // Process each period sequentially to avoid rate limiting
  for (const period of periods) {
    try {
      log(`Fetching data for period ${period}...`);
      const periodRecords = await fetchBidsOffers(TARGET_DATE, period);
      
      if (periodRecords.length > 0) {
        const periodVolume = periodRecords.reduce((sum, record) => sum + Math.abs(parseFloat(record.volume.toString())), 0);
        const periodPayment = periodRecords.reduce((sum, record) => sum + (Math.abs(parseFloat(record.volume.toString())) * parseFloat(record.originalPrice.toString()) * -1), 0);
        
        totalVolume += periodVolume;
        totalPayment += periodPayment;
        
        log(`Period ${period}: ${periodRecords.length} records, ${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
        batchRecords.push(...periodRecords);
      } else {
        log(`Period ${period}: No records found`);
      }
      
      // Mark period as processed
      processedPeriods.push(period);
      
      // Save state after each period
      saveState();
      
      // Add a delay between periods to avoid rate limiting
      if (period !== periods[periods.length - 1]) {
        log(`Waiting 1 second before fetching next period...`);
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    } catch (error) {
      log(`Error fetching period ${period}: ${error}`);
    }
  }
  
  return batchRecords;
}

/**
 * Insert fresh records into the curtailment_records table
 */
async function insertBatchRecords(apiRecords: any[]): Promise<number> {
  if (apiRecords.length === 0) {
    log('No records to insert in this batch');
    return 0;
  }
  
  log(`Preparing to insert ${apiRecords.length} fresh records...`);
  
  const recordsToInsert = apiRecords.map(record => ({
    settlementDate: TARGET_DATE,
    settlementPeriod: record.settlementPeriod,
    farmId: record.id, // Use id as farmId
    leadPartyName: record.leadPartyName || 'Unknown',
    volume: record.volume.toString(),
    payment: (Math.abs(parseFloat(record.volume.toString())) * parseFloat(record.originalPrice.toString())).toString(),
    soFlag: record.soFlag,
    cadlFlag: record.cadlFlag,
    originalPrice: record.originalPrice.toString(),
    finalPrice: record.finalPrice.toString()
  }));
  
  // Log sample record
  if (recordsToInsert.length > 0) {
    log(`Sample record to insert: ${JSON.stringify(recordsToInsert[0], null, 2)}`);
  }
  
  // Insert in small batches to avoid query size limits
  const INSERT_BATCH_SIZE = 50;
  let insertedCount = 0;
  
  for (let i = 0; i < recordsToInsert.length; i += INSERT_BATCH_SIZE) {
    const batch = recordsToInsert.slice(i, i + INSERT_BATCH_SIZE);
    log(`Inserting batch ${Math.floor(i/INSERT_BATCH_SIZE) + 1} of ${Math.ceil(recordsToInsert.length/INSERT_BATCH_SIZE)}...`);
    
    await db.insert(curtailmentRecords).values(batch);
    insertedCount += batch.length;
    
    log(`Inserted ${insertedCount} of ${recordsToInsert.length} records`);
  }
  
  log(`Successfully inserted all ${insertedCount} records in this batch`);
  return insertedCount;
}

/**
 * Update the daily_summaries table
 */
async function updateDailySummary(): Promise<void> {
  log(`Updating daily summary for ${TARGET_DATE}...`);
  
  // Create new daily summary
  await db.insert(dailySummaries).values({
    summaryDate: TARGET_DATE,
    totalCurtailedEnergy: totalVolume.toString(),
    totalPayment: totalPayment.toString()
  });
  
  log(`Updated daily summary with: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
}

/**
 * Process a batch of periods
 */
async function processBatch(periods: number[]): Promise<boolean> {
  try {
    log(`Processing batch with periods: ${periods.join(', ')}...`);
    
    // Fetch data for this batch
    const batchRecords = await fetchBatchData(periods);
    
    // Insert records for this batch
    if (batchRecords.length > 0) {
      const insertedCount = await insertBatchRecords(batchRecords);
      // Add successful records to our total collection
      allRecords.push(...batchRecords);
    }
    
    log(`Completed batch processing for periods: ${periods.join(', ')}`);
    return true;
  } catch (error) {
    log(`ERROR processing batch ${periods.join(', ')}: ${error}`);
    return false;
  }
}

/**
 * Main function
 */
async function main() {
  try {
    setupLogging();
    
    log(`=== STARTING RESUMABLE REINGESTION OF ${TARGET_DATE} ===`);
    log(`Mode: ${RESUME_MODE ? 'Resume' : 'New'} ${FORCE_DELETE ? '(Force Delete)' : ''}`);
    
    // Check if we're resuming from a previous run
    const hasState = loadState();
    
    if (!RESUME_MODE || !hasState || FORCE_DELETE) {
      // Delete existing data if not resuming or forced deletion
      await deleteExistingRecords();
      await deleteExistingSummary();
      
      // Reset state
      processedPeriods = [];
      allRecords = [];
      totalVolume = 0;
      totalPayment = 0;
      saveState();
    }
    
    // Get periods that still need processing
    const remainingPeriods = ALL_PERIODS.filter(p => !processedPeriods.includes(p));
    log(`Remaining periods to process: ${remainingPeriods.length}`);
    
    // Process in batches
    if (remainingPeriods.length > 0) {
      // Create batches of periods
      const periodBatches = [];
      for (let i = 0; i < remainingPeriods.length; i += BATCH_SIZE) {
        periodBatches.push(remainingPeriods.slice(i, i + BATCH_SIZE));
      }
      
      log(`Created ${periodBatches.length} batches of periods to process`);
      
      // Process each batch sequentially
      for (let i = 0; i < periodBatches.length; i++) {
        const batch = periodBatches[i];
        log(`Processing batch ${i+1} of ${periodBatches.length}: ${batch.join(', ')}`);
        await processBatch(batch);
        log(`Completed batch ${i+1} of ${periodBatches.length}`);
        saveState();
      }
    }
    
    // Update daily summary
    if (ALL_PERIODS.length === processedPeriods.length) {
      log(`All ${ALL_PERIODS.length} periods processed successfully`);
      
      // Create or update the daily summary
      await updateDailySummary();
      
      log(`=== REINGESTION COMPLETED ===`);
      log(`Processed all 48 periods`);
      log(`Inserted ${allRecords.length} records`);
      log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
      log(`Total payment: £${totalPayment.toFixed(2)}`);
      
      // Clean up state file
      if (fs.existsSync(STATE_FILE)) {
        fs.unlinkSync(STATE_FILE);
        log(`Removed state file as processing is complete`);
      }
    } else {
      log(`=== REINGESTION PAUSED ===`);
      log(`Processed ${processedPeriods.length} of ${ALL_PERIODS.length} periods`);
      log(`Resume with: node reingest_elexon_resumable.js ${TARGET_DATE} --resume`);
    }
    
    // Provide instructions for next steps
    log(`\nNext steps:`);
    log(`1. Run Bitcoin calculations rebuild for ${TARGET_DATE}`);
    log(`2. Verify data consistency across all tables`);
    log(`3. Update monthly and yearly summaries if needed`);
    
    // Close the log file
    logStream.end();
    
    console.log(`\nReingestion ${ALL_PERIODS.length === processedPeriods.length ? 'completed successfully' : 'paused'}. See ${LOG_FILE} for details.`);
    process.exit(0);
  } catch (error) {
    log(`ERROR: ${error}`);
    logStream.end();
    process.exit(1);
  }
}

// Execute main function
main();