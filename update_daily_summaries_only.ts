/**
 * Update Bitcoin Daily Summaries Only
 * 
 * This script updates just the bitcoin_daily_summaries table using existing historical calculation data.
 */

import { db } from "./db";
import { 
  historicalBitcoinCalculations,
  bitcoinDailySummaries
} from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import fs from "fs";

// Configuration
const TARGET_DATE = process.argv[2] || '2025-04-01';
const LOG_FILE = `logs/update_daily_summaries_${TARGET_DATE}_${new Date().toISOString().replace(/:/g, '-')}.log`;

// Define miner models
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

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
 * Delete existing daily summaries for the target date
 */
async function deleteExistingDailySummaries(): Promise<void> {
  log(`Deleting existing bitcoin daily summaries for ${TARGET_DATE}...`);
  
  // Delete from bitcoin_daily_summaries
  const deletedDaily = await db
    .delete(bitcoinDailySummaries)
    .where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE))
    .returning();
    
  log(`Deleted ${deletedDaily.length} records from bitcoin_daily_summaries`);
}

/**
 * Check if there are historical calculations for the date
 */
async function checkHistoricalCalculations(): Promise<boolean> {
  log(`Checking for historical calculations for ${TARGET_DATE}...`);
  
  const count = await db
    .select({ count: sql`COUNT(*)` })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
  const recordCount = parseInt(count[0].count.toString());
  log(`Found ${recordCount} historical calculation records`);
  
  return recordCount > 0;
}

/**
 * Update the bitcoin_daily_summaries table
 */
async function updateDailySummaries(): Promise<void> {
  log(`Updating bitcoin_daily_summaries for ${TARGET_DATE}...`);
  
  // Process each miner model
  for (const minerModel of MINER_MODELS) {
    // Calculate total Bitcoin mined for this model and date
    const result = await db
      .select({
        totalBitcoin: sql`SUM(bitcoin_mined::numeric)`,
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );
    
    const totalBitcoin = result[0]?.totalBitcoin || '0';
    
    log(`${minerModel} total Bitcoin for ${TARGET_DATE}: ${parseFloat(totalBitcoin).toFixed(8)} BTC`);
    
    // Insert into daily summaries
    await db.insert(bitcoinDailySummaries).values({
      summaryDate: TARGET_DATE,
      minerModel: minerModel,
      bitcoinMined: totalBitcoin.toString()
    });
  }
  
  log(`Daily summaries updated successfully`);
}

/**
 * Main function
 */
async function main() {
  try {
    setupLogging();
    
    log(`=== STARTING DAILY SUMMARIES UPDATE FOR ${TARGET_DATE} ===`);
    
    // Check if historical calculations exist
    const hasHistoricalData = await checkHistoricalCalculations();
    
    if (!hasHistoricalData) {
      log(`ERROR: No historical calculations found for ${TARGET_DATE}. Run the full update script first.`);
      logStream.end();
      process.exit(1);
    }
    
    // Process
    await deleteExistingDailySummaries();
    await updateDailySummaries();
    
    log(`=== UPDATE COMPLETED ===`);
    
    // Close the log file
    logStream.end();
    
    console.log(`\nBitcoin daily summaries updated successfully. See ${LOG_FILE} for details.`);
    process.exit(0);
  } catch (error) {
    log(`ERROR: ${error}`);
    logStream.end();
    process.exit(1);
  }
}

// Execute main function
main();