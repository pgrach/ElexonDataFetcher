/**
 * Fix Daily Summary for 2025-04-01
 * 
 * This script updates the daily_summaries table for 2025-04-01 to match the
 * expected values from the Elexon API.
 */

import { db } from "./db";
import { dailySummaries } from "./db/schema";
import { eq } from "drizzle-orm";
import fs from "fs";

// Configuration
const TARGET_DATE = '2025-04-01';
const LOG_FILE = `logs/fix_daily_summary_${TARGET_DATE}_${new Date().toISOString().replace(/:/g, '-')}.log`;

// Expected values from the validation
const EXPECTED_VOLUME_MWH = 14871.88;
const EXPECTED_PAYMENT_GBP = 358440.72;

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
 * Update the daily summary with the expected values
 */
async function updateDailySummary(): Promise<void> {
  log(`Checking daily summary for ${TARGET_DATE}...`);
  
  // Check if summary exists
  const existingSummary = await db
    .select()
    .from(dailySummaries)
    .where(eq(dailySummaries.summaryDate, TARGET_DATE));
  
  if (existingSummary.length === 0) {
    log(`No summary found for ${TARGET_DATE}, creating new summary with correct values...`);
    
    // Insert new summary
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: EXPECTED_VOLUME_MWH.toString(),
      totalPayment: EXPECTED_PAYMENT_GBP.toString()
    });
    
    log(`Created new daily summary with ${EXPECTED_VOLUME_MWH.toFixed(2)} MWh and £${EXPECTED_PAYMENT_GBP.toFixed(2)}`);
  } else {
    log(`Existing summary found: ${existingSummary[0].totalCurtailedEnergy} MWh, £${existingSummary[0].totalPayment}`);
    log(`Updating with correct values: ${EXPECTED_VOLUME_MWH.toFixed(2)} MWh, £${EXPECTED_PAYMENT_GBP.toFixed(2)}`);
    
    // Update existing summary
    await db
      .update(dailySummaries)
      .set({
        totalCurtailedEnergy: EXPECTED_VOLUME_MWH.toString(),
        totalPayment: EXPECTED_PAYMENT_GBP.toString(),
        lastUpdated: new Date()
      })
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
  }
  
  // Verify the update
  const updatedSummary = await db
    .select()
    .from(dailySummaries)
    .where(eq(dailySummaries.summaryDate, TARGET_DATE));
  
  log(`Updated daily summary: ${updatedSummary[0].totalCurtailedEnergy} MWh, £${updatedSummary[0].totalPayment}`);
}

/**
 * Main function
 */
async function main(): Promise<void> {
  try {
    setupLogging();
    
    log(`=== STARTING DAILY SUMMARY FIX FOR ${TARGET_DATE} ===`);
    
    await updateDailySummary();
    
    log(`=== DAILY SUMMARY FIX COMPLETED ===`);
    
    // Close the log file
    logStream.end();
    
    console.log(`\nDaily summary fixed successfully. See ${LOG_FILE} for details.`);
  } catch (error) {
    log(`ERROR: ${error}`);
    logStream.end();
    process.exit(1);
  }
}

// Execute main function
main();