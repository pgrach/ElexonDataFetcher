/**
 * Edinburgh Wind Farm Data Processing for 2025-04-13
 * 
 * This script focuses on processing the T_EDINW-1 BMU data for settlement periods 33, 34, and 35
 * on April 13, 2025, which were missing from the previous processing.
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, sql, and } from "drizzle-orm";
import { processDailyCurtailment } from "./server/services/curtailment_enhanced";
import fs from "fs";

// Target date for reprocessing
const TARGET_DATE = "2025-04-13";
// Target periods
const TARGET_PERIODS = [33, 34, 35];
// Output log file
const LOG_FILE = `logs/edinburgh_processing_${new Date().toISOString().replace(/:/g, '-')}.log`;

/**
 * Log a step with a timestamp
 */
function logStep(message: string): void {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${message}`;
  console.log(logMessage);
  fs.appendFileSync(LOG_FILE, logMessage + "\n");
}

/**
 * Process the Edinburgh wind farm data
 */
async function processEdinburghData(): Promise<void> {
  logStep(`Starting Edinburgh wind farm data processing for ${TARGET_DATE}...`);
  
  // Step 1: Check if we already have Edinburgh wind farm records
  const edinburghRecords = await db
    .select({
      period: curtailmentRecords.settlementPeriod,
      farmId: curtailmentRecords.farmId,
      volume: curtailmentRecords.volume,
      payment: curtailmentRecords.payment
    })
    .from(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, TARGET_DATE),
        sql`${curtailmentRecords.farmId} LIKE 'T_EDINW%'`
      )
    )
    .orderBy(curtailmentRecords.settlementPeriod);
  
  if (edinburghRecords.length > 0) {
    logStep(`Found ${edinburghRecords.length} existing Edinburgh wind farm records:`);
    for (const record of edinburghRecords) {
      logStep(`  - Period ${record.period}: ${record.farmId}, ${record.volume} MWh, £${record.payment}`);
    }
    
    logStep("Deleting existing Edinburgh wind farm records...");
    
    // Delete existing records for T_EDINW BMUs
    await db.delete(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          sql`${curtailmentRecords.farmId} LIKE 'T_EDINW%'`
        )
      );
  } else {
    logStep("No existing Edinburgh wind farm records found.");
  }
  
  // Step 2: Process the entire date (which will include our target periods)
  logStep(`Processing curtailment data for ${TARGET_DATE}...`);
  try {
    await processDailyCurtailment(TARGET_DATE);
    
    // Step 3: Verify the records were added
    const newRecords = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        farmId: curtailmentRecords.farmId,
        volume: curtailmentRecords.volume,
        payment: curtailmentRecords.payment
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          sql`${curtailmentRecords.farmId} LIKE 'T_EDINW%'`
        )
      )
      .orderBy(curtailmentRecords.settlementPeriod);
    
    if (newRecords.length > 0) {
      logStep(`Successfully added ${newRecords.length} Edinburgh wind farm records:`);
      for (const record of newRecords) {
        logStep(`  - Period ${record.period}: ${record.farmId}, ${record.volume} MWh, £${record.payment}`);
      }
      
      // Calculate totals
      const totalVolume = newRecords.reduce((sum, record) => sum + Number(record.volume), 0);
      const totalPayment = newRecords.reduce((sum, record) => sum + Number(record.payment), 0);
      
      logStep(`Total processed: ${totalVolume.toFixed(2)} MWh, £${Math.abs(totalPayment).toFixed(2)}`);
    } else {
      logStep("WARNING: No Edinburgh wind farm records were added.");
    }
  } catch (error) {
    logStep(`ERROR processing curtailment data: ${error}`);
  }
  
  // Step 4: Get all curtailment records for the target date
  const allRecords = await db
    .select({
      count: sql<number>`count(*)`,
      volume: sql<string>`SUM(${curtailmentRecords.volume})`,
      payment: sql<string>`SUM(${curtailmentRecords.payment})`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  logStep(`Total curtailment records for ${TARGET_DATE}: ${allRecords[0].count}`);
  logStep(`Total volume: ${parseFloat(allRecords[0].volume).toFixed(2)} MWh`);
  logStep(`Total payment: £${Math.abs(parseFloat(allRecords[0].payment)).toFixed(2)}`);
  
  logStep("Processing completed");
}

// Run the processing
processEdinburghData()
  .then(() => {
    logStep("Script execution completed successfully");
    process.exit(0);
  })
  .catch(error => {
    logStep(`FATAL ERROR: ${error}`);
    process.exit(1);
  });