/**
 * Curtailment Reprocessing Script for 2025-04-16
 * 
 * This script specifically reprocesses curtailment data for 2025-04-16
 * from the Elexon API for all BMUs and all 48 settlement periods.
 * 
 * Run with: npx tsx reprocess-april16-curtailment.ts
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq } from "drizzle-orm";
import { processDailyCurtailment } from "./server/services/curtailment_enhanced";

const TARGET_DATE = "2025-04-16";

/**
 * Reprocess curtailment data
 */
async function reprocessCurtailment() {
  console.log(`\n=== Starting Curtailment Reprocessing for ${TARGET_DATE} ===\n`);
  const startTime = new Date();
  
  try {
    // Step 1: Delete existing curtailment records for the target date
    console.log(`Removing existing curtailment records for ${TARGET_DATE}...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Step 2: Reprocess curtailment data
    console.log(`\nReprocessing curtailment data for ${TARGET_DATE}...`);
    await processDailyCurtailment(TARGET_DATE);
    console.log(`Successfully reprocessed curtailment data for ${TARGET_DATE}`);
    
    // Calculate execution time
    const endTime = new Date();
    const executionTimeMs = endTime.getTime() - startTime.getTime();
    console.log(`\n=== Reprocessing Completed ===`);
    console.log(`Total execution time: ${(executionTimeMs / 1000).toFixed(2)} seconds`);
    
  } catch (error) {
    console.error(`\n❌ Reprocessing failed:`, error);
    process.exit(1);
  }
}

// Run the reprocessing
reprocessCurtailment().then(() => {
  console.log("\nCurtailment reprocessing completed successfully");
  process.exit(0);
}).catch(error => {
  console.error("\nUnexpected error during reprocessing:", error);
  process.exit(1);
});