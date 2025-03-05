/**
 * Reprocess March 4, 2025
 * 
 * This script will reprocess the entire day of March 4, 2025
 * using the processDailyCurtailment function.
 */

import { processDailyCurtailment } from "./server/services/curtailment";
import { processSingleDay } from "./server/services/bitcoinService";
import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq } from "drizzle-orm";

const TARGET_DATE = '2025-03-04';

async function main() {
  try {
    console.log(`\n=== Reprocessing Data for ${TARGET_DATE} ===\n`);
    
    // Step 1: Reprocess the day's curtailment data
    console.log("Processing curtailment data...");
    await processDailyCurtailment(TARGET_DATE);
    
    // Step 2: Count periods and records after processing
    const periodCount = await db
      .select({ periods: db.func.count() })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.settlementPeriod);
    
    const recordCount = await db
      .select({ count: db.func.count() })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`After processing: ${periodCount.length}/48 periods and ${recordCount[0]?.count || 0} total records`);
    
    // Step 3: Update Bitcoin calculations for all miner models
    console.log("\nUpdating Bitcoin calculations...");
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const minerModel of minerModels) {
      console.log(`Processing ${minerModel}...`);
      await processSingleDay(TARGET_DATE, minerModel);
    }
    
    console.log("\n=== Processing Complete ===\n");
    console.log(`Done! Reprocessed ${TARGET_DATE} with ${periodCount.length} periods.`);
  } catch (error) {
    console.error('Error during reprocessing:', error);
  }
}

// Run the script
main();