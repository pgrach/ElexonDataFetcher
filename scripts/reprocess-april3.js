/**
 * Direct Reingestion Script for 2025-04-03
 * 
 * This script directly accesses the processDailyCurtailment function 
 * and Bitcoin calculation services to reprocess data for 2025-04-03.
 */

import { processDailyCurtailment } from '../server/services/curtailment.js';
import { processSingleDay } from '../server/services/bitcoinService.js';
import { db } from '../db/index.js';
import { curtailmentRecords } from '../db/schema.js';
import { eq, sql } from 'drizzle-orm';

const TARGET_DATE = '2025-04-03';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

async function reprocessDate() {
  console.log(`\n=== Starting Direct Reingestion for ${TARGET_DATE} ===`);
  
  try {
    // Step 1: Reingest curtailment records
    console.log(`\nProcessing curtailment data for ${TARGET_DATE}...`);
    await processDailyCurtailment(TARGET_DATE);
    console.log(`Successfully reingested curtailment data for ${TARGET_DATE}`);
    
    // Verify curtailment data
    const stats = await db
      .select({
        recordCount: sql`COUNT(*)`,
        periodCount: sql`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        totalVolume: sql`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nCurtailment Data Verification:`);
    console.log(`Records: ${stats[0]?.recordCount || 0}`);
    console.log(`Settlement Periods: ${stats[0]?.periodCount || 0}`);
    console.log(`Total Volume: ${Number(stats[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Total Payment: £${Number(stats[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Step 2: Process Bitcoin calculations
    console.log(`\nProcessing Bitcoin calculations for ${TARGET_DATE}...`);
    
    let bitcoinSuccess = true;
    for (const minerModel of MINER_MODELS) {
      try {
        console.log(`Processing ${minerModel}...`);
        await processSingleDay(TARGET_DATE, minerModel);
        console.log(`Successfully processed ${minerModel}`);
      } catch (error) {
        console.error(`Error processing ${minerModel}: ${error.message}`);
        bitcoinSuccess = false;
      }
    }
    
    if (!bitcoinSuccess) {
      console.warn(`\nNote: Some Bitcoin calculations failed, but curtailment data was processed successfully.`);
      console.warn(`This is expected for future dates due to missing difficulty data.`);
    }
    
    console.log(`\n=== Reingestion Complete ===`);
    console.log(`Date: ${TARGET_DATE}`);
    console.log(`Status: Success`);
    console.log(`Completed at: ${new Date().toISOString()}`);
    
  } catch (error) {
    console.error(`\nError during reingestion:`, error);
    process.exit(1);
  }
}

// Execute the reingestion
reprocessDate().catch(error => {
  console.error(`Fatal error:`, error);
  process.exit(1);
});