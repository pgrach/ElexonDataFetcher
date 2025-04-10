/**
 * Direct Reingestion Script for 2025-04-03
 * 
 * This script directly accesses the processDailyCurtailment function 
 * to reprocess curtailment data for 2025-04-03. Bitcoin calculations
 * are skipped to avoid potential constraint errors.
 * 
 * Run with: npx tsx scripts/reprocess-april3.ts
 */

import { processDailyCurtailment } from '../server/services/curtailment';
import { db } from '../db';
import { curtailmentRecords, dailySummaries } from '../db/schema';
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
    
    // Check the daily summary
    const summary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    console.log(`\nDaily Summary:`);
    if (summary.length > 0) {
      console.log(`Date: ${summary[0].summaryDate}`);
      console.log(`Total Curtailed Energy: ${summary[0].totalCurtailedEnergy} MWh`);
      console.log(`Total Payment: £${summary[0].totalPayment}`);
      console.log(`Last Updated: ${summary[0].lastUpdated}`);
    } else {
      console.log(`No daily summary found for ${TARGET_DATE}`);
    }
    
    console.log(`\nSkipping Bitcoin calculations to avoid constraint errors.`);
    
    console.log(`\n=== Reingestion Complete ===`);
    console.log(`Date: ${TARGET_DATE}`);
    console.log(`Status: Success`);
    console.log(`Completed at: ${new Date().toISOString()}`);
    
  } catch (error) {
    console.error(`\nError during reingestion:`, error instanceof Error ? error.message : 'Unknown error');
    process.exit(1);
  }
}

// Execute the reingestion
reprocessDate().catch(error => {
  console.error(`Fatal error:`, error instanceof Error ? error.message : 'Unknown error');
  process.exit(1);
});