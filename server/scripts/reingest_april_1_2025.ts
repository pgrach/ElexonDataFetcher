/**
 * Reingest April 1, 2025 Data
 * 
 * This script performs a complete reingestion for April 1, 2025 by:
 * 1. Clearing all existing data for this date
 * 2. Fetching fresh data from the Elexon API for all 48 settlement periods
 * 3. Updating the daily, monthly, and yearly summaries
 * 4. Regenerating all Bitcoin mining calculations for the date
 * 5. Verifying data completeness
 */

import { db } from "@db";
import { curtailmentRecords, bitcoinDailySummaries } from "@db/schema";
import { eq, and, sql } from "drizzle-orm";

// Target date
const TARGET_DATE = "2025-04-01";
const TARGET_MONTH = "2025-04";
const TARGET_YEAR = "2025";

/**
 * Main reingestion process
 */
async function reingestion() {
  const startTime = Date.now();
  
  console.log('\n============================================');
  console.log(`STARTING COMPLETE REINGESTION FOR ${TARGET_DATE}`);
  console.log('============================================\n');
  
  try {
    // Step 1: Delete existing records for the date
    console.log(`Clearing existing records for ${TARGET_DATE}...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Step 2: Delete existing Bitcoin calculations for the date
    console.log(`Clearing existing Bitcoin calculations for ${TARGET_DATE}...`);
    await db.delete(bitcoinDailySummaries)
      .where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE));
    
    // Step 3: Import and use the existing curtailment service to process the date
    console.log(`\n=== Processing curtailment data for ${TARGET_DATE} ===`);
    const { processDailyCurtailment } = await import("../services/curtailment");
    await processDailyCurtailment(TARGET_DATE);
    console.log(`Curtailment processing completed for ${TARGET_DATE}`);
    
    // Step 4: Process Bitcoin calculations for all miner models
    console.log(`\n=== Updating Bitcoin calculations for ${TARGET_DATE} ===`);
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    const { processSingleDay } = await import("../services/bitcoinService");
    
    for (const minerModel of minerModels) {
      console.log(`Processing ${minerModel} calculations...`);
      await processSingleDay(TARGET_DATE, minerModel);
    }
    
    // Step 5: Verify the data
    console.log(`\n=== Verifying data integrity for ${TARGET_DATE} ===`);
    const stats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Curtailment Records: ${stats[0].recordCount}`);
    console.log(`Settlement Periods: ${stats[0].periodCount}/48`);
    console.log(`Total Volume: ${parseFloat(stats[0].totalVolume || '0').toFixed(2)} MWh`);
    console.log(`Total Payment: £${parseFloat(stats[0].totalPayment || '0').toFixed(2)}`);
    
    // Check Bitcoin calculations
    console.log('\nBitcoin Mining Calculations:');
    for (const minerModel of minerModels) {
      const btcCalc = await db
        .select({
          bitcoinMined: sql<string>`bitcoin_mined`
        })
        .from(bitcoinDailySummaries)
        .where(
          and(
            eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
            eq(bitcoinDailySummaries.minerModel, minerModel)
          )
        );
        
      if (btcCalc.length > 0) {
        console.log(`${minerModel}: ${parseFloat(btcCalc[0]?.bitcoinMined || '0').toFixed(8)} BTC`);
      } else {
        console.log(`${minerModel}: No Bitcoin calculation found`);
      }
    }
    
    const endTime = Date.now();
    console.log('\n============================================');
    console.log('REINGESTION COMPLETED SUCCESSFULLY');
    console.log(`Duration: ${((endTime - startTime) / 1000).toFixed(2)} seconds`);
    console.log('============================================\n');
    
  } catch (error) {
    console.error('\nREINGESTION FAILED:', error);
    process.exit(1);
  }
}

// Execute the reingestion process
reingestion()
  .then(() => process.exit(0))
  .catch(error => {
    console.error('Unhandled error during reingestion:', error);
    process.exit(1);
  });