/**
 * Complete reprocessing script for April 1, 2025
 * 
 * This script:
 * 1. Clears existing data for April 1, 2025
 * 2. Processes new curtailment data for all 48 settlement periods
 * 3. Updates the daily, monthly, and yearly summaries
 * 4. Recalculates Bitcoin mining potential for all miner models
 */

import { db } from "@db";
import { curtailmentRecords, bitcoinDailySummaries } from "@db/schema";
import { eq, and, sql } from "drizzle-orm";
import { processDailyCurtailment } from "../services/curtailment";

// Target date
const TARGET_DATE = "2025-04-01";

async function reprocessData() {
  const startTime = Date.now();
  
  console.log('\n============================================');
  console.log(`STARTING COMPLETE REPROCESSING FOR ${TARGET_DATE}`);
  console.log('============================================\n');
  
  try {
    // Step 1: Delete existing records for the date
    console.log(`Clearing existing records for ${TARGET_DATE}...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Step 2: Clear existing Bitcoin calculations
    console.log(`Clearing existing Bitcoin calculations for ${TARGET_DATE}...`);
    await db.delete(bitcoinDailySummaries)
      .where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE));
    
    // Step 3: Process curtailment data for all periods
    console.log(`\n=== Processing curtailment data for ${TARGET_DATE} ===`);
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
    
    // Step 5: Verify data integrity by checking records
    console.log(`\n=== Verifying data integrity for ${TARGET_DATE} ===`);
    
    // Create our own verification function
    const periodCounts = await db
      .select({
        period: sql<number>`settlement_period`,
        count: sql<number>`COUNT(*)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(sql`settlement_period`)
      .orderBy(sql`settlement_period`);
    
    console.log(`Found ${periodCounts.length} periods with data`);
    
    // Check for missing periods
    const allPeriods = Array.from({length: 48}, (_, i) => i + 1);
    const existingPeriods = periodCounts.map(p => p.period);
    const missingPeriods = allPeriods.filter(p => !existingPeriods.includes(p));
    
    if (missingPeriods.length > 0) {
      console.log(`Missing periods: ${missingPeriods.join(', ')}`);
    } else {
      console.log('All 48 settlement periods have data');
    }
    
    // Step 6: Final check
    const stats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\n=== Final Data Summary for ${TARGET_DATE} ===`);
    console.log(`Curtailment Records: ${stats[0].recordCount}`);
    console.log(`Settlement Periods: ${stats[0].periodCount}/48`);
    console.log(`Total Energy Volume: ${parseFloat(stats[0].totalVolume || '0').toFixed(2)} MWh`);
    console.log(`Total Payment: £${parseFloat(stats[0].totalPayment || '0').toFixed(2)}`);
    
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
    console.log('REPROCESSING COMPLETED SUCCESSFULLY');
    console.log(`Duration: ${((endTime - startTime) / 1000).toFixed(2)} seconds`);
    console.log('============================================\n');
    
  } catch (error) {
    console.error('\nREPROCESSING FAILED:', error);
    process.exit(1);
  }
}

// Execute the reprocessing
reprocessData()
  .then(() => process.exit(0))
  .catch(error => {
    console.error('Unhandled error during reprocessing:', error);
    process.exit(1);
  });