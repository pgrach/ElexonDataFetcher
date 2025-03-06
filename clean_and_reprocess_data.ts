/**
 * Clean and Reprocess Curtailment Data
 * 
 * This script removes all existing curtailment records for 2025-03-05
 * and then runs a clean reprocessing to ensure no duplicates.
 * 
 * Usage:
 *   npx tsx clean_and_reprocess_data.ts
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations, dailySummaries } from "./db/schema";
import { processDailyCurtailment } from "./server/services/curtailment";
import { processSingleDay } from "./server/services/bitcoinService";
import { eq, sql, and } from "drizzle-orm";

const TARGET_DATE = '2025-03-05';

async function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function verifyDataCompleteness(): Promise<boolean> {
  // Check if we have all 48 periods
  const periodCheck = await db
    .select({
      count: sql<number>`COUNT(DISTINCT settlement_period)`,
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));

  return periodCheck[0].count === 48;
}

async function cleanAndReprocessData() {
  console.log(`\n=== Cleaning and Reprocessing Curtailment Data for ${TARGET_DATE} ===\n`);
  
  // Check initial state
  console.log('Checking initial database state...');
  const beforeCheck = await db
    .select({
      recordCount: sql<number>`COUNT(*)`,
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  console.log(`Initial state: ${beforeCheck[0].recordCount} records across ${beforeCheck[0].periodCount} periods`);
  console.log(`Total volume: ${Number(beforeCheck[0].totalVolume).toFixed(2)} MWh`);
  console.log(`Total payment: £${Number(beforeCheck[0].totalPayment).toFixed(2)}`);

  try {
    // Count duplicate entries
    const duplicateCheck = await db.execute(sql`
      SELECT COUNT(*) as duplicate_count
      FROM (
        SELECT settlement_period, farm_id, COUNT(*) 
        FROM curtailment_records 
        WHERE settlement_date = ${TARGET_DATE}
        GROUP BY settlement_period, farm_id
        HAVING COUNT(*) > 1
      ) as duplicates
    `);
    
    console.log(`Found ${duplicateCheck.rows[0].duplicate_count} records with duplicates`);

    // Step 1: Count and then delete all Bitcoin calculations for this date
    console.log(`\nDeleting all Bitcoin calculations for ${TARGET_DATE}...`);
    const calcCount = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
    await db
      .delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
      
    console.log(`Deleted ${calcCount[0]?.count || 0} Bitcoin calculation records`);

    // Step 2: Count and then delete all curtailment records for this date
    console.log(`\nDeleting all curtailment records for ${TARGET_DATE}...`);
    const recordCount = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    await db
      .delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
    console.log(`Deleted ${recordCount[0]?.count || 0} curtailment records`);

    // Step 3: Delete daily summary for this date
    console.log(`\nDeleting daily summary for ${TARGET_DATE}...`);
    await db
      .delete(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    // Step 4: Process the day from scratch
    console.log(`\nReprocessing data for ${TARGET_DATE}...`);
    await processDailyCurtailment(TARGET_DATE);
    
    // Verify all periods were processed
    const isComplete = await verifyDataCompleteness();
    
    if (!isComplete) {
      console.error(`Warning: Not all 48 periods were processed for ${TARGET_DATE}`);
    }
    
    // Get final data state
    const afterCheck = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nFinal state: ${afterCheck[0].recordCount} records across ${afterCheck[0].periodCount} periods`);
    console.log(`Total volume: ${Number(afterCheck[0].totalVolume).toFixed(2)} MWh`);
    console.log(`Total payment: £${Number(afterCheck[0].totalPayment).toFixed(2)}`);
    
    // Check for duplicates after reprocessing
    const finalDuplicateCheck = await db.execute(sql`
      SELECT COUNT(*) as duplicate_count
      FROM (
        SELECT settlement_period, farm_id, COUNT(*) 
        FROM curtailment_records 
        WHERE settlement_date = ${TARGET_DATE}
        GROUP BY settlement_period, farm_id
        HAVING COUNT(*) > 1
      ) as duplicates
    `);
    
    console.log(`Duplicates after reprocessing: ${finalDuplicateCheck.rows[0].duplicate_count}`);
    
    // Update Bitcoin calculations based on the refreshed data
    console.log(`\nUpdating Bitcoin calculations for ${TARGET_DATE}...`);
    
    // Process for all miner models
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    for (const model of minerModels) {
      await processSingleDay(TARGET_DATE, model);
    }
    
    console.log(`\n=== Cleaning and Reprocessing Complete for ${TARGET_DATE} ===`);
  } catch (error) {
    console.error(`Error reprocessing data for ${TARGET_DATE}:`, error);
  }
}

// Execute the reprocessing
cleanAndReprocessData();