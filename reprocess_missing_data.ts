/**
 * Re-ingest Script for Missing Curtailment Data
 * 
 * This script re-ingests curtailment records for 2025-03-05 from the Elexon API,
 * specifically focused on periods 32-48 which are missing in the current database.
 * 
 * Usage:
 *   npx tsx reprocess_missing_data.ts
 */

import { processDailyCurtailment } from "./server/services/curtailment";
import { processSingleDay } from "./server/services/bitcoinService";
import { db } from "@db";
import { format } from 'date-fns';
import { curtailmentRecords } from "@db/schema";
import { eq, sql } from "drizzle-orm";

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

async function reprocessData() {
  console.log(`\n=== Reprocessing Curtailment Data for ${TARGET_DATE} ===\n`);
  
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
    // Process the entire day - will delete existing records and re-ingest all periods
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
    
    // Update Bitcoin calculations based on the refreshed data
    console.log(`\nUpdating Bitcoin calculations for ${TARGET_DATE}...`);
    
    // Process for all miner models
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    for (const model of minerModels) {
      await processSingleDay(TARGET_DATE, model);
    }
    
    console.log(`\n=== Reprocessing Complete for ${TARGET_DATE} ===`);
  } catch (error) {
    console.error(`Error reprocessing data for ${TARGET_DATE}:`, error);
  }
}

// Execute the reprocessing
reprocessData();