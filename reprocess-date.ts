/**
 * Script to reingest data for a specific date
 * 
 * This script uses the existing API endpoint for reingestion
 * to update curtailment records and trigger cascading updates.
 */

import axios from 'axios';
import { processDailyCurtailment } from './server/services/curtailment';
import { processSingleDay } from './server/services/bitcoinService';
import { db } from './db';
import { curtailmentRecords, dailySummaries } from './db/schema';
import { eq, sql } from 'drizzle-orm';

const TARGET_DATE = "2025-03-05";
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

async function main() {
  console.log(`\n=== Starting Targeted Data Reingestion for ${TARGET_DATE} ===\n`);
  
  try {
    console.log("Step 1: Reingesting curtailment data from Elexon API...");
    // Process curtailment data for the target date
    await processDailyCurtailment(TARGET_DATE);
    
    // Verify the curtailment data update
    const curtailmentStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nCurtailment records updated for ${TARGET_DATE}:`);
    console.log(`- Records: ${curtailmentStats[0]?.recordCount || 0}`);
    console.log(`- Unique periods: ${curtailmentStats[0]?.periodCount || 0}`);
    console.log(`- Unique farms: ${curtailmentStats[0]?.farmCount || 0}`);
    console.log(`- Total volume: ${Number(curtailmentStats[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`- Total payment: £${Number(curtailmentStats[0]?.totalPayment || 0).toFixed(2)}`);
    
    console.log("\nStep 2: Updating Bitcoin calculations...");
    
    // Process Bitcoin calculations for all miner models
    for (const minerModel of MINER_MODELS) {
      console.log(`Processing ${minerModel}...`);
      await processSingleDay(TARGET_DATE, minerModel);
    }
    
    console.log(`\n=== Data Reingestion Complete for ${TARGET_DATE} ===\n`);
    console.log("Summary of updates:");
    
    // Get final daily summary
    const summary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, TARGET_DATE)
    });
    
    console.log(`Energy: ${Number(summary?.totalCurtailedEnergy || 0).toFixed(2)} MWh`);
    console.log(`Payment: £${Number(summary?.totalPayment || 0).toFixed(2)}`);
    
    process.exit(0);
  } catch (error) {
    console.error("Error during data reingestion:", error);
    process.exit(1);
  }
}

// Run the script
main();