/**
 * Direct Reingestion Script for 2025-04-12 (Curtailment Only)
 * 
 * This script reprocesses curtailment data from Elexon for April 12, 2025,
 * ensuring data for all BMUs and all 48 settlement periods is properly ingested.
 * Bitcoin calculations can be run separately if needed.
 * 
 * Run with: npx tsx scripts/reprocess-april12-curtailment-only.ts
 */

import { processDailyCurtailment } from '../server/services/curtailment';
import { db } from '../db';
import { curtailmentRecords, dailySummaries } from '../db/schema';
import { eq, sql } from 'drizzle-orm';

const TARGET_DATE = '2025-04-12';

async function reprocessCurtailmentOnly() {
  console.log(`\n=== Starting Curtailment Reingestion for ${TARGET_DATE} ===`);
  
  try {
    // Step 1: Remove existing curtailment records
    console.log(`\nRemoving existing curtailment records for ${TARGET_DATE}...`);
    // Count records before deletion
    const curtailmentCount = await db
      .select({
        count: sql<number>`COUNT(*)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Delete records
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Deleted ${curtailmentCount[0]?.count || 0} existing curtailment records`);
    
    // Step 2: Reingest curtailment data from Elexon
    console.log(`\nIngesting curtailment data for ${TARGET_DATE} from Elexon...`);
    await processDailyCurtailment(TARGET_DATE);
    
    // Step 3: Verify ingested curtailment data
    const curtailmentStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        farmCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.farmId})`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nCurtailment Data Verification:`);
    console.log(`Records: ${curtailmentStats[0]?.recordCount || 0}`);
    console.log(`Settlement Periods: ${curtailmentStats[0]?.periodCount || 0}`);
    console.log(`Farms: ${curtailmentStats[0]?.farmCount || 0}`);
    console.log(`Total Volume: ${Number(curtailmentStats[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Total Payment: £${Number(curtailmentStats[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Step 4: Check daily summary
    const summary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    console.log(`\nDaily Summary Verification:`);
    if (summary.length > 0) {
      console.log(`Date: ${summary[0].summaryDate}`);
      console.log(`Total Curtailed Energy: ${summary[0].totalCurtailedEnergy} MWh`);
      console.log(`Total Payment: £${summary[0].totalPayment}`);
    } else {
      console.log(`No daily summary found for ${TARGET_DATE}`);
    }
    
    console.log(`\n=== Curtailment Reprocessing Complete for ${TARGET_DATE} ===`);
    console.log(`✓ Successfully reingested curtailment data for ${TARGET_DATE}`);
    console.log(`\nNote: Bitcoin calculations were not performed. Run bitcoinService.processSingleDay() separately if needed.`);
    
  } catch (error) {
    console.error(`\nError during reprocessing:`, error);
    process.exit(1);
  }
}

// Run the reprocessing
reprocessCurtailmentOnly();