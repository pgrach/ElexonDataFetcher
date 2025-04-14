/**
 * Direct Reingestion Script for 2025-04-12
 * 
 * This script reprocesses all curtailment data from Elexon for April 12, 2025,
 * ensuring data for all BMUs and all 48 settlement periods is properly ingested.
 * It then calculates Bitcoin mining potential for all supported miner models.
 * 
 * Run with: npx tsx scripts/reprocess-april12.ts
 */

import { processDailyCurtailment } from '../server/services/curtailment';
import { processSingleDay } from '../server/services/bitcoinService';
import { db } from '../db';
import { curtailmentRecords, historicalBitcoinCalculations, dailySummaries } from '../db/schema';
import { eq, sql } from 'drizzle-orm';
import { minerModels } from '../server/types/bitcoin';

const TARGET_DATE = '2025-04-12';
const MINER_MODELS = Object.keys(minerModels); // ['S19J_PRO', 'S9', 'M20S']

async function reprocessDate() {
  console.log(`\n=== Starting Complete Reingestion for ${TARGET_DATE} ===`);
  
  try {
    // Step 1: Remove existing data for clean reingestion
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
    
    console.log(`\nRemoving existing Bitcoin calculations for ${TARGET_DATE}...`);
    // Count records before deletion
    const bitcoinCount = await db
      .select({
        count: sql<number>`COUNT(*)`
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
    // Delete records
    await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
    console.log(`Deleted ${bitcoinCount[0]?.count || 0} existing Bitcoin calculations`);
    
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
    
    // Step 5: Calculate Bitcoin mining potential for all miner models
    console.log(`\nCalculating Bitcoin mining potential for ${MINER_MODELS.length} miner models...`);
    
    for (const minerModel of MINER_MODELS) {
      try {
        console.log(`\nProcessing ${minerModel}...`);
        await processSingleDay(TARGET_DATE, minerModel);
        
        // Verify Bitcoin calculations for this model
        const bitcoinStats = await db
          .select({
            recordCount: sql<number>`COUNT(*)`,
            periodCount: sql<number>`COUNT(DISTINCT ${historicalBitcoinCalculations.settlementPeriod})`,
            farmCount: sql<number>`COUNT(DISTINCT ${historicalBitcoinCalculations.farmId})`,
            totalBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
          })
          .from(historicalBitcoinCalculations)
          .where(
            sql`${historicalBitcoinCalculations.settlementDate} = ${TARGET_DATE} AND 
                ${historicalBitcoinCalculations.minerModel} = ${minerModel}`
          );
        
        console.log(`${minerModel} Bitcoin Calculations:`);
        console.log(`Records: ${bitcoinStats[0]?.recordCount || 0}`);
        console.log(`Total Bitcoin: ${Number(bitcoinStats[0]?.totalBitcoin || 0).toFixed(8)} BTC`);
      } catch (error) {
        console.error(`Error processing ${minerModel} for ${TARGET_DATE}:`, error);
        console.log(`Continuing with next miner model...`);
      }
    }
    
    console.log(`\n=== Reprocessing Complete for ${TARGET_DATE} ===`);
    console.log(`✓ Successfully reingested and processed all data for ${TARGET_DATE}`);
    
  } catch (error) {
    console.error(`\nError during reprocessing:`, error);
    process.exit(1);
  }
}

// Run the reprocessing
reprocessDate();