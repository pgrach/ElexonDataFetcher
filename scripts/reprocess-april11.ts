/**
 * Complete Reprocessing Script for 2025-04-11
 * 
 * This script performs a complete reingestion of curtailment data and Bitcoin calculations
 * for April 11, 2025, followed by wind generation data processing to ensure
 * accurate generation/curtailment percentages.
 * 
 * Run with: npx tsx scripts/reprocess-april11.ts
 */

import { db } from '../db';
import { curtailmentRecords, historicalBitcoinCalculations, dailySummaries } from '../db/schema';
import { eq, and, sql } from 'drizzle-orm';
import { logger } from '../server/utils/logger';
import { addDays, subDays, format } from 'date-fns';
import { processDateRange, getWindGenerationDataForDate, hasWindDataForDate } from '../server/services/windGenerationService';
import { minerModels } from '../server/types/bitcoin';
import { processDailyCurtailment } from '../server/services/curtailment';

// The date we're reprocessing
const TARGET_DATE = '2025-04-11';

/**
 * Process curtailment ingestion for the specified date
 */
async function reprocessCurtailmentData(date: string): Promise<void> {
  try {
    console.log(`\nRemoving existing curtailment records for ${date}...`);
    // Remove existing curtailment records
    const deletedRecords = await db
      .delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .returning({ id: curtailmentRecords.id });
    
    console.log(`Deleted ${deletedRecords.length} existing curtailment records`);
    
    console.log(`\nRemoving existing Bitcoin calculations for ${date}...`);
    // Remove existing Bitcoin calculations
    const deletedBitcoin = await db
      .delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, date))
      .returning({ id: historicalBitcoinCalculations.id });
    
    console.log(`Deleted ${deletedBitcoin.length} existing Bitcoin calculations`);
    
    console.log(`\nIngesting curtailment data for ${date} from Elexon...`);
    
    // Process curtailment data using the correct function
    await processDailyCurtailment(date);
    
    console.log(`Successfully processed data for ${date}`);
    
    // Verify the data
    const curtailmentStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        farmCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.farmId})`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    console.log(`\nCurtailment Data Verification:`);
    console.log(`Records: ${curtailmentStats[0]?.recordCount || 0}`);
    console.log(`Settlement Periods: ${curtailmentStats[0]?.periodCount || 0}`);
    console.log(`Farms: ${curtailmentStats[0]?.farmCount || 0}`);
    console.log(`Total Volume: ${Number(curtailmentStats[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Total Payment: £${Math.abs(Number(curtailmentStats[0]?.totalPayment || 0)).toFixed(2)}`);
    
    // Check daily summary
    const dailySummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date));
    
    if (dailySummary.length > 0) {
      console.log(`\nDaily Summary Verification:`);
      console.log(`Date: ${dailySummary[0].summaryDate}`);
      console.log(`Total Curtailed Energy: ${dailySummary[0].totalCurtailedEnergy} MWh`);
      console.log(`Total Payment: £${Math.abs(Number(dailySummary[0].totalPayment)).toFixed(2)}`);
    } else {
      console.log(`\nNo daily summary found for ${date}`);
    }
    
  } catch (error) {
    console.error(`Error reprocessing curtailment data for ${date}:`, error);
    throw error;
  }
}

/**
 * Process Bitcoin calculations for the specified date
 */
async function processBitcoinCalculations(date: string): Promise<void> {
  try {
    console.log(`\nCalculating Bitcoin mining potential for 3 miner models...`);
    
    // Import required functions
    const { processSingleDay } = await import('../server/services/bitcoinService');
    
    // Process each miner model
    for (const minerModel of Object.keys(minerModels)) {
      console.log(`\nProcessing ${minerModel}...`);
      await processSingleDay(date, minerModel);
    }
    
    // Verify Bitcoin calculations
    const bitcoinStats = await db
      .select({
        minerModel: historicalBitcoinCalculations.minerModel,
        recordCount: sql<number>`COUNT(*)`,
        totalBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, date))
      .groupBy(historicalBitcoinCalculations.minerModel);
    
    console.log(`\nBitcoin Calculation Verification:`);
    for (const stat of bitcoinStats) {
      console.log(`${stat.minerModel}: ${Number(stat.totalBitcoin).toFixed(8)} BTC from ${stat.recordCount} records`);
    }
    
  } catch (error) {
    console.error(`Error processing Bitcoin calculations for ${date}:`, error);
    throw error;
  }
}

/**
 * Process wind generation data for the specified date
 */
async function processWindGeneration(date: string): Promise<void> {
  try {
    console.log(`\nProcessing wind generation data for ${date}...`);
    
    // Check if data already exists
    const hasWindData = await hasWindDataForDate(date);
    
    if (hasWindData) {
      console.log(`Wind generation data already exists for ${date}`);
      console.log('Clearing existing data for reprocessing...');
      
      // Remove existing data
      await db.execute(sql`DELETE FROM wind_generation_data WHERE settlement_date = ${date}::date`);
      console.log('Existing wind generation data cleared successfully.');
    }
    
    // Fetch and process wind generation data
    const recordsProcessed = await processDateRange(date, date);
    console.log(`Processed ${recordsProcessed} wind generation records for ${date}`);
    
    // Get the results for verification
    const windData = await getWindGenerationDataForDate(date);
    console.log(`Retrieved ${windData.length} wind generation records`);
    
    if (windData.length > 0) {
      // Calculate total wind generation
      let totalGeneration = 0;
      for (const record of windData) {
        totalGeneration += parseFloat(record.totalWind);
      }
      
      console.log(`Total wind generation: ${totalGeneration.toFixed(2)} MWh`);
      
      // Get curtailment data for comparison
      const curtailmentStats = await db
        .select({
          totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, date));
      
      const curtailedVolume = Number(curtailmentStats[0]?.totalVolume || 0);
      
      // Calculate percentages
      const curtailmentPercentage = (curtailedVolume / (totalGeneration + curtailedVolume)) * 100;
      
      console.log(`\nWind Farm Percentages for ${date}:`);
      console.log(`Actual Generation: ${(100 - curtailmentPercentage).toFixed(2)}%`);
      console.log(`Curtailed Volume: ${curtailmentPercentage.toFixed(2)}%`);
    }
    
  } catch (error) {
    console.error(`Error processing wind generation data for ${date}:`, error);
    throw error;
  }
}

/**
 * Run the complete reprocessing flow
 */
async function runCompleteReprocessing() {
  console.log(`\n=== Starting Complete Reingestion for ${TARGET_DATE} ===`);
  
  try {
    // Step 1: Reprocess curtailment data
    await reprocessCurtailmentData(TARGET_DATE);
    
    // Step 2: Process Bitcoin calculations
    await processBitcoinCalculations(TARGET_DATE);
    
    // Step 3: Process wind generation data for accurate percentages
    await processWindGeneration(TARGET_DATE);
    
    console.log(`\n=== Reprocessing Complete for ${TARGET_DATE} ===`);
    
  } catch (error) {
    console.error(`\nError during reprocessing:`, error);
    process.exit(1);
  }
}

// Run the reprocessing
runCompleteReprocessing();