/**
 * Script to reprocess curtailment data for 2025-04-03
 * 
 * This script fetches data from Elexon API for all 48 settlement periods
 * and updates all dependent tables (summaries, Bitcoin calculations)
 */

import { processDailyCurtailment } from "../server/services/curtailment_enhanced";
import { db } from "../db";
import { 
  curtailmentRecords, 
  dailySummaries, 
  bitcoinDailySummaries, 
  historicalBitcoinCalculations 
} from "../db/schema";
import { eq, sql } from "drizzle-orm";
import { processSingleDay } from "../server/services/bitcoinService";
import { manualUpdateYearlyBitcoinSummary } from "../server/services/bitcoinService";
import { minerModels } from "../server/types/bitcoin";
import { calculateMonthlyBitcoinSummary } from "../server/services/bitcoinService";

// Target date for reprocessing
const TARGET_DATE = "2025-04-03";
const YEAR_MONTH = TARGET_DATE.substring(0, 7); // "2025-04" 
const YEAR = TARGET_DATE.substring(0, 4); // "2025"

// List of miner models to process
const MINER_MODEL_LIST = Object.keys(minerModels);

/**
 * Main function to orchestrate the reprocessing
 */
async function reprocessData() {
  console.log("===== REPROCESSING CURTAILMENT DATA FOR 2025-04-03 =====");
  console.log("\nStep 1: Checking current state");
  
  try {
    // Check if data already exists for this date
    const existingRecords = await db
      .select({
        count: sql<number>`COUNT(*)::int`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const recordCount = existingRecords[0]?.count || 0;
    console.log(`Found ${recordCount} existing curtailment records for ${TARGET_DATE}`);
    
    // Check daily summary
    const existingSummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, TARGET_DATE)
    });
    
    console.log(`Daily summary exists: ${existingSummary ? 'Yes' : 'No'}`);
    
    if (existingSummary) {
      console.log(`  Energy: ${existingSummary.totalCurtailedEnergy} MWh`);
      console.log(`  Payment: £${existingSummary.totalPayment}`);
    }
    
    console.log("\nStep 2: Clearing existing data for clean reprocessing");
    
    // Clear existing curtailment records for the date
    if (recordCount > 0) {
      await db.delete(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      console.log(`Deleted ${recordCount} existing curtailment records`);
    }
    
    // Clear existing daily summary
    if (existingSummary) {
      await db.delete(dailySummaries)
        .where(eq(dailySummaries.summaryDate, TARGET_DATE));
      console.log(`Deleted existing daily summary`);
    }
    
    // Clear existing Bitcoin calculations
    for (const minerModel of Object.keys(minerModels)) {
      const deletedBitcoin = await db.delete(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
      
      console.log(`Deleted Bitcoin calculations for ${minerModel}`);
      
      // Delete daily Bitcoin summary
      await db.delete(bitcoinDailySummaries)
        .where(
          and(
            eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
            eq(bitcoinDailySummaries.minerModel, minerModel)
          )
        );
      
      console.log(`Deleted daily Bitcoin summary for ${minerModel}`);
    }
    
    console.log("\nStep 3: Fetching and processing curtailment data from Elexon API");
    
    // Trigger the processDailyCurtailment function to fetch and process all periods for the date
    const startTime = Date.now();
    await processDailyCurtailment(TARGET_DATE);
    const processingTime = (Date.now() - startTime) / 1000;
    
    console.log(`Completed processing in ${processingTime.toFixed(2)}s`);
    
    // Verify the new records
    const newRecords = await db
      .select({
        count: sql<number>`COUNT(*)::int`,
        periods: sql<number>`COUNT(DISTINCT settlement_period)::int`,
        totalVolume: sql<string>`SUM(ABS(volume))::text`,
        totalPayment: sql<string>`SUM(payment)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log("\nStep 4: Verifying new curtailment data");
    console.log(`New Records: ${newRecords[0]?.count || 0}`);
    console.log(`Settlement Periods: ${newRecords[0]?.periods || 0} (out of 48)`);
    console.log(`Total Volume: ${newRecords[0]?.totalVolume || '0'} MWh`);
    console.log(`Total Payment: £${newRecords[0]?.totalPayment || '0'}`);
    
    // Check daily summary
    const newSummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, TARGET_DATE)
    });
    
    console.log("\nStep 5: Verifying daily summary");
    if (newSummary) {
      console.log(`Daily summary created successfully`);
      console.log(`  Energy: ${newSummary.totalCurtailedEnergy} MWh`);
      console.log(`  Payment: £${newSummary.totalPayment}`);
    } else {
      console.log(`No daily summary created - likely no curtailment occurred on this date`);
    }
    
    console.log("\nStep 6: Processing Bitcoin calculations");
    
    // Process Bitcoin calculations for each miner model
    for (const minerModel of Object.keys(minerModels)) {
      console.log(`Processing Bitcoin calculations for ${minerModel}...`);
      await processSingleDay(TARGET_DATE, minerModel);
      
      // Verify Bitcoin calculations
      const bitcoinStats = await db
        .select({
          count: sql<number>`COUNT(*)::int`,
          totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
      
      console.log(`  Records: ${bitcoinStats[0]?.count || 0}`);
      console.log(`  Total Bitcoin: ${bitcoinStats[0]?.totalBitcoin || '0'} BTC`);
    }
    
    console.log("\nStep 7: Updating monthly Bitcoin summaries");
    
    // Update monthly summaries
    for (const minerModel of Object.keys(minerModels)) {
      console.log(`Updating monthly Bitcoin summary for ${YEAR_MONTH} and ${minerModel}...`);
      await calculateMonthlyBitcoinSummary(YEAR_MONTH, minerModel);
    }
    
    console.log("\nStep 8: Updating yearly Bitcoin summaries");
    
    // Update yearly summaries
    console.log(`Updating yearly Bitcoin summary for ${YEAR}...`);
    await manualUpdateYearlyBitcoinSummary(YEAR);
    
    console.log("\n===== REPROCESSING COMPLETE =====");
    console.log(`Successfully reprocessed data for ${TARGET_DATE}`);
    console.log("All dependent tables have been updated");
    
    process.exit(0);
  } catch (error) {
    console.error("ERROR DURING REPROCESSING:", error);
    process.exit(1);
  }
}

// Add missing 'and' function import
import { and } from "drizzle-orm";

// Execute the main function
reprocessData();