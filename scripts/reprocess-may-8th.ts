/**
 * Reprocess Data for 2025-05-08
 * 
 * This script is a simplified version specifically for reprocessing 2025-05-08.
 * It performs a complete reprocessing of Elexon data, curtailment records,
 * wind generation data, and Bitcoin calculations for this date.
 * 
 * Usage:
 *   npx tsx scripts/reprocess-may-8th.ts
 */

import { db } from "../db";
import { 
  curtailmentRecords, 
  historicalBitcoinCalculations,
  dailySummaries
} from "../db/schema";
import { processDailyCurtailment } from "../server/services/curtailment_enhanced";
import { processWindDataForDate } from "../server/services/windDataUpdater";
import { processSingleDay } from "../server/services/bitcoinService";
import { eq, and, sql } from "drizzle-orm";

// Constants
const TARGET_DATE = '2025-05-08';
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

/**
 * Main function to reprocess data for 2025-05-08
 */
async function reprocessMay8th() {
  console.log(`\n=== Starting Full Reprocessing for ${TARGET_DATE} ===\n`);
  
  try {
    // Step 1: Clear existing data for the target date
    console.log(`Clearing existing curtailment records for ${TARGET_DATE}...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Clearing existing Bitcoin calculations for ${TARGET_DATE}...`);
    await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
    // Step 2: Reprocess curtailment data
    console.log(`\nReprocessing curtailment data for ${TARGET_DATE}...`);
    try {
      await processDailyCurtailment(TARGET_DATE);
      
      // Verify curtailment data was processed
      const curtailmentStats = await db
        .select({
          count: sql<number>`COUNT(*)`,
          periodCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
          totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
          totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
      console.log(`Successfully reprocessed curtailment data for ${TARGET_DATE}:`, {
        records: curtailmentStats[0].count,
        periods: curtailmentStats[0].periodCount,
        volume: Number(curtailmentStats[0].totalVolume || 0).toFixed(2) + ' MWh',
        payment: '£' + Number(curtailmentStats[0].totalPayment || 0).toFixed(2)
      });
    } catch (error) {
      console.error(`Error processing curtailment data:`, error);
      throw error;
    }
    
    // Step 3: Process wind generation data
    console.log(`\nProcessing wind generation data for ${TARGET_DATE}...`);
    try {
      const windDataProcessed = await processWindDataForDate(TARGET_DATE);
      if (windDataProcessed) {
        console.log(`Successfully processed wind generation data for ${TARGET_DATE}`);
      } else {
        console.log(`No wind generation data found for ${TARGET_DATE}`);
      }
    } catch (error) {
      console.error(`Error processing wind generation data:`, error);
      // Continue even if wind data processing fails
      console.log(`Continuing with Bitcoin calculations despite wind data error`);
    }
    
    // Step 4: Process Bitcoin calculations for each miner model
    console.log(`\nProcessing Bitcoin calculations for ${TARGET_DATE}...`);
    for (const minerModel of MINER_MODELS) {
      try {
        console.log(`Processing Bitcoin calculations for ${minerModel}...`);
        await processSingleDay(TARGET_DATE, minerModel);
        
        // Verify Bitcoin calculations were processed
        const bitcoinStats = await db
          .select({
            count: sql<number>`COUNT(*)`,
            totalBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
          })
          .from(historicalBitcoinCalculations)
          .where(and(
            eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          ));
        
        console.log(`Successfully processed Bitcoin calculations for ${minerModel}:`, {
          records: bitcoinStats[0].count,
          bitcoinMined: Number(bitcoinStats[0].totalBitcoin || 0).toFixed(8) + ' BTC'
        });
      } catch (error) {
        console.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
        // Continue with other miner models even if one fails
      }
    }
    
    // Step 5: Verify daily summary was updated
    const dailySummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, TARGET_DATE)
    });
    
    if (dailySummary) {
      console.log(`\nVerified daily summary for ${TARGET_DATE}:`, {
        energy: Number(dailySummary.totalCurtailedEnergy || 0).toFixed(2) + ' MWh',
        payment: '£' + Number(dailySummary.totalPayment || 0).toFixed(2),
        windGeneration: Number(dailySummary.totalWindGeneration || 0).toFixed(2) + ' MWh'
      });
    } else {
      console.log(`\nWarning: No daily summary found for ${TARGET_DATE}`);
    }
    
    console.log(`\n=== Reprocessing Complete for ${TARGET_DATE} ===`);
    console.log(`All data has been successfully reprocessed.`);
    
  } catch (error) {
    console.error(`\nError during reprocessing:`, error);
    process.exit(1);
  }
}

// Run the reprocessing function
reprocessMay8th();