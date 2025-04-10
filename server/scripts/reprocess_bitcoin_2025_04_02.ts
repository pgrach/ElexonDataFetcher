/**
 * Reprocess Bitcoin Calculations for 2025-04-02
 * 
 * This script focuses on reprocessing Bitcoin calculations for 2025-04-02 specifically,
 * using the existing curtailment_records and updating all related summary tables.
 * It processes all three miner models: S19J_PRO, S9, and M20S.
 */

import { db } from "@db";
import { 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries, 
  bitcoinMonthlySummaries, 
  bitcoinYearlySummaries 
} from "@db/schema";
import { processSingleDay } from "../services/bitcoinService";
import { and, eq, sql } from "drizzle-orm";
import { performance } from "perf_hooks";

// Target date
const TARGET_DATE = '2025-04-02';

// Miner models to process
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

/**
 * Clear existing Bitcoin calculations for the target date
 */
async function clearExistingCalculations(): Promise<void> {
  console.log(`\n==== Clearing existing Bitcoin calculations for ${TARGET_DATE} ====\n`);
  
  try {
    // 1. Clear historical_bitcoin_calculations
    for (const minerModel of MINER_MODELS) {
      await db.delete(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      console.log(`Cleared historical Bitcoin calculations for ${TARGET_DATE} and ${minerModel}`);
    }
    
    // 2. Clear bitcoin_daily_summaries
    for (const minerModel of MINER_MODELS) {
      await db.delete(bitcoinDailySummaries)
        .where(and(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        ));
      
      console.log(`Cleared Bitcoin daily summaries for ${TARGET_DATE} and ${minerModel}`);
    }
    
    console.log(`\n==== Successfully cleared existing Bitcoin calculations ====\n`);
  } catch (error) {
    console.error('Error clearing existing Bitcoin calculations:', error);
    throw error;
  }
}

/**
 * Reprocess Bitcoin calculations
 */
async function reprocessBitcoinCalculations(): Promise<void> {
  const startTime = performance.now();
  
  try {
    // Step 1: Clear existing calculations
    await clearExistingCalculations();
    
    // Step 2: Process Bitcoin calculations for each miner model
    console.log(`\n==== Processing Bitcoin calculations for ${TARGET_DATE} ====\n`);
    
    for (const minerModel of MINER_MODELS) {
      console.log(`\n==== Processing ${minerModel} miner model ====\n`);
      await processSingleDay(TARGET_DATE, minerModel);
    }
    
    // Step 3: Verify Bitcoin calculations
    await verifyBitcoinCalculations();
    
    const endTime = performance.now();
    const durationSeconds = ((endTime - startTime) / 1000).toFixed(2);
    
    console.log(`\n==== Reprocessing completed successfully ====`);
    console.log(`Total execution time: ${durationSeconds} seconds`);
    
  } catch (error) {
    console.error(`Error during Bitcoin reprocessing:`, error);
    throw error;
  }
}

/**
 * Verify Bitcoin calculations were created correctly
 */
async function verifyBitcoinCalculations(): Promise<void> {
  console.log(`\n==== Verifying Bitcoin calculations ====\n`);
  
  try {
    for (const minerModel of MINER_MODELS) {
      // Check historical Bitcoin calculations
      const historicalCount = await db
        .select({ count: sql<number>`COUNT(*)::int` })
        .from(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      console.log(`Historical Bitcoin calculations for ${minerModel}: ${historicalCount[0]?.count || 0} records`);
      
      // Check historical Bitcoin total
      const historicalTotal = await db
        .select({ total: sql<string>`SUM(bitcoin_mined::numeric)` })
        .from(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      console.log(`Total Bitcoin calculated for ${minerModel}: ${historicalTotal[0]?.total || '0'} BTC`);
      
      // Check daily summary
      const dailySummary = await db
        .select()
        .from(bitcoinDailySummaries)
        .where(and(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        ));
      
      if (dailySummary.length > 0) {
        console.log(`Daily summary for ${minerModel}: ${dailySummary[0].bitcoinMined} BTC`);
      } else {
        console.log(`Warning: No daily summary found for ${minerModel}`);
      }
    }
    
    // Check monthly summaries
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM
    for (const minerModel of MINER_MODELS) {
      const monthlySummary = await db
        .select()
        .from(bitcoinMonthlySummaries)
        .where(and(
          eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        ));
      
      if (monthlySummary.length > 0) {
        console.log(`Monthly summary for ${yearMonth} and ${minerModel}: ${monthlySummary[0].bitcoinMined} BTC`);
      } else {
        console.log(`Warning: No monthly summary found for ${yearMonth} and ${minerModel}`);
      }
    }
    
    // Check yearly summaries
    const year = TARGET_DATE.substring(0, 4); // YYYY
    for (const minerModel of MINER_MODELS) {
      const yearlySummary = await db
        .select()
        .from(bitcoinYearlySummaries)
        .where(and(
          eq(bitcoinYearlySummaries.year, year),
          eq(bitcoinYearlySummaries.minerModel, minerModel)
        ));
      
      if (yearlySummary.length > 0) {
        console.log(`Yearly summary for ${year} and ${minerModel}: ${yearlySummary[0].bitcoinMined} BTC`);
      } else {
        console.log(`Warning: No yearly summary found for ${year} and ${minerModel}`);
      }
    }
    
    console.log(`\n==== Verification completed ====\n`);
  } catch (error) {
    console.error('Error verifying Bitcoin calculations:', error);
    throw error;
  }
}

// Execute the reprocessing
reprocessBitcoinCalculations()
  .then(() => {
    console.log('Bitcoin calculation reprocessing completed successfully. Exiting...');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Bitcoin calculation reprocessing failed with error:', error);
    process.exit(1);
  });