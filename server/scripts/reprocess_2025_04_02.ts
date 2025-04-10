/**
 * Reprocess Script for 2025-04-02
 * 
 * This script reprocesses all 48 settlement periods from Elexon API for 2025-04-02
 * and updates all dependent tables including:
 * - curtailment_records
 * - daily_summaries, monthly_summaries, yearly_summaries
 * - historical_bitcoin_calculations
 * - bitcoin_daily_summaries, bitcoin_monthly_summaries, bitcoin_yearly_summaries
 */

import { db } from "@db";
import { processDailyCurtailment } from "../services/curtailment";
import { processSingleDay } from "../services/bitcoinService";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries, 
         historicalBitcoinCalculations, bitcoinDailySummaries, bitcoinMonthlySummaries,
         bitcoinYearlySummaries } from "@db/schema";
import { eq, and, sql } from "drizzle-orm";
import { performance } from "perf_hooks";

// Target date to reprocess
const TARGET_DATE = '2025-04-02';

// Miner models to process for Bitcoin calculations
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

/**
 * Clear all existing data for the target date to ensure clean reprocessing
 */
async function clearExistingData(): Promise<void> {
  console.log(`\n==== Clearing existing data for ${TARGET_DATE} ====\n`);
  
  try {
    // 1. Clear curtailment_records
    const deletedRecords = await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .returning({ id: curtailmentRecords.id });
    
    console.log(`Deleted ${deletedRecords.length} curtailment records`);
    
    // 2. Clear historical_bitcoin_calculations
    for (const minerModel of MINER_MODELS) {
      const deletedCalcs = await db.delete(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ))
        .returning({ id: historicalBitcoinCalculations.id });
      
      console.log(`Deleted ${deletedCalcs.length} historical Bitcoin calculations for ${minerModel}`);
    }
    
    // 3. Clear bitcoin_daily_summaries
    for (const minerModel of MINER_MODELS) {
      const deletedSummaries = await db.delete(bitcoinDailySummaries)
        .where(and(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        ))
        .returning({ id: bitcoinDailySummaries.id });
      
      console.log(`Deleted ${deletedSummaries.length} Bitcoin daily summaries for ${minerModel}`);
    }
    
    // 4. Delete daily_summary for this date
    const deletedDailySummary = await db.delete(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE))
      .returning({ id: dailySummaries.id });
    
    console.log(`Deleted ${deletedDailySummary.length} daily summaries`);
    
    console.log(`\n==== Successfully cleared all existing data for ${TARGET_DATE} ====\n`);
  } catch (error) {
    console.error('Error clearing existing data:', error);
    throw error;
  }
}

/**
 * Run the complete reprocessing pipeline
 */
async function reprocessData(): Promise<void> {
  const startTime = performance.now();
  
  try {
    console.log(`\n==== Starting reprocessing for ${TARGET_DATE} ====\n`);
    
    // Step 1: Clear existing data
    await clearExistingData();
    
    // Step 2: Process curtailment records and update summaries
    console.log(`\n==== Processing curtailment data from Elexon API ====\n`);
    await processDailyCurtailment(TARGET_DATE);
    
    // Step 3: Process Bitcoin calculations for each miner model
    console.log(`\n==== Processing Bitcoin calculations ====\n`);
    for (const minerModel of MINER_MODELS) {
      console.log(`Processing Bitcoin calculations for ${minerModel}...`);
      await processSingleDay(TARGET_DATE, minerModel);
    }
    
    // Step 4: Verify data integrity
    console.log(`\n==== Verifying processed data ====\n`);
    await verifyProcessedData();
    
    const endTime = performance.now();
    const durationSeconds = ((endTime - startTime) / 1000).toFixed(2);
    
    console.log(`\n==== Reprocessing completed successfully ====`);
    console.log(`Total execution time: ${durationSeconds} seconds`);
    
  } catch (error) {
    console.error(`Error during reprocessing:`, error);
    throw error;
  }
}

/**
 * Verify that all data was processed correctly
 */
async function verifyProcessedData(): Promise<void> {
  try {
    // 1. Check curtailment_records
    const curtailmentCount = await db
      .select({ count: sql<number>`COUNT(*)::int` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Curtailment records: ${curtailmentCount[0]?.count || 0}`);
    
    // 2. Check daily_summary
    const dailySummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    if (dailySummary.length > 0) {
      console.log(`Daily summary: Total curtailed energy = ${dailySummary[0].totalCurtailedEnergy} MWh, Total payment = £${dailySummary[0].totalPayment}`);
    } else {
      console.log(`Warning: No daily summary found for ${TARGET_DATE}`);
    }
    
    // 3. Check monthly summary
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM
    const monthlySummary = await db
      .select()
      .from(monthlySummaries)
      .where(eq(monthlySummaries.yearMonth, yearMonth));
    
    if (monthlySummary.length > 0) {
      console.log(`Monthly summary for ${yearMonth}: Total curtailed energy = ${monthlySummary[0].totalCurtailedEnergy} MWh, Total payment = £${monthlySummary[0].totalPayment}`);
    } else {
      console.log(`Warning: No monthly summary found for ${yearMonth}`);
    }
    
    // 4. Check historical Bitcoin calculations
    for (const minerModel of MINER_MODELS) {
      const bitcoinCalcCount = await db
        .select({ count: sql<number>`COUNT(*)::int` })
        .from(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      console.log(`Historical Bitcoin calculations for ${minerModel}: ${bitcoinCalcCount[0]?.count || 0}`);
    }
    
    // 5. Check Bitcoin daily summaries
    for (const minerModel of MINER_MODELS) {
      const bitcoinDailySummary = await db
        .select()
        .from(bitcoinDailySummaries)
        .where(and(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        ));
      
      if (bitcoinDailySummary.length > 0) {
        console.log(`Bitcoin daily summary for ${minerModel}: ${bitcoinDailySummary[0].bitcoinMined} BTC`);
      } else {
        console.log(`Warning: No Bitcoin daily summary found for ${minerModel}`);
      }
    }
    
  } catch (error) {
    console.error('Error verifying processed data:', error);
    throw error;
  }
}

// Execute the reprocessing
reprocessData()
  .then(() => {
    console.log('Reprocessing completed successfully. Exiting...');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Reprocessing failed with error:', error);
    process.exit(1);
  });