/**
 * Complete Bitcoin Calculations for 2025-04-02
 * 
 * This script completes the Bitcoin calculation pipeline for 2025-04-02 by:
 * 1. Processing Bitcoin calculations for all miner models
 * 2. Generating Bitcoin daily summaries
 * 3. Updating Bitcoin monthly summaries
 * 4. Updating Bitcoin yearly summaries
 */

import { db } from "@db";
import { processSingleDay } from "../services/bitcoinService";
import { historicalBitcoinCalculations, bitcoinDailySummaries, 
         bitcoinMonthlySummaries, bitcoinYearlySummaries } from "@db/schema";
import { eq, and, sql } from "drizzle-orm";
import { performance } from "perf_hooks";

// Target date to process
const TARGET_DATE = '2025-04-02';

// All miner models to process
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

/**
 * Run the Bitcoin calculation pipeline
 */
async function processBitcoinCalculations(): Promise<void> {
  const startTime = performance.now();
  
  try {
    console.log(`\n==== Processing Bitcoin Calculations for ${TARGET_DATE} ====\n`);
    
    // Step 1: Process Bitcoin calculations for each miner model
    for (const minerModel of MINER_MODELS) {
      console.log(`Processing Bitcoin calculations for ${minerModel}...`);
      try {
        // Clear existing records first to avoid duplicates
        await db.delete(historicalBitcoinCalculations)
          .where(and(
            eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          ));
        
        // Process the calculations
        await processSingleDay(TARGET_DATE, minerModel);
        
        // Verify the calculations were created
        const calcCount = await db.select({ count: sql<number>`COUNT(*)::int` })
          .from(historicalBitcoinCalculations)
          .where(and(
            eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          ));
        
        console.log(`Created ${calcCount[0]?.count || 0} Bitcoin calculations for ${minerModel}`);
      } catch (error) {
        console.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
      }
    }
    
    // Step 2: Update Bitcoin daily summaries
    console.log(`\n==== Updating Bitcoin Daily Summaries ====\n`);
    
    for (const minerModel of MINER_MODELS) {
      try {
        // Calculate total Bitcoin mined for the day
        const bitcoinTotal = await db.execute(sql`
          SELECT SUM(bitcoin_mined::NUMERIC) as total_bitcoin
          FROM historical_bitcoin_calculations
          WHERE settlement_date = ${TARGET_DATE}
          AND miner_model = ${minerModel}
        `);
        
        const totalBitcoin = bitcoinTotal.rows?.[0]?.total_bitcoin;
        
        if (!totalBitcoin) {
          console.log(`No Bitcoin data found for ${TARGET_DATE} and ${minerModel}`);
          continue;
        }
        
        // Delete existing summary if any
        await db.delete(bitcoinDailySummaries)
          .where(and(
            eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
            eq(bitcoinDailySummaries.minerModel, minerModel)
          ));
        
        // Insert new summary
        await db.insert(bitcoinDailySummaries).values({
          summaryDate: TARGET_DATE,
          minerModel: minerModel,
          bitcoinMined: totalBitcoin.toString(),
          updatedAt: new Date(),
          createdAt: new Date()
        });
        
        console.log(`Updated Bitcoin daily summary for ${minerModel}: ${totalBitcoin} BTC`);
      } catch (error) {
        console.error(`Error updating Bitcoin daily summary for ${minerModel}:`, error);
      }
    }
    
    // Step 3: Update Bitcoin monthly summaries
    console.log(`\n==== Updating Bitcoin Monthly Summaries ====\n`);
    
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM
    
    for (const minerModel of MINER_MODELS) {
      try {
        // Calculate monthly total from daily summaries
        const monthlyTotal = await db.execute(sql`
          SELECT SUM(bitcoin_mined::NUMERIC) as total_bitcoin
          FROM bitcoin_daily_summaries
          WHERE summary_date LIKE ${yearMonth + '%'}
          AND miner_model = ${minerModel}
        `);
        
        const totalBitcoin = monthlyTotal.rows?.[0]?.total_bitcoin;
        
        if (!totalBitcoin) {
          console.log(`No Bitcoin daily summary data found for ${yearMonth} and ${minerModel}`);
          continue;
        }
        
        // Delete existing monthly summary if any
        await db.delete(bitcoinMonthlySummaries)
          .where(and(
            eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
            eq(bitcoinMonthlySummaries.minerModel, minerModel)
          ));
        
        // Insert new monthly summary
        await db.insert(bitcoinMonthlySummaries).values({
          yearMonth: yearMonth,
          minerModel: minerModel,
          bitcoinMined: totalBitcoin.toString(),
          updatedAt: new Date()
        });
        
        console.log(`Updated Bitcoin monthly summary for ${yearMonth} and ${minerModel}: ${totalBitcoin} BTC`);
      } catch (error) {
        console.error(`Error updating Bitcoin monthly summary for ${minerModel}:`, error);
      }
    }
    
    // Step 4: Update Bitcoin yearly summaries
    console.log(`\n==== Updating Bitcoin Yearly Summaries ====\n`);
    
    const year = TARGET_DATE.substring(0, 4); // YYYY
    
    for (const minerModel of MINER_MODELS) {
      try {
        // Calculate yearly total from monthly summaries
        const yearlyTotal = await db.execute(sql`
          SELECT SUM(bitcoin_mined::NUMERIC) as total_bitcoin
          FROM bitcoin_monthly_summaries
          WHERE year_month LIKE ${year + '%'}
          AND miner_model = ${minerModel}
        `);
        
        const totalBitcoin = yearlyTotal.rows?.[0]?.total_bitcoin;
        
        if (!totalBitcoin) {
          console.log(`No Bitcoin monthly summary data found for ${year} and ${minerModel}`);
          continue;
        }
        
        // Delete existing yearly summary if any
        await db.delete(bitcoinYearlySummaries)
          .where(and(
            eq(bitcoinYearlySummaries.year, year),
            eq(bitcoinYearlySummaries.minerModel, minerModel)
          ));
        
        // Insert new yearly summary
        await db.insert(bitcoinYearlySummaries).values({
          year: year,
          minerModel: minerModel,
          bitcoinMined: totalBitcoin.toString(),
          updatedAt: new Date()
        });
        
        console.log(`Updated Bitcoin yearly summary for ${year} and ${minerModel}: ${totalBitcoin} BTC`);
      } catch (error) {
        console.error(`Error updating Bitcoin yearly summary for ${minerModel}:`, error);
      }
    }
    
    // Final verification
    console.log(`\n==== Verifying Bitcoin data pipeline ====\n`);
    await verifyBitcoinPipeline();
    
    const endTime = performance.now();
    const durationSeconds = ((endTime - startTime) / 1000).toFixed(2);
    
    console.log(`\n==== Processing completed successfully ====`);
    console.log(`Total execution time: ${durationSeconds} seconds`);
    
  } catch (error) {
    console.error(`Error during Bitcoin processing:`, error);
    throw error;
  }
}

/**
 * Verify the Bitcoin data pipeline has been properly updated
 */
async function verifyBitcoinPipeline(): Promise<void> {
  try {
    // Check historical calculations
    for (const minerModel of MINER_MODELS) {
      const calcCount = await db.select({ count: sql<number>`COUNT(*)::int` })
        .from(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      console.log(`Historical Bitcoin calculations for ${minerModel}: ${calcCount[0]?.count || 0}`);
    }
    
    // Check daily summaries
    for (const minerModel of MINER_MODELS) {
      const dailySummary = await db.select()
        .from(bitcoinDailySummaries)
        .where(and(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        ));
      
      if (dailySummary.length > 0) {
        console.log(`Bitcoin daily summary for ${minerModel}: ${dailySummary[0].bitcoinMined} BTC`);
      } else {
        console.log(`Warning: No Bitcoin daily summary found for ${minerModel}`);
      }
    }
    
    // Check monthly summaries
    const yearMonth = TARGET_DATE.substring(0, 7);
    for (const minerModel of MINER_MODELS) {
      const monthlySummary = await db.select()
        .from(bitcoinMonthlySummaries)
        .where(and(
          eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        ));
      
      if (monthlySummary.length > 0) {
        console.log(`Bitcoin monthly summary for ${yearMonth} and ${minerModel}: ${monthlySummary[0].bitcoinMined} BTC`);
      } else {
        console.log(`Warning: No Bitcoin monthly summary found for ${yearMonth} and ${minerModel}`);
      }
    }
    
    // Check yearly summaries
    const year = TARGET_DATE.substring(0, 4);
    for (const minerModel of MINER_MODELS) {
      const yearlySummary = await db.select()
        .from(bitcoinYearlySummaries)
        .where(and(
          eq(bitcoinYearlySummaries.year, year),
          eq(bitcoinYearlySummaries.minerModel, minerModel)
        ));
      
      if (yearlySummary.length > 0) {
        console.log(`Bitcoin yearly summary for ${year} and ${minerModel}: ${yearlySummary[0].bitcoinMined} BTC`);
      } else {
        console.log(`Warning: No Bitcoin yearly summary found for ${year} and ${minerModel}`);
      }
    }
  } catch (error) {
    console.error('Error verifying Bitcoin pipeline:', error);
    throw error;
  }
}

// Execute the processing
processBitcoinCalculations()
  .then(() => {
    console.log('Bitcoin calculations completed successfully. Exiting...');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Processing failed with error:', error);
    process.exit(1);
  });