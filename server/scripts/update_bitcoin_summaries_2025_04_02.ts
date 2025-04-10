/**
 * Update Bitcoin Summaries for 2025-04-02
 * 
 * This script updates all Bitcoin summary tables for 2025-04-02 based on 
 * existing historical_bitcoin_calculations data.
 */

import { db } from "@db";
import { historicalBitcoinCalculations, bitcoinDailySummaries, 
         bitcoinMonthlySummaries, bitcoinYearlySummaries } from "@db/schema";
import { eq, and, sql } from "drizzle-orm";
import { performance } from "perf_hooks";

// Target date to process
const TARGET_DATE = '2025-04-02';

// All miner models to process
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

/**
 * Update Bitcoin daily summaries for 2025-04-02
 */
async function updateDailySummaries(): Promise<void> {
  console.log(`\n==== Updating Bitcoin Daily Summaries ====\n`);
  
  for (const minerModel of MINER_MODELS) {
    try {
      // Check if we have historical calculations for this model
      const calcCount = await db.select({ count: sql<number>`COUNT(*)::int` })
        .from(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      if (!calcCount[0] || calcCount[0].count === 0) {
        console.log(`No historical Bitcoin calculations found for ${minerModel}`);
        continue;
      }
      
      console.log(`Found ${calcCount[0].count} historical Bitcoin calculations for ${minerModel}`);
      
      // Calculate total Bitcoin mined for the day
      const bitcoinTotal = await db.execute(sql`
        SELECT SUM(bitcoin_mined::NUMERIC) as total_bitcoin
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${TARGET_DATE}
        AND miner_model = ${minerModel}
      `);
      
      const totalBitcoin = bitcoinTotal.rows?.[0]?.total_bitcoin;
      
      if (!totalBitcoin) {
        console.log(`No Bitcoin total could be calculated for ${minerModel}`);
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
}

/**
 * Update Bitcoin monthly summaries for 2025-04
 */
async function updateMonthlySummaries(): Promise<void> {
  console.log(`\n==== Updating Bitcoin Monthly Summaries ====\n`);
  
  const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM
  
  // First, check if we have daily summaries for the month
  const dailySummariesCount = await db.select({ count: sql<number>`COUNT(*)::int` })
    .from(bitcoinDailySummaries)
    .where(sql`TO_CHAR(summary_date, 'YYYY-MM') = ${yearMonth}`);
  
  if (!dailySummariesCount[0] || dailySummariesCount[0].count === 0) {
    console.log(`No Bitcoin daily summaries found for ${yearMonth}`);
    return;
  }
  
  console.log(`Found ${dailySummariesCount[0].count} Bitcoin daily summaries for ${yearMonth}`);
  
  for (const minerModel of MINER_MODELS) {
    try {
      // Calculate monthly total from daily summaries
      const monthlyTotal = await db.execute(sql`
        SELECT SUM(bitcoin_mined::NUMERIC) as total_bitcoin
        FROM bitcoin_daily_summaries
        WHERE TO_CHAR(summary_date, 'YYYY-MM') = ${yearMonth}
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
}

/**
 * Update Bitcoin yearly summaries for 2025
 */
async function updateYearlySummaries(): Promise<void> {
  console.log(`\n==== Updating Bitcoin Yearly Summaries ====\n`);
  
  const year = TARGET_DATE.substring(0, 4); // YYYY
  
  // First, check if we have monthly summaries for the year
  const monthlySummariesCount = await db.select({ count: sql<number>`COUNT(*)::int` })
    .from(bitcoinMonthlySummaries)
    .where(sql`LEFT(year_month, 4) = ${year}`);
  
  if (!monthlySummariesCount[0] || monthlySummariesCount[0].count === 0) {
    console.log(`No Bitcoin monthly summaries found for ${year}`);
    return;
  }
  
  console.log(`Found ${monthlySummariesCount[0].count} Bitcoin monthly summaries for ${year}`);
  
  for (const minerModel of MINER_MODELS) {
    try {
      // Calculate yearly total from monthly summaries
      const yearlyTotal = await db.execute(sql`
        SELECT SUM(bitcoin_mined::NUMERIC) as total_bitcoin
        FROM bitcoin_monthly_summaries
        WHERE LEFT(year_month, 4) = ${year}
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
}

/**
 * Main function to run all updates
 */
async function main(): Promise<void> {
  const startTime = performance.now();
  
  try {
    console.log(`\n==== Starting Bitcoin Summaries Update for ${TARGET_DATE} ====\n`);
    
    // Step 1: Update daily summaries
    await updateDailySummaries();
    
    // Step 2: Update monthly summaries
    await updateMonthlySummaries();
    
    // Step 3: Update yearly summaries
    await updateYearlySummaries();
    
    // Final verification
    await verifyBitcoinSummaries();
    
    const endTime = performance.now();
    const durationSeconds = ((endTime - startTime) / 1000).toFixed(2);
    
    console.log(`\n==== Bitcoin Summaries Update Completed ====`);
    console.log(`Total execution time: ${durationSeconds} seconds`);
    
  } catch (error) {
    console.error(`Error during Bitcoin summaries update:`, error);
    throw error;
  }
}

/**
 * Verify the Bitcoin summaries have been properly updated
 */
async function verifyBitcoinSummaries(): Promise<void> {
  console.log(`\n==== Verifying Bitcoin Summaries ====\n`);
  
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
      console.log(`No Bitcoin daily summary found for ${minerModel}`);
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
      console.log(`No Bitcoin monthly summary found for ${yearMonth} and ${minerModel}`);
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
      console.log(`No Bitcoin yearly summary found for ${year} and ${minerModel}`);
    }
  }
}

// Execute the update
main()
  .then(() => {
    console.log('Bitcoin summaries update completed successfully.');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Bitcoin summaries update failed with error:', error);
    process.exit(1);
  });