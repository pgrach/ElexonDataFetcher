/**
 * Reprocess April 2024 Bitcoin Calculations
 * 
 * This script specifically targets April 2024, which is a special case
 * because the halving occurred on April 20, 2024. It ensures that:
 * 
 * 1. Dates before April 20 use the pre-halving reward (6.25 BTC)
 * 2. Dates on or after April 20 use the post-halving reward (3.125 BTC)
 */

import { db } from "./db";
import { 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries, 
  bitcoinMonthlySummaries, 
  bitcoinYearlySummaries 
} from "./db/schema";
import { processSingleDay } from "./server/services/bitcoinService";
import { format, parseISO } from "date-fns";
import { gte, and, lte, eq, sql } from "drizzle-orm";

// Constants
const HALVING_DATE = '2024-04-20'; // Bitcoin halving occurred on April 20, 2024
const APRIL_START = '2024-04-01';
const APRIL_END = '2024-04-30';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

/**
 * Get an array of unique dates in April 2024 that have Bitcoin calculations
 */
async function getAprilDates(): Promise<string[]> {
  const datesResult = await db.select({
    date: historicalBitcoinCalculations.settlementDate
  })
  .from(historicalBitcoinCalculations)
  .where(
    and(
      gte(historicalBitcoinCalculations.settlementDate, APRIL_START),
      lte(historicalBitcoinCalculations.settlementDate, APRIL_END)
    )
  )
  .groupBy(historicalBitcoinCalculations.settlementDate)
  .orderBy(historicalBitcoinCalculations.settlementDate);

  return datesResult.map(r => r.date);
}

/**
 * Process a specific date
 */
async function processDate(date: string): Promise<void> {
  console.log(`\nProcessing date: ${date}`);
  
  // Get count of records for this date
  const countResult = await db.select({
    count: sql<number>`count(*)`.as('count')
  })
  .from(historicalBitcoinCalculations)
  .where(eq(historicalBitcoinCalculations.settlementDate, date));
  
  const recordCount = Number(countResult[0]?.count || 0);
  console.log(`Found ${recordCount} records for ${date}`);
  
  if (recordCount === 0) {
    console.log(`No records to process for ${date}`);
    return;
  }
  
  // Special consideration for April 2024 dates
  const isPostHalving = date >= HALVING_DATE;
  console.log(`Date is ${isPostHalving ? 'after' : 'before'} halving date (will use ${isPostHalving ? '3.125' : '6.25'} BTC reward)`);
  
  if (isPostHalving) {
    // For post-halving dates, we need to delete and recalculate
    // Delete records for this date
    await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, date));
    
    console.log(`Deleted ${recordCount} Bitcoin calculations for ${date}`);
    
    // Recalculate Bitcoin for each miner model
    for (const minerModel of MINER_MODELS) {
      console.log(`Recalculating Bitcoin for ${date} with model ${minerModel}...`);
      try {
        await processSingleDay(date, minerModel);
      } catch (error) {
        console.error(`Error processing ${date} with ${minerModel}:`, error);
      }
    }
  } else {
    // For pre-halving dates, we leave them as is
    console.log(`Keeping existing calculations for ${date} (pre-halving)`);
  }
}

/**
 * Update April 2024 monthly summary based on recalculated daily data
 */
async function updateAprilSummary(): Promise<void> {
  console.log('\nUpdating April 2024 monthly summary');
  
  for (const minerModel of MINER_MODELS) {
    console.log(`Updating monthly summary for April 2024 and ${minerModel}...`);
    
    try {
      // Delete existing monthly summary
      await db.delete(bitcoinMonthlySummaries)
        .where(
          and(
            eq(bitcoinMonthlySummaries.yearMonth, '2024-04'),
            eq(bitcoinMonthlySummaries.minerModel, minerModel)
          )
        );
      
      // Calculate new monthly total based on daily summaries
      const dailySummaries = await db.select({
        sum: sql<number>`sum(bitcoin_mined)`.as('sum')
      })
      .from(bitcoinDailySummaries)
      .where(
        and(
          sql`summary_date LIKE '2024-04-%'`,
          eq(bitcoinDailySummaries.minerModel, minerModel)
        )
      );
      
      const monthlyTotal = Number(dailySummaries[0]?.sum || 0);
      
      // Insert new monthly summary
      await db.insert(bitcoinMonthlySummaries).values({
        yearMonth: '2024-04',
        minerModel: minerModel,
        bitcoinMined: monthlyTotal,
        createdAt: new Date(),
        updatedAt: new Date()
      });
      
      console.log(`Updated monthly summary for April 2024 and ${minerModel}: ${monthlyTotal.toFixed(8)} BTC`);
    } catch (error) {
      console.error(`Error updating monthly summary for April 2024 and ${minerModel}:`, error);
    }
  }
  
  // Now update the 2024 yearly summary
  console.log('\nUpdating 2024 yearly summary');
  
  for (const minerModel of MINER_MODELS) {
    console.log(`Updating yearly summary for 2024 and ${minerModel}...`);
    
    try {
      // Get sum of all monthly data for 2024
      const yearlyTotal = await db.select({
        sum: sql<number>`sum(bitcoin_mined)`.as('sum')
      })
      .from(bitcoinMonthlySummaries)
      .where(
        and(
          sql`year_month LIKE '2024-%'`,
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        )
      );
      
      const total = Number(yearlyTotal[0]?.sum || 0);
      
      // Delete existing yearly summary
      await db.delete(bitcoinYearlySummaries)
        .where(
          and(
            eq(bitcoinYearlySummaries.year, '2024'),
            eq(bitcoinYearlySummaries.minerModel, minerModel)
          )
        );
      
      // Insert new yearly summary
      await db.insert(bitcoinYearlySummaries).values({
        year: '2024',
        minerModel: minerModel,
        bitcoinMined: total,
        createdAt: new Date(),
        updatedAt: new Date()
      });
      
      console.log(`Updated yearly summary for 2024 and ${minerModel}: ${total.toFixed(8)} BTC`);
    } catch (error) {
      console.error(`Error updating yearly summary for 2024 and ${minerModel}:`, error);
    }
  }
}

/**
 * Process all April 2024 dates
 */
async function processApril2024(): Promise<void> {
  try {
    console.log('\n=== April 2024 Bitcoin Recalculation ===');
    console.log(`Halving date: ${HALVING_DATE}`);
    console.log(`Processing date range: ${APRIL_START} to ${APRIL_END}`);
    
    // Get all April dates
    const aprilDates = await getAprilDates();
    console.log(`Found ${aprilDates.length} days with data in April 2024`);
    
    // Process each date
    for (const date of aprilDates) {
      await processDate(date);
    }
    
    // Update monthly and yearly summaries
    await updateAprilSummary();
    
    console.log('\n=== April 2024 Bitcoin Recalculation Complete ===');
  } catch (error) {
    console.error('Error during processing:', error);
  }
}

// Run the process
processApril2024();