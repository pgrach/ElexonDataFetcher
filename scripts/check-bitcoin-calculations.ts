/**
 * Bitcoin Calculations Checker Script
 * 
 * This script checks the full flow of Bitcoin calculations to ensure data is properly
 * flowing from curtailment records to historical_bitcoin_calculations and then to the
 * summary tables (daily, monthly, yearly).
 * 
 * Run with: npx tsx scripts/check-bitcoin-calculations.ts
 */

import { db } from '../db';
import { 
  curtailmentRecords, 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries,
  bitcoinMonthlySummaries,
  bitcoinYearlySummaries
} from '../db/schema';
import { sql, eq, desc, and, between } from 'drizzle-orm';
import { minerModels } from '../server/types/bitcoin';
import { format, parse, subDays } from 'date-fns';

// Target dates for verification
const TODAY = format(new Date(), 'yyyy-MM-dd');
const YESTERDAY = format(subDays(new Date(), 1), 'yyyy-MM-dd');
const SPECIFIC_DATE = '2025-04-11'; // April 11, 2025 that we just processed

// Miner models to check
const MINER_MODEL_KEYS = Object.keys(minerModels);

/**
 * Check historical Bitcoin calculations for a specific date
 */
async function checkHistoricalCalculations(date: string) {
  try {
    console.log(`\n=== Historical Bitcoin Calculations for ${date} ===`);
    
    // Check calculations by miner model
    for (const minerModel of MINER_MODEL_KEYS) {
      const stats = await db
        .select({
          recordCount: sql<number>`COUNT(*)`,
          periodCount: sql<number>`COUNT(DISTINCT ${historicalBitcoinCalculations.settlementPeriod})`,
          farmCount: sql<number>`COUNT(DISTINCT ${historicalBitcoinCalculations.farmId})`,
          totalBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`,
          averageDifficulty: sql<string>`AVG(${historicalBitcoinCalculations.difficulty}::numeric)`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, date),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
      
      console.log(`\n${minerModel}:`);
      console.log(`Records: ${stats[0]?.recordCount || 0}`);
      console.log(`Settlement Periods: ${stats[0]?.periodCount || 0}`);
      console.log(`Farms: ${stats[0]?.farmCount || 0}`);
      console.log(`Total Bitcoin: ${Number(stats[0]?.totalBitcoin || 0).toFixed(8)} BTC`);
      
      if (stats[0]?.recordCount > 0) {
        console.log(`Average Difficulty: ${Number(stats[0]?.averageDifficulty).toExponential(2)}`);
        
        // Check if there are farms with significant Bitcoin
        const topFarms = await db
          .select({
            farmId: historicalBitcoinCalculations.farmId,
            totalBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
          })
          .from(historicalBitcoinCalculations)
          .where(
            and(
              eq(historicalBitcoinCalculations.settlementDate, date),
              eq(historicalBitcoinCalculations.minerModel, minerModel)
            )
          )
          .groupBy(historicalBitcoinCalculations.farmId)
          .orderBy(sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)` as any, 'desc')
          .limit(3);
        
        if (topFarms.length > 0) {
          console.log(`\nTop Farms for ${minerModel}:`);
          for (const farm of topFarms) {
            console.log(`${farm.farmId}: ${Number(farm.totalBitcoin).toFixed(8)} BTC`);
          }
        }
      }
    }
    
  } catch (error) {
    console.error(`Error checking historical calculations:`, error);
  }
}

/**
 * Check Bitcoin daily summaries
 */
async function checkDailySummaries(date: string) {
  try {
    console.log(`\n=== Bitcoin Daily Summaries for ${date} ===`);
    
    const summaries = await db
      .select()
      .from(bitcoinDailySummaries)
      .where(eq(bitcoinDailySummaries.summaryDate, date));
    
    if (summaries.length === 0) {
      console.log(`No daily summaries found for ${date}`);
    } else {
      for (const summary of summaries) {
        console.log(`\n${summary.minerModel}:`);
        console.log(`Bitcoin Mined: ${Number(summary.bitcoinMined).toFixed(8)} BTC`);
        console.log(`Current GBP Value: £${Number(summary.valueAtCurrentPrice).toFixed(2)}`);
        console.log(`Difficulty: ${Number(summary.difficulty).toExponential(2)}`);
      }
    }
    
  } catch (error) {
    console.error(`Error checking daily summaries:`, error);
  }
}

/**
 * Check Bitcoin monthly summaries for the month containing the specified date
 */
async function checkMonthlySummaries(date: string) {
  try {
    const parsedDate = parse(date, 'yyyy-MM-dd', new Date());
    const yearMonth = format(parsedDate, 'yyyy-MM');
    
    console.log(`\n=== Bitcoin Monthly Summaries for ${yearMonth} ===`);
    
    const summaries = await db
      .select()
      .from(bitcoinMonthlySummaries)
      .where(eq(bitcoinMonthlySummaries.yearMonth, yearMonth));
    
    if (summaries.length === 0) {
      console.log(`No monthly summaries found for ${yearMonth}`);
    } else {
      for (const summary of summaries) {
        console.log(`\n${summary.minerModel}:`);
        console.log(`Bitcoin Mined: ${Number(summary.bitcoinMined).toFixed(8)} BTC`);
        console.log(`Current GBP Value: £${Number(summary.valueAtCurrentPrice).toFixed(2)}`);
        console.log(`Difficulty: ${Number(summary.difficulty).toExponential(2)}`);
      }
    }
    
  } catch (error) {
    console.error(`Error checking monthly summaries:`, error);
  }
}

/**
 * Check Bitcoin yearly summaries for the year containing the specified date
 */
async function checkYearlySummaries(date: string) {
  try {
    const year = date.substring(0, 4);
    
    console.log(`\n=== Bitcoin Yearly Summaries for ${year} ===`);
    
    const summaries = await db
      .select()
      .from(bitcoinYearlySummaries)
      .where(eq(bitcoinYearlySummaries.year, year));
    
    if (summaries.length === 0) {
      console.log(`No yearly summaries found for ${year}`);
    } else {
      for (const summary of summaries) {
        console.log(`\n${summary.minerModel}:`);
        console.log(`Bitcoin Mined: ${Number(summary.bitcoinMined).toFixed(8)} BTC`);
        console.log(`Current GBP Value: £${Number(summary.valueAtCurrentPrice).toFixed(2)}`);
        console.log(`Difficulty: ${Number(summary.difficulty).toExponential(2)}`);
      }
    }
    
  } catch (error) {
    console.error(`Error checking yearly summaries:`, error);
  }
}

/**
 * Run the verification on specific dates
 */
async function runVerification() {
  console.log(`\n=== Bitcoin Calculation Flow Verification ===`);
  
  // Check specific date (April 11, 2025)
  await checkHistoricalCalculations(SPECIFIC_DATE);
  await checkDailySummaries(SPECIFIC_DATE);
  await checkMonthlySummaries(SPECIFIC_DATE);
  await checkYearlySummaries(SPECIFIC_DATE);
  
  // Check yesterday's data
  console.log(`\n\n----- Yesterday's Data (${YESTERDAY}) -----`);
  await checkHistoricalCalculations(YESTERDAY);
  await checkDailySummaries(YESTERDAY);
  
  // Check today's data
  console.log(`\n\n----- Today's Data (${TODAY}) -----`);
  await checkHistoricalCalculations(TODAY);
  await checkDailySummaries(TODAY);
  
  console.log(`\n=== Verification Complete ===`);
}

// Run the verification
runVerification();