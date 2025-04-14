/**
 * Bitcoin Summaries Update Script
 * 
 * This script updates Bitcoin summaries (daily, monthly, yearly) based on
 * historical_bitcoin_calculations table data.
 * 
 * Run with: npx tsx scripts/update-bitcoin-summaries.ts 2025-04-11
 */

import { db } from '../db';
import { 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries, 
  bitcoinMonthlySummaries,
  bitcoinYearlySummaries,
  curtailmentRecords
} from '../db/schema';
import { eq, sql, and } from 'drizzle-orm';
import { format, parse } from 'date-fns';

// Get date from command line arguments or use default
const TARGET_DATE = process.argv[2] || '2025-04-11';
// Get miner models from the database
let MINER_MODEL_KEYS: string[] = []; 

/**
 * Create or update a Bitcoin daily summary for a specific date and miner model
 */
async function updateDailySummary(date: string, minerModel: string) {
  try {
    console.log(`\nUpdating daily summary for ${date} and ${minerModel}...`);
    
    // Calculate totals from historical calculations
    const totals = await db
      .select({
        bitcoinMined: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined})`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, date),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );
    
    if (!totals[0] || !totals[0].bitcoinMined) {
      console.log(`No Bitcoin data found for ${date} and ${minerModel}`);
      return;
    }
    
    const bitcoinMined = Number(totals[0].bitcoinMined);
    const bitcoinPrice = 65000; // Using a default value for simplicity
    
    // Check if summary already exists
    const existingSummary = await db
      .select()
      .from(bitcoinDailySummaries)
      .where(
        and(
          eq(bitcoinDailySummaries.summaryDate, date),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        )
      );
    
    if (existingSummary.length > 0) {
      // Update existing summary
      await db
        .update(bitcoinDailySummaries)
        .set({
          bitcoinMined: bitcoinMined.toString(),
          updatedAt: new Date()
        })
        .where(
          and(
            eq(bitcoinDailySummaries.summaryDate, date),
            eq(bitcoinDailySummaries.minerModel, minerModel)
          )
        );
      
      console.log(`Updated daily summary: ${bitcoinMined.toFixed(8)} BTC`);
    } else {
      // Create new summary
      await db
        .insert(bitcoinDailySummaries)
        .values({
          summaryDate: date,
          minerModel: minerModel,
          bitcoinMined: bitcoinMined.toString()
        });
      
      console.log(`Created new daily summary: ${bitcoinMined.toFixed(8)} BTC`);
    }
    
  } catch (error) {
    console.error(`Error updating daily summary for ${date} and ${minerModel}:`, error);
  }
}

/**
 * Update monthly summary based on daily summaries
 */
async function updateMonthlySummary(date: string, minerModel: string) {
  try {
    const parsedDate = parse(date, 'yyyy-MM-dd', new Date());
    const yearMonth = format(parsedDate, 'yyyy-MM');
    
    console.log(`\nUpdating monthly summary for ${yearMonth} and ${minerModel}...`);
    
    // Calculate from daily summaries
    const totals = await db
      .select({
        bitcoinMined: sql<string>`SUM(${bitcoinDailySummaries.bitcoinMined})`
      })
      .from(bitcoinDailySummaries)
      .where(
        and(
          sql`TO_CHAR(${bitcoinDailySummaries.summaryDate}, 'YYYY-MM') = ${yearMonth}`,
          eq(bitcoinDailySummaries.minerModel, minerModel)
        )
      );
    
    if (!totals[0] || !totals[0].bitcoinMined) {
      console.log(`No daily summaries found for ${yearMonth} and ${minerModel}`);
      return;
    }
    
    const bitcoinMined = Number(totals[0].bitcoinMined);
    
    // Check if summary already exists
    const existingSummary = await db
      .select()
      .from(bitcoinMonthlySummaries)
      .where(
        and(
          eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        )
      );
    
    if (existingSummary.length > 0) {
      // Update existing summary
      await db
        .update(bitcoinMonthlySummaries)
        .set({
          bitcoinMined: bitcoinMined.toString(),
          updatedAt: new Date()
        })
        .where(
          and(
            eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
            eq(bitcoinMonthlySummaries.minerModel, minerModel)
          )
        );
      
      console.log(`Updated monthly summary: ${bitcoinMined.toFixed(8)} BTC`);
    } else {
      // Create new summary
      await db
        .insert(bitcoinMonthlySummaries)
        .values({
          yearMonth: yearMonth,
          minerModel: minerModel,
          bitcoinMined: bitcoinMined.toString()
        });
      
      console.log(`Created new monthly summary: ${bitcoinMined.toFixed(8)} BTC`);
    }
    
  } catch (error) {
    console.error(`Error updating monthly summary for ${date} and ${minerModel}:`, error);
  }
}

/**
 * Update yearly summary based on monthly summaries
 */
async function updateYearlySummary(date: string, minerModel: string) {
  try {
    const year = date.substring(0, 4);
    
    console.log(`\nUpdating yearly summary for ${year} and ${minerModel}...`);
    
    // Calculate from monthly summaries
    const totals = await db
      .select({
        bitcoinMined: sql<string>`SUM(${bitcoinMonthlySummaries.bitcoinMined})`
      })
      .from(bitcoinMonthlySummaries)
      .where(
        and(
          sql`SUBSTRING(${bitcoinMonthlySummaries.yearMonth}, 1, 4) = ${year}`,
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        )
      );
    
    if (!totals[0] || !totals[0].bitcoinMined) {
      console.log(`No monthly summaries found for ${year} and ${minerModel}`);
      return;
    }
    
    const bitcoinMined = Number(totals[0].bitcoinMined);
    
    // Check if summary already exists
    const existingSummary = await db
      .select()
      .from(bitcoinYearlySummaries)
      .where(
        and(
          eq(bitcoinYearlySummaries.year, year),
          eq(bitcoinYearlySummaries.minerModel, minerModel)
        )
      );
    
    if (existingSummary.length > 0) {
      // Update existing summary
      await db
        .update(bitcoinYearlySummaries)
        .set({
          bitcoinMined: bitcoinMined.toString(),
          updatedAt: new Date()
        })
        .where(
          and(
            eq(bitcoinYearlySummaries.year, year),
            eq(bitcoinYearlySummaries.minerModel, minerModel)
          )
        );
      
      console.log(`Updated yearly summary: ${bitcoinMined.toFixed(8)} BTC`);
    } else {
      // Create new summary
      await db
        .insert(bitcoinYearlySummaries)
        .values({
          year: year,
          minerModel: minerModel,
          bitcoinMined: bitcoinMined.toString()
        });
      
      console.log(`Created new yearly summary: ${bitcoinMined.toFixed(8)} BTC`);
    }
    
  } catch (error) {
    console.error(`Error updating yearly summary for ${date} and ${minerModel}:`, error);
  }
}

/**
 * Get the list of miner models from the database
 */
async function getMinerModels() {
  try {
    // Get distinct miner models from historical Bitcoin calculations
    const results = await db
      .select({
        minerModel: historicalBitcoinCalculations.minerModel
      })
      .from(historicalBitcoinCalculations)
      .groupBy(historicalBitcoinCalculations.minerModel);
    
    return results.map(result => result.minerModel);
  } catch (error) {
    console.error('Error fetching miner models:', error);
    return ['S19J_PRO', 'S9', 'M20S']; // Fallback to default models
  }
}

/**
 * Run the update process
 */
async function runUpdate() {
  console.log(`=== Bitcoin Summaries Update for ${TARGET_DATE} ===`);
  
  try {
    // Get miner models from the database
    MINER_MODEL_KEYS = await getMinerModels();
    console.log(`Found miner models: ${MINER_MODEL_KEYS.join(', ')}`);
    
    // Update daily summaries
    for (const minerModel of MINER_MODEL_KEYS) {
      await updateDailySummary(TARGET_DATE, minerModel);
    }
    
    // Update monthly summary
    for (const minerModel of MINER_MODEL_KEYS) {
      await updateMonthlySummary(TARGET_DATE, minerModel);
    }
    
    // Update yearly summary
    for (const minerModel of MINER_MODEL_KEYS) {
      await updateYearlySummary(TARGET_DATE, minerModel);
    }
    
    console.log(`\n=== Update Complete ===`);
    
  } catch (error) {
    console.error('Error during update:', error);
    process.exit(1);
  }
}

// Run the update
runUpdate();