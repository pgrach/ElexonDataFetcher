/**
 * Fix Daily Bitcoin Summaries
 * 
 * This script recalculates missing daily Bitcoin summaries by:
 * 1. Finding dates that have historical records but no daily summary
 * 2. Recalculating the daily summary for each date and miner model
 * 
 * Run with: npx tsx fix-daily-summaries.ts
 */

import { db } from "./db";
import { 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries 
} from "./db/schema";
import { format } from "date-fns";
import { eq, isNull, and, sql, desc } from "drizzle-orm";

// Constants
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

/**
 * Find dates that have historical records but no daily summary
 */
async function findMissingDailySummaries(limit: number = 30): Promise<string[]> {
  // Get the most recent dates with historical records
  const datesWithHistorical = await db.select({
    settlementDate: historicalBitcoinCalculations.settlementDate,
  })
  .from(historicalBitcoinCalculations)
  .groupBy(historicalBitcoinCalculations.settlementDate)
  .orderBy(desc(historicalBitcoinCalculations.settlementDate))
  .limit(limit);
  
  const dates = datesWithHistorical.map(r => r.settlementDate);
  const missingDates: string[] = [];
  
  // Check each date to see if it has a daily summary for all three miner models
  for (const date of dates) {
    const dailySummaries = await db.select({
      count: sql<number>`count(*)`.as('count')
    })
    .from(bitcoinDailySummaries)
    .where(eq(bitcoinDailySummaries.summaryDate, date));
    
    const summaryCount = Number(dailySummaries[0]?.count || 0);
    
    // If we don't have 3 summaries (one for each miner model), mark as missing
    if (summaryCount < 3) {
      missingDates.push(date);
    }
  }
  
  return missingDates;
}

/**
 * Recalculate a daily summary for a specific date and miner model
 */
async function recalculateDailySummary(date: string, minerModel: string): Promise<void> {
  try {
    // Get all historical calculations for this date and miner model
    const calculations = await db.select({
      bitcoinMined: historicalBitcoinCalculations.bitcoinMined,
    })
    .from(historicalBitcoinCalculations)
    .where(
      and(
        eq(historicalBitcoinCalculations.settlementDate, date),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      )
    );
    
    // If we have calculations, create a summary
    if (calculations.length > 0) {
      // Sum the Bitcoin mined
      const totalBitcoin = calculations.reduce(
        (sum, calc) => sum + Number(calc.bitcoinMined), 
        0
      );
      
      // Delete any existing summary for this date and model
      await db.delete(bitcoinDailySummaries)
        .where(
          and(
            eq(bitcoinDailySummaries.summaryDate, date),
            eq(bitcoinDailySummaries.minerModel, minerModel)
          )
        );
      
      // Create a new summary
      await db.insert(bitcoinDailySummaries).values({
        summaryDate: date,
        minerModel: minerModel,
        bitcoinMined: totalBitcoin,
        createdAt: new Date(),
        updatedAt: new Date()
      });
      
      console.log(`Created daily summary for ${date} and ${minerModel}: ${totalBitcoin.toFixed(8)} BTC`);
    } else {
      console.log(`No historical calculations found for ${date} and ${minerModel}`);
    }
  } catch (error) {
    console.error(`Error recalculating daily summary for ${date} and ${minerModel}:`, error);
  }
}

async function fixDailySummaries() {
  try {
    console.log('\n=== Daily Bitcoin Summaries Fix ===');
    
    // Find dates with missing summaries (last 30 days)
    const missingDates = await findMissingDailySummaries();
    
    if (missingDates.length === 0) {
      console.log('No missing daily summaries found for the last 30 days.');
      return;
    }
    
    console.log(`Found ${missingDates.length} dates with missing daily summaries:`);
    console.log(missingDates.join(', '));
    
    // Process each missing date
    for (const date of missingDates) {
      console.log(`\nProcessing date: ${date}`);
      
      // Recalculate for each miner model
      for (const minerModel of MINER_MODELS) {
        await recalculateDailySummary(date, minerModel);
      }
      
      // Verify the fix
      const dailySummaries = await db.select({
        count: sql<number>`count(*)`.as('count')
      })
      .from(bitcoinDailySummaries)
      .where(eq(bitcoinDailySummaries.summaryDate, date));
      
      const summaryCount = Number(dailySummaries[0]?.count || 0);
      console.log(`${date} now has ${summaryCount}/3 daily summaries ${summaryCount === 3 ? '✓' : '❌'}`);
    }
    
    console.log('\n=== Daily Bitcoin Summaries Fix Complete ===');
  } catch (error) {
    console.error('Error fixing daily summaries:', error);
  }
}

// Execute the fix
fixDailySummaries();