/**
 * Process All Bitcoin Calculations from April-December 2024
 * 
 * This script processes data for April-December 2024 to update
 * the Bitcoin calculations using the post-halving reward rate.
 */

import { db } from "./db";
import { 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries, 
  bitcoinMonthlySummaries, 
  bitcoinYearlySummaries 
} from "./db/schema";
import { processSingleDay } from "./server/services/bitcoinService";
import { format, parseISO, addMonths } from "date-fns";
import { gte, and, lte, eq, sql } from "drizzle-orm";

// Constants
const HALVING_DATE = '2024-04-20'; // Bitcoin halving occurred on April 20, 2024
const END_DATE = '2024-12-31';     // End of 2024
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

/**
 * Get an array of unique dates from the historical calculations table
 */
async function getUniqueDates(startDate: string, endDate: string): Promise<string[]> {
  const datesResult = await db.select({
    date: historicalBitcoinCalculations.settlementDate
  })
  .from(historicalBitcoinCalculations)
  .where(
    and(
      gte(historicalBitcoinCalculations.settlementDate, startDate),
      lte(historicalBitcoinCalculations.settlementDate, endDate)
    )
  )
  .groupBy(historicalBitcoinCalculations.settlementDate)
  .orderBy(historicalBitcoinCalculations.settlementDate);

  return datesResult.map(r => r.date);
}

/**
 * Process a batch of dates
 */
async function processBatch(dates: string[]): Promise<void> {
  console.log(`Processing batch of ${dates.length} dates`);
  
  for (const date of dates) {
    // Skip dates before the halving
    if (date < HALVING_DATE) {
      console.log(`Skipping ${date} (before halving date)`);
      continue;
    }
    
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
      continue;
    }
    
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
    
    console.log(`Completed processing for ${date}`);
  }
}

/**
 * Process all dates month by month
 */
async function processAllMonths(): Promise<void> {
  try {
    console.log('\n=== 2024 Bitcoin Recalculation ===');
    console.log(`Start date: ${HALVING_DATE}`);
    console.log(`End date: ${END_DATE}`);
    
    // Process month by month
    let currentDate = parseISO(HALVING_DATE);
    const endDate = parseISO(END_DATE);
    
    while (currentDate <= endDate) {
      const monthStart = format(currentDate, 'yyyy-MM-01');
      const nextMonth = addMonths(currentDate, 1);
      const monthEnd = format(addMonths(currentDate, 1), 'yyyy-MM-01');
      
      console.log(`\n=== Processing Month: ${format(currentDate, 'yyyy-MM')} ===`);
      console.log(`Date range: ${monthStart} to ${monthEnd}`);
      
      // Get unique dates for this month
      const datesInMonth = await getUniqueDates(monthStart, monthEnd);
      console.log(`Found ${datesInMonth.length} days with data in ${format(currentDate, 'yyyy-MM')}`);
      
      if (datesInMonth.length > 0) {
        await processBatch(datesInMonth);
        console.log(`\nCompleted processing for ${format(currentDate, 'yyyy-MM')}`);
      } else {
        console.log(`No data for ${format(currentDate, 'yyyy-MM')}, skipping`);
      }
      
      // Move to next month
      currentDate = nextMonth;
    }
    
    console.log('\n=== 2024 Bitcoin Recalculation Complete ===');
  } catch (error) {
    console.error('Error during processing:', error);
  }
}

// Run the process
processAllMonths();
