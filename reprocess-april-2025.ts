/**
 * Reprocess Bitcoin Calculations for April 2025 Only
 * 
 * This script deletes and recalculates all Bitcoin calculations for April 2025
 * with verification of all 4 tables.
 * 
 * Run with: npx tsx reprocess-april-2025.ts
 */

import { db } from "./db";
import { 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries, 
  bitcoinMonthlySummaries, 
  bitcoinYearlySummaries 
} from "./db/schema";
import { processSingleDay } from "./server/services/bitcoinService";
import { format, parseISO, addDays } from "date-fns";
import { gte, and, eq, lte, sql } from "drizzle-orm";

// Constants
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const VERIFY_UPDATES = true;

/**
 * Verify that all four Bitcoin tables were updated
 * @param date - The settlement date
 * @returns Object with counts of records in each table
 */
async function verifyTableUpdates(date: string): Promise<{
  historical: number,
  daily: number,
  monthly: number,
  yearly: number
}> {
  const yearMonth = date.substring(0, 7); // YYYY-MM format
  const year = date.substring(0, 4);      // YYYY format
  
  // Check historicalBitcoinCalculations table
  const historicalCount = await db.select({
    count: sql<number>`count(*)`.as('count')
  })
  .from(historicalBitcoinCalculations)
  .where(eq(historicalBitcoinCalculations.settlementDate, date));
  
  // Check bitcoinDailySummaries table
  const dailyCount = await db.select({
    count: sql<number>`count(*)`.as('count')
  })
  .from(bitcoinDailySummaries)
  .where(eq(bitcoinDailySummaries.summaryDate, date));
  
  // Check bitcoinMonthlySummaries table
  const monthlyCount = await db.select({
    count: sql<number>`count(*)`.as('count')
  })
  .from(bitcoinMonthlySummaries)
  .where(eq(bitcoinMonthlySummaries.yearMonth, yearMonth));
  
  // Check bitcoinYearlySummaries table
  const yearlyCount = await db.select({
    count: sql<number>`count(*)`.as('count')
  })
  .from(bitcoinYearlySummaries)
  .where(eq(bitcoinYearlySummaries.year, year));
  
  return {
    historical: Number(historicalCount[0]?.count || 0),
    daily: Number(dailyCount[0]?.count || 0),
    monthly: Number(monthlyCount[0]?.count || 0),
    yearly: Number(yearlyCount[0]?.count || 0)
  };
}

/**
 * Get an array of date strings (YYYY-MM-DD) between start and end date, inclusive
 */
function getDateRange(startDate: string, endDate: string): string[] {
  const start = parseISO(startDate);
  const end = parseISO(endDate);
  const dates: string[] = [];
  
  let currentDate = start;
  while (currentDate <= end) {
    dates.push(format(currentDate, 'yyyy-MM-dd'));
    currentDate = addDays(currentDate, 1);
  }
  
  return dates;
}

async function reprocessApril2025() {
  try {
    console.log('\n=== April 2025 Bitcoin Recalculation ===');
    
    // Today's date in case we need it
    const todayDate = format(new Date(), 'yyyy-MM-dd');
    
    // April 2025 date range
    const startDate = '2025-04-01';
    const endDate = todayDate > '2025-04-30' ? '2025-04-30' : todayDate;
    
    console.log(`Start date: ${startDate}`);
    console.log(`End date: ${endDate}`);
    
    // Get all dates in April (up to today if we're still in April)
    const dates = getDateRange(startDate, endDate);
    console.log(`Processing ${dates.length} days in April 2025`);
    
    // Step 1: Find days with data
    // First check if there are any Bitcoin calculations in the date range
    const existingDatesQuery = await db.select({ 
      settlementDate: historicalBitcoinCalculations.settlementDate,
      count: sql<number>`count(*)`.as('count')
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
    
    // Create a map of dates to record counts for quick lookup
    const dateMap = new Map<string, number>();
    let totalRecords = 0;
    
    existingDatesQuery.forEach(({ settlementDate, count }) => {
      dateMap.set(settlementDate, Number(count));
      totalRecords += Number(count);
    });
    
    console.log(`\nFound ${dateMap.size} days in April with data to process (${totalRecords} total records)`);
    
    if (dateMap.size === 0) {
      console.log('No Bitcoin calculation records found for April 2025.');
      return;
    }
    
    // Process each date with data
    let processedDays = 0;
    let processedRecords = 0;
    
    // Loop through all dates in April
    for (const date of dates) {
      const count = dateMap.get(date) || 0;
      
      // Skip dates with no data
      if (count === 0) {
        console.log(`\nSkipping date: ${date} (no existing data)`);
        continue;
      }
      
      console.log(`\nProcessing date: ${date} (${count} records)`);
      
      // Optional: Verify tables before updates
      if (VERIFY_UPDATES) {
        const beforeCounts = await verifyTableUpdates(date);
        console.log(`Before update table counts for ${date}:`);
        console.log(`- Historical: ${beforeCounts.historical} records`);
        console.log(`- Daily: ${beforeCounts.daily} records`);
        console.log(`- Monthly: ${beforeCounts.monthly} records (for ${date.substring(0, 7)})`);
        console.log(`- Yearly: ${beforeCounts.yearly} records (for ${date.substring(0, 4)})`);
      }
      
      // Delete records for this date
      const result = await db.delete(historicalBitcoinCalculations)
        .where(eq(historicalBitcoinCalculations.settlementDate, date));
      
      console.log(`Deleted ${count} Bitcoin calculations for ${date}`);
      
      // Recalculate Bitcoin for each miner model
      for (const minerModel of MINER_MODELS) {
        console.log(`Recalculating Bitcoin for ${date} with model ${minerModel}...`);
        try {
          await processSingleDay(date, minerModel);
        } catch (error) {
          console.error(`Error processing ${date} with ${minerModel}:`, error);
        }
      }
      
      // Verify that all tables were updated
      if (VERIFY_UPDATES) {
        // Wait a short time to ensure all DB operations complete
        await new Promise(resolve => setTimeout(resolve, 1000));
        
        const afterCounts = await verifyTableUpdates(date);
        console.log(`\nAfter update table counts for ${date}:`);
        console.log(`- Historical: ${afterCounts.historical} records ${afterCounts.historical > 0 ? '✓' : '❌'}`);
        console.log(`- Daily: ${afterCounts.daily} records ${afterCounts.daily > 0 ? '✓' : '❌'}`);
        console.log(`- Monthly: ${afterCounts.monthly} records ${afterCounts.monthly > 0 ? '✓' : '❌'} (for ${date.substring(0, 7)})`);
        console.log(`- Yearly: ${afterCounts.yearly} records ${afterCounts.yearly > 0 ? '✓' : '❌'} (for ${date.substring(0, 4)})`);
        
        // Alert if any tables didn't get updated
        if (afterCounts.historical === 0 || afterCounts.daily === 0 || 
            afterCounts.monthly === 0 || afterCounts.yearly === 0) {
          console.log(`\n⚠️ WARNING: Some tables may not have updated properly for ${date}`);
        } else {
          console.log(`\n✅ All four tables verified as updated for ${date}`);
        }
      }
      
      processedDays++;
      processedRecords += count;
    }
    
    console.log(`\n=== April 2025 Bitcoin Recalculation Complete ===`);
    console.log(`Processed ${processedDays} days with ${processedRecords} total records`);
    
  } catch (error) {
    console.error('Error during recalculation:', error);
  }
}

// Execute the script
reprocessApril2025();
