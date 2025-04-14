/**
 * Reprocess All Bitcoin Calculations Post-Halving
 * 
 * This script deletes all Bitcoin calculations for dates on or after April 20, 2024
 * (Bitcoin halving date), then triggers a recalculation of those dates using
 * the correct block reward value of 3.125 BTC.
 * 
 * Run with: npx tsx reprocess-all-post-halving.ts
 * 
 * Options:
 * --dry-run: Show what would be updated without making changes
 * --limit-months=N: Process only the most recent N months (default: all)
 * --verify: Verify all 4 tables are updated for each date (slower)
 */

import { db } from "./db";
import { 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries, 
  bitcoinMonthlySummaries, 
  bitcoinYearlySummaries 
} from "./db/schema";
import { processSingleDay } from "./server/services/bitcoinService";
import { format, parseISO, addDays, subMonths } from "date-fns";
import { gte, and, eq, sql } from "drizzle-orm";

// Constants
const HALVING_DATE = '2024-04-20'; // Bitcoin halving occurred on April 20, 2024
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const DRY_RUN = process.argv.includes('--dry-run');
const VERIFY_UPDATES = process.argv.includes('--verify');

// Optional month limiting
const limitMonthsArg = process.argv.find(arg => arg.startsWith('--limit-months='));
const LIMIT_MONTHS = limitMonthsArg 
  ? parseInt(limitMonthsArg.split('=')[1], 10) 
  : 0; // 0 means no limit

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

async function reprocessPostHalvingCalculations() {
  try {
    console.log('\n=== Post-Halving Bitcoin Recalculation ===');
    console.log(`Halving date: ${HALVING_DATE}`);
    console.log(`Dry run: ${DRY_RUN ? 'Yes (no changes will be made)' : 'No (database will be updated)'}`);
    console.log(`Verify updates: ${VERIFY_UPDATES ? 'Yes (will verify all 4 tables)' : 'No'}`);
    
    // Step 1: Get today's date
    const todayDate = format(new Date(), 'yyyy-MM-dd');
    
    // Determine start date - either the halving date or a number of months ago
    let startDate = HALVING_DATE;
    if (LIMIT_MONTHS > 0) {
      const limitDate = format(subMonths(new Date(), LIMIT_MONTHS), 'yyyy-MM-dd');
      // Use the later of halving date or limit date
      startDate = parseISO(limitDate) > parseISO(HALVING_DATE) ? limitDate : HALVING_DATE;
      console.log(`Limiting processing to the last ${LIMIT_MONTHS} months (${startDate})`);
    }
    
    console.log(`Start date: ${startDate}`);
    console.log(`End date: ${todayDate}`);
    
    // Step 2: First check if there are any Bitcoin calculations in the date range
    // This avoids trying to process dates that don't have any data
    const existingDatesQuery = await db.select({ 
      settlementDate: historicalBitcoinCalculations.settlementDate,
      count: sql<number>`count(*)`.as('count')
    })
    .from(historicalBitcoinCalculations)
    .where(gte(historicalBitcoinCalculations.settlementDate, startDate))
    .groupBy(historicalBitcoinCalculations.settlementDate)
    .orderBy(historicalBitcoinCalculations.settlementDate);
    
    // Create a map of dates to record counts for quick lookup
    const dateMap = new Map<string, number>();
    let totalRecords = 0;
    
    existingDatesQuery.forEach(({ settlementDate, count }) => {
      dateMap.set(settlementDate, Number(count));
      totalRecords += Number(count);
    });
    
    console.log(`\nFound ${dateMap.size} days with data to process (${totalRecords} total records)`);
    
    if (dateMap.size === 0) {
      console.log('No post-halving Bitcoin calculation records found.');
      return;
    }
    
    // Confirm before proceeding with large operations
    if (totalRecords > 10000 && !DRY_RUN) {
      console.log(`\nWARNING: You are about to update more than 10,000 records (${totalRecords}).`);
      console.log('Consider using --dry-run first or --limit-months=N to limit the scope.');
      console.log('Waiting 5 seconds before proceeding... Press Ctrl+C to abort.');
      
      // Wait 5 seconds to give time to abort
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
    
    // Step 3: Process all dates with data
    let processedDays = 0;
    let processedRecords = 0;
    
    for (const [date, count] of dateMap.entries()) {
      console.log(`\nProcessing date: ${date} (${count} records)`);
      
      if (!DRY_RUN) {
        // Optional: Verify tables before updates if --verify flag is enabled
        let beforeCounts;
        if (VERIFY_UPDATES) {
          beforeCounts = await verifyTableUpdates(date);
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
        
        // Verify that all tables were updated if --verify flag is enabled
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
      } else {
        console.log(`[DRY RUN] Would delete ${count} records for ${date}`);
        console.log(`[DRY RUN] Would recalculate Bitcoin for ${date} with models: ${MINER_MODELS.join(', ')}`);
        
        if (VERIFY_UPDATES) {
          console.log(`[DRY RUN] Would verify updates to all 4 tables after processing`);
        }
      }
      
      processedDays++;
      processedRecords += count;
      
      // Log progress every 10 days
      if (processedDays % 10 === 0) {
        console.log(`\nProgress: ${processedDays}/${dateMap.size} days (${Math.round(processedDays/dateMap.size*100)}%)`);
        console.log(`Processed ${processedRecords}/${totalRecords} records (${Math.round(processedRecords/totalRecords*100)}%)`);
      }
    }
    
    console.log(`\n=== Post-Halving Bitcoin Recalculation Complete ===`);
    console.log(`Processed ${processedDays} days with ${processedRecords} total records`);
    
    if (DRY_RUN) {
      console.log(`\nThis was a dry run. To apply changes, run without the --dry-run flag.`);
      console.log(`You can limit processing to recent months with --limit-months=N (e.g., --limit-months=3)`);
    }
  } catch (error) {
    console.error('Error during recalculation:', error);
  }
}

// Execute the script
reprocessPostHalvingCalculations();