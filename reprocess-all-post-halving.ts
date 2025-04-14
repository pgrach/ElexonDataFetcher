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
 */

import { db } from "./db";
import { historicalBitcoinCalculations } from "./db/schema";
import { processSingleDay } from "./server/services/bitcoinService";
import { format, parseISO, addDays, subMonths } from "date-fns";
import { gte, and, eq, sql } from "drizzle-orm";

// Constants
const HALVING_DATE = '2024-04-20'; // Bitcoin halving occurred on April 20, 2024
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const DRY_RUN = process.argv.includes('--dry-run');

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

async function reprocessPostHalvingCalculations() {
  try {
    console.log('\n=== Post-Halving Bitcoin Recalculation ===');
    console.log(`Halving date: ${HALVING_DATE}`);
    console.log(`Dry run: ${DRY_RUN ? 'Yes (no changes will be made)' : 'No (database will be updated)'}`);
    
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
      } else {
        console.log(`[DRY RUN] Would delete ${count} records for ${date}`);
        console.log(`[DRY RUN] Would recalculate Bitcoin for ${date} with models: ${MINER_MODELS.join(', ')}`);
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