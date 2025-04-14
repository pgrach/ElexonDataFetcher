/**
 * Reprocess Bitcoin Calculations Post-Halving
 * 
 * This script deletes all Bitcoin calculations for dates on or after April 1, 2025,
 * then triggers a recalculation of those dates using the correct block reward value.
 * 
 * Run with: npx tsx reprocess-april.ts
 */

import { db } from "./db";
import { historicalBitcoinCalculations } from "./db/schema";
import { processSingleDay } from "./server/services/bitcoinService";
import { format, parseISO, addDays } from "date-fns";
import { gte, and, eq } from "drizzle-orm";

// Constants
const START_DATE = '2025-04-01';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const DRY_RUN = process.argv.includes('--dry-run');

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
    console.log(`Starting date: ${START_DATE}`);
    console.log(`Dry run: ${DRY_RUN ? 'Yes (no changes will be made)' : 'No (database will be updated)'}`);
    
    // Step 1: Get today's date
    const todayDate = format(new Date(), 'yyyy-MM-dd');
    console.log(`End date: ${todayDate}`);
    
    // Step 2: Get all dates to process
    const datesToProcess = getDateRange(START_DATE, todayDate);
    console.log(`\nWill process ${datesToProcess.length} days`);
    
    // Step 3: Delete all Bitcoin calculations for these dates
    for (const date of datesToProcess) {
      console.log(`\nProcessing date: ${date}`);
      
      // Count records for this date
      const recordCount = await db.select({ count: sql`count(*)` })
        .from(historicalBitcoinCalculations)
        .where(eq(historicalBitcoinCalculations.settlementDate, date));
      
      const count = Number(recordCount[0].count);
      
      if (count > 0) {
        console.log(`Found ${count} Bitcoin calculation records for ${date}`);
        
        if (!DRY_RUN) {
          // Delete records for this date
          const result = await db.delete(historicalBitcoinCalculations)
            .where(eq(historicalBitcoinCalculations.settlementDate, date));
          
          console.log(`Deleted Bitcoin calculations for ${date}`);
        } else {
          console.log(`[DRY RUN] Would delete ${count} records for ${date}`);
        }
      } else {
        console.log(`No existing Bitcoin calculations found for ${date}`);
      }
      
      // Step 4: Recalculate Bitcoin for each miner model
      if (!DRY_RUN) {
        for (const minerModel of MINER_MODELS) {
          console.log(`Recalculating Bitcoin for ${date} with model ${minerModel}...`);
          await processSingleDay(date, minerModel);
        }
      } else {
        console.log(`[DRY RUN] Would recalculate Bitcoin for ${date} with models: ${MINER_MODELS.join(', ')}`);
      }
    }
    
    console.log('\n=== Post-Halving Bitcoin Recalculation Complete ===');
  } catch (error) {
    console.error('Error during recalculation:', error);
  }
}

// Execute the script
reprocessPostHalvingCalculations();