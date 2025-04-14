/**
 * Bitcoin Halving Recalculation Script
 * 
 * This script recalculates Bitcoin mining potential for all records after
 * April 20, 2024 (Bitcoin halving date) to ensure they use the new 3.125 BTC reward
 * instead of the previous 6.25 BTC reward.
 * 
 * Run with: npx tsx recalculate_post_halving.ts
 */

import { db } from "./db";
import { historicalBitcoinCalculations, bitcoinDailySummaries, bitcoinMonthlySummaries } from "./db/schema";
import { calculateBitcoin } from "./server/utils/bitcoin";
import { format, parseISO } from "date-fns";
import { gt, and, eq, sql } from "drizzle-orm";
import { calculateMonthlyBitcoinSummary, manualUpdateYearlyBitcoinSummary } from "./server/services/bitcoinService";
import { logger } from "./server/utils/logger";

// Halving date
const HALVING_DATE = new Date('2024-04-20');
const HALVING_DATE_STR = '2024-04-20';

// Options
const DRY_RUN = process.argv.includes('--dry-run');
const FORCE_ALL = process.argv.includes('--force-all');
const MAX_RECORDS = 5000; // Maximum records to process in a batch
const LIMIT_TO_RECENT = true; // Only process specific months
const SPECIFIC_MONTH = process.argv.includes('--april-2025') || process.argv.includes('--current-month');
const RECENT_DATE_CUTOFF = SPECIFIC_MONTH ? '2025-04-01' : '2025-01-01';

async function recalculatePostHalvingRecords() {
  try {
    console.log(`\n=== Bitcoin Halving Recalculation Script ===`);
    console.log(`Halving date: ${HALVING_DATE_STR}`);
    console.log(`Dry run: ${DRY_RUN ? 'Yes (no changes will be made)' : 'No (database will be updated)'}`);
    console.log(`Force recalculation of all records: ${FORCE_ALL ? 'Yes' : 'No (only post-halving)'}\n`);

    // Get affected dates (unique settlement dates after halving)
    let dateQuery;
    
    if (LIMIT_TO_RECENT) {
      // Only query dates after Jan 1, 2025 (more recent data)
      console.log(`Limiting to dates after ${RECENT_DATE_CUTOFF} for faster processing`);
      
      dateQuery = db.select({ settlementDate: historicalBitcoinCalculations.settlementDate })
        .from(historicalBitcoinCalculations)
        .where(gt(historicalBitcoinCalculations.settlementDate, RECENT_DATE_CUTOFF))
        .groupBy(historicalBitcoinCalculations.settlementDate)
        .orderBy(historicalBitcoinCalculations.settlementDate);
    } else if (FORCE_ALL) {
      // Process all dates regardless of halving
      dateQuery = db.select({ settlementDate: historicalBitcoinCalculations.settlementDate })
        .from(historicalBitcoinCalculations)
        .groupBy(historicalBitcoinCalculations.settlementDate)
        .orderBy(historicalBitcoinCalculations.settlementDate);
    } else {
      // Only process dates after halving
      dateQuery = db.select({ settlementDate: historicalBitcoinCalculations.settlementDate })
        .from(historicalBitcoinCalculations)
        .where(gt(historicalBitcoinCalculations.settlementDate, HALVING_DATE_STR))
        .groupBy(historicalBitcoinCalculations.settlementDate)
        .orderBy(historicalBitcoinCalculations.settlementDate);
    }

    const dates = await dateQuery;
    
    if (dates.length === 0) {
      console.log(`No dates ${FORCE_ALL ? '' : 'after halving'} found in historical Bitcoin calculations.`);
      return;
    }

    console.log(`Found ${dates.length} dates ${FORCE_ALL ? '' : 'after halving'} to recalculate.`);
    
    // Group by year-month for later monthly summary updates
    const yearMonths = new Set<string>();
    
    // Process date by date
    let totalRecords = 0;
    let totalUpdated = 0;
    
    for (const { settlementDate } of dates) {
      const date = parseISO(settlementDate);
      const yearMonth = format(date, 'yyyy-MM');
      yearMonths.add(yearMonth);
      
      console.log(`\nProcessing date: ${settlementDate}`);
      
      // Get all records for this date
      const records = await db.query.historicalBitcoinCalculations.findMany({
        where: eq(historicalBitcoinCalculations.settlementDate, settlementDate),
        limit: MAX_RECORDS
      });
      
      console.log(`Found ${records.length} records for ${settlementDate}`);
      
      let dateUpdated = 0;
      for (const record of records) {
        // Only recalculate if date is after halving or if forcing all records
        if (FORCE_ALL || new Date(record.settlementDate) >= HALVING_DATE) {
          // Recalculate bitcoin mining based on the correct date (post-halving)
          const date = parseISO(record.settlementDate);
          
          // Convert strings to numbers and validate inputs before calculation
          const energy = Number(record.curtailedEnergy);
          const difficulty = Number(record.difficulty);
          const minerModel = record.minerModel;
          
          // Only log the first few records for debugging
          if (dateUpdated < 2) {
            console.log(`Debug inputs:`, {
              energy: energy,
              minerModel: minerModel,
              difficulty: difficulty,
              date: date.toISOString(),
              isEnergyValid: !isNaN(energy) && energy > 0,
              isDifficultyValid: !isNaN(difficulty) && difficulty > 0
            });
          }
          
          // Skip invalid records
          if (isNaN(energy) || energy <= 0 || isNaN(difficulty) || difficulty <= 0) {
            console.log(`Skipping record ${record.id} due to invalid inputs: energy=${energy}, difficulty=${difficulty}`);
            continue;
          }
          
          let recalculatedBitcoin;
          try {
            recalculatedBitcoin = calculateBitcoin(
              energy, 
              minerModel, 
              difficulty,
              date
            );
          } catch (error) {
            console.error(`Error calculating Bitcoin for record ${record.id}:`, error);
            continue;
          }
          
          const currentBitcoin = Number(record.bitcoinMined);
          const difference = recalculatedBitcoin - currentBitcoin;
          const absoluteDifference = Math.abs(difference);
          const percentChange = (absoluteDifference / currentBitcoin) * 100;
          
          // Log some sample records for debugging
          if (dateUpdated < 5) {
            console.log(`Record ${record.id}: Current BTC=${currentBitcoin}, Recalculated BTC=${recalculatedBitcoin}, Diff=${difference.toFixed(8)} (${percentChange.toFixed(2)}%)`);
          }
          
          if (Math.abs(percentChange) > 0.01) { // Lower threshold to catch more changes
            totalUpdated++;
            dateUpdated++;
            
            if (!DRY_RUN) {
              // Update the record with recalculated value
              await db.update(historicalBitcoinCalculations)
                .set({
                  bitcoinMined: recalculatedBitcoin.toString(),
                  calculatedAt: new Date() // Use calculatedAt instead of updatedAt
                })
                .where(and(
                  eq(historicalBitcoinCalculations.id, record.id)
                ));
            }
            
            if (dateUpdated <= 5 || dateUpdated % 100 === 0) {
              // Log a few sample updates
              console.log(`${DRY_RUN ? '[DRY RUN] Would update' : 'Updated'} record ${record.id}: ${currentBitcoin} BTC -> ${recalculatedBitcoin} BTC (${percentChange.toFixed(2)}% ${percentChange >= 0 ? 'decrease' : 'increase'})`);
            }
          }
        }
      }
      
      totalRecords += records.length;
      console.log(`Updated ${dateUpdated} out of ${records.length} records for ${settlementDate}`);
    }
    
    console.log(`\nRecalculation summary:`);
    console.log(`Total records processed: ${totalRecords}`);
    console.log(`Total records updated: ${totalUpdated}`);
    
    // Update monthly summaries
    if (!DRY_RUN && totalUpdated > 0) {
      console.log(`\nUpdating monthly summaries for affected months:`);
      
      const minerModels = ['S19J_PRO', 'S9', 'M20S'];
      for (const yearMonth of yearMonths) {
        console.log(`Updating summaries for ${yearMonth}...`);
        
        // Update monthly summaries for each miner model
        for (const minerModel of minerModels) {
          await calculateMonthlyBitcoinSummary(yearMonth, minerModel)
            .catch(error => {
              console.error(`Error updating monthly summary for ${yearMonth} with ${minerModel}:`, error);
            });
        }
      }
      
      // Update yearly summaries
      console.log(`\nUpdating yearly summaries for affected years:`);
      const years = new Set([...yearMonths].map(ym => ym.substring(0, 4)));
      
      for (const year of years) {
        console.log(`Updating summaries for ${year}...`);
        await manualUpdateYearlyBitcoinSummary(year)
          .catch(error => {
            console.error(`Error updating yearly summary for ${year}:`, error);
          });
      }
    }
    
    console.log(`\n=== Bitcoin Halving Recalculation ${DRY_RUN ? 'Simulation' : 'Process'} Complete ===`);
    
    if (DRY_RUN) {
      console.log(`\nThis was a dry run. To apply changes, run without the --dry-run flag.`);
    }
  } catch (error) {
    console.error('Error during recalculation:', error);
  } finally {
    // Exit the process
    process.exit(0);
  }
}

// Execute the script
recalculatePostHalvingRecords();