/**
 * Reconcile 2024 Data
 * 
 * This script analyzes the data reconciliation between curtailment_records and 
 * historicalBitcoinCalculations tables for all 2024 data, identifying and fixing
 * any missing Bitcoin calculations.
 * 
 * For each curtailment_record, there should be 3 corresponding historicalBitcoinCalculations
 * (one for each miner model: S19J_PRO, M20S, and S9).
 */

import { db } from "@db";
import { curtailmentRecords, historicalBitcoinCalculations } from "@db/schema";
import { eq, and, sql, like } from "drizzle-orm";
import { format, parseISO, differenceInDays } from "date-fns";
import { processSingleDay } from "../services/bitcoinService";
import pLimit from "p-limit";

// Configuration
const START_DATE = '2024-01-01';
const END_DATE = '2024-12-31';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const CONCURRENCY_LIMIT = 3; // Limit concurrency to avoid database overload
const BATCH_SIZE = 5; // Process dates in batches

// Type definition for tracking data
interface ReconciliationStats {
  date: string;
  totalCurtailmentRecords: number;
  totalPeriods: number;
  totalFarms: number;
  missingCalculations: {
    [key: string]: { // miner model
      count: number;
      periods: number[];
    }
  },
  fixed: boolean;
}

/**
 * Sleep for specified milliseconds
 */
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Analyze a specific date to check for missing Bitcoin calculations
 */
async function analyzeDate(date: string): Promise<ReconciliationStats> {
  console.log(`Analyzing ${date}...`);
  
  // Get curtailment statistics
  const curtailmentData = await db
    .select({
      count: sql<number>`COUNT(*)`,
      periods: sql<number[]>`array_agg(DISTINCT settlement_period)`,
      farms: sql<string[]>`array_agg(DISTINCT farm_id)`
    })
    .from(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, date),
        sql`ABS(volume::numeric) > 0`
      )
    );
  
  const totalCurtailmentRecords = curtailmentData[0]?.count || 0;
  const periods = curtailmentData[0]?.periods || [];
  const farms = curtailmentData[0]?.farms || [];
  
  // Initialize stats object
  const stats: ReconciliationStats = {
    date,
    totalCurtailmentRecords,
    totalPeriods: periods.length,
    totalFarms: farms.length,
    missingCalculations: {},
    fixed: false
  };
  
  // If no curtailment records, return early
  if (totalCurtailmentRecords === 0) {
    console.log(`No curtailment records found for ${date}`);
    return stats;
  }
  
  // Check each miner model for missing calculations
  for (const minerModel of MINER_MODELS) {
    // Get periods present in curtailment records
    const curtailmentPeriods = await db
      .select({
        period: curtailmentRecords.settlementPeriod
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          sql`ABS(volume::numeric) > 0`
        )
      )
      .groupBy(curtailmentRecords.settlementPeriod);
    
    const curtailmentPeriodList = curtailmentPeriods.map(r => r.period);
    
    // Get periods present in bitcoin calculations
    const calculationPeriods = await db
      .select({
        period: historicalBitcoinCalculations.settlementPeriod
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, date),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      )
      .groupBy(historicalBitcoinCalculations.settlementPeriod);
    
    const calculationPeriodList = calculationPeriods.map(r => r.period);
    
    // Find missing periods
    const missingPeriods = curtailmentPeriodList.filter(
      period => !calculationPeriodList.includes(period)
    );
    
    if (missingPeriods.length > 0) {
      // Store missing periods info
      stats.missingCalculations[minerModel] = {
        count: missingPeriods.length,
        periods: missingPeriods
      };
    }
  }
  
  return stats;
}

/**
 * Fix missing calculations for a date
 */
async function fixMissingCalculations(date: string, stats: ReconciliationStats): Promise<boolean> {
  try {
    const modelsWithMissing = Object.keys(stats.missingCalculations);
    
    if (modelsWithMissing.length === 0) {
      console.log(`✓ No missing calculations to fix for ${date}`);
      return false;
    }
    
    console.log(`Fixing missing calculations for ${date}:`);
    console.log(`- Missing miner models: ${modelsWithMissing.join(', ')}`);
    
    for (const minerModel of modelsWithMissing) {
      const missingInfo = stats.missingCalculations[minerModel];
      console.log(`- ${minerModel}: Missing ${missingInfo.count} periods: ${missingInfo.periods.join(', ')}`);
      
      // Process this day for the miner model
      await processSingleDay(date, minerModel);
      console.log(`✓ Processed ${date} for ${minerModel}`);
    }
    
    // Verify fix
    const verificationStats = await analyzeDate(date);
    const verificationModelsWithMissing = Object.keys(verificationStats.missingCalculations);
    
    if (verificationModelsWithMissing.length === 0) {
      console.log(`✓ Successfully fixed all calculations for ${date}`);
      return true;
    } else {
      console.log(`× Failed to fix some calculations for ${date}:`);
      console.log(`- Still missing: ${verificationModelsWithMissing.join(', ')}`);
      return false;
    }
  } catch (error) {
    console.error(`Error fixing missing calculations for ${date}:`, error);
    return false;
  }
}

/**
 * Get all dates in 2024 with curtailment records
 */
async function get2024Dates(): Promise<string[]> {
  console.log('Finding all 2024 dates with curtailment records...');
  
  const result = await db
    .select({
      date: curtailmentRecords.settlementDate
    })
    .from(curtailmentRecords)
    .where(like(sql`settlement_date::text`, '2024-%'))
    .groupBy(curtailmentRecords.settlementDate)
    .orderBy(curtailmentRecords.settlementDate);
  
  const dates = result.map(row => format(row.date, 'yyyy-MM-dd'));
  console.log(`Found ${dates.length} dates in 2024 with curtailment records`);
  
  return dates;
}

/**
 * Main function to reconcile 2024 data
 */
async function reconcile2024Data() {
  try {
    console.log(`=== Starting 2024 Data Reconciliation ===`);
    console.log(`Time range: ${START_DATE} to ${END_DATE}`);
    
    // Get all 2024 dates with curtailment records
    const allDates = await get2024Dates();
    
    if (allDates.length === 0) {
      console.log('No 2024 data found in curtailment_records table.');
      return;
    }
    
    // Track statistics
    const datesToFix: string[] = [];
    let totalMissingCalculations = 0;
    const missingByModel: Record<string, number> = {};
    const reconciliationResults: ReconciliationStats[] = [];
    
    // Process dates in batches with limited concurrency
    const limit = pLimit(CONCURRENCY_LIMIT);
    
    console.log(`\n=== Analyzing Dates ===`);
    for (let i = 0; i < allDates.length; i += BATCH_SIZE) {
      const batch = allDates.slice(i, i + BATCH_SIZE);
      
      const batchResults = await Promise.all(
        batch.map(date => limit(() => analyzeDate(date)))
      );
      
      // Process batch results
      for (const stats of batchResults) {
        reconciliationResults.push(stats);
        
        const modelsWithMissing = Object.keys(stats.missingCalculations);
        if (modelsWithMissing.length > 0) {
          datesToFix.push(stats.date);
          
          for (const model of modelsWithMissing) {
            const count = stats.missingCalculations[model].count;
            totalMissingCalculations += count;
            missingByModel[model] = (missingByModel[model] || 0) + count;
          }
        }
      }
      
      // Print progress
      const progress = ((i + batch.length) / allDates.length * 100).toFixed(1);
      console.log(`Progress: ${progress}% (${i + batch.length}/${allDates.length} dates)`);
      
      // Add a small delay between batches to avoid hammering the database
      if (i + BATCH_SIZE < allDates.length) {
        await sleep(500);
      }
    }
    
    // Print analysis summary
    console.log(`\n=== Analysis Summary ===`);
    console.log(`Total dates examined: ${allDates.length}`);
    console.log(`Dates with missing calculations: ${datesToFix.length}`);
    console.log(`Total missing calculations: ${totalMissingCalculations}`);
    
    if (Object.keys(missingByModel).length > 0) {
      console.log(`\nMissing calculations by miner model:`);
      for (const [model, count] of Object.entries(missingByModel)) {
        console.log(`- ${model}: ${count}`);
      }
    }
    
    // Fix missing data if needed
    if (datesToFix.length > 0) {
      console.log(`\n=== Fixing Missing Calculations ===`);
      console.log(`Dates to fix: ${datesToFix.length}`);
      
      let fixedCount = 0;
      
      // Process dates in batches
      for (let i = 0; i < datesToFix.length; i += BATCH_SIZE) {
        const batch = datesToFix.slice(i, i + BATCH_SIZE);
        
        // Fix each date in the batch sequentially
        for (const date of batch) {
          const stats = reconciliationResults.find(r => r.date === date);
          if (stats) {
            const fixed = await fixMissingCalculations(date, stats);
            if (fixed) {
              fixedCount++;
              stats.fixed = true;
            }
          }
        }
        
        // Print progress
        const progress = ((i + batch.length) / datesToFix.length * 100).toFixed(1);
        console.log(`Fix progress: ${progress}% (${i + batch.length}/${datesToFix.length} dates)`);
        
        // Add a delay between batches to avoid overloading the database
        if (i + BATCH_SIZE < datesToFix.length) {
          await sleep(1000);
        }
      }
      
      console.log(`\n=== Fix Summary ===`);
      console.log(`Dates fixed: ${fixedCount}/${datesToFix.length}`);
      
      // List any dates that couldn't be fixed
      const unfixedDates = datesToFix.filter(date => {
        const stats = reconciliationResults.find(r => r.date === date);
        return stats && !stats.fixed;
      });
      
      if (unfixedDates.length > 0) {
        console.log(`\nDates that couldn't be fixed (${unfixedDates.length}):`);
        unfixedDates.forEach(date => console.log(`- ${date}`));
      }
    } else {
      console.log('\n✓ All 2024 data is properly reconciled! No fixes needed.');
    }
    
    console.log(`\n=== Reconciliation Complete ===`);
  } catch (error) {
    console.error('Error during 2024 data reconciliation:', error);
    throw error;
  }
}

// Run the reconciliation if this script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  reconcile2024Data()
    .then(() => {
      console.log('Reconciliation script completed successfully');
      process.exit(0);
    })
    .catch(error => {
      console.error('Fatal error during reconciliation:', error);
      process.exit(1);
    });
}

export { reconcile2024Data };