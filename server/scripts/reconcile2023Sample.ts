/**
 * Reconcile 2023 Data (Sample Analysis)
 * 
 * This script analyzes a sample of dates from 2023 to check data reconciliation
 * between curtailment_records and historicalBitcoinCalculations tables.
 * 
 * It takes a sample of dates from different months to identify potential issues.
 */

import { db } from "@db";
import { curtailmentRecords, historicalBitcoinCalculations } from "@db/schema";
import { eq, and, sql, like } from "drizzle-orm";
import { format, parseISO } from "date-fns";
import { processSingleDay } from "../services/bitcoinService";
import pLimit from "p-limit";

// Configuration
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const CONCURRENCY_LIMIT = 3; // Limit concurrency to avoid database overload
const SAMPLE_SIZE = 24; // Analyze 2 dates per month

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
 * Get sample dates from 2023 for analysis
 */
async function getSampleDates(): Promise<string[]> {
  console.log('Getting sample dates from 2023...');
  
  // Collect sample dates from different months
  const sampleDates: string[] = [];
  
  for (let month = 1; month <= 12; month++) {
    const monthStr = month.toString().padStart(2, '0');
    
    // Get 2 dates from each month
    const monthDates = await db
      .select({
        date: curtailmentRecords.settlementDate
      })
      .from(curtailmentRecords)
      .where(like(sql`settlement_date::text`, `2023-${monthStr}-%`))
      .groupBy(curtailmentRecords.settlementDate)
      .orderBy(curtailmentRecords.settlementDate)
      .limit(2);
    
    monthDates.forEach(row => {
      sampleDates.push(format(row.date, 'yyyy-MM-dd'));
    });
  }
  
  console.log(`Selected ${sampleDates.length} sample dates from 2023`);
  return sampleDates;
}

/**
 * Main function to analyze and fix 2023 data samples
 */
async function reconcile2023Sample() {
  try {
    console.log(`=== Starting 2023 Data Reconciliation (Sample Analysis) ===`);
    
    // Get sample dates from 2023
    const sampleDates = await getSampleDates();
    
    if (sampleDates.length === 0) {
      console.log('No 2023 sample dates found.');
      return;
    }
    
    console.log(`Selected ${sampleDates.length} dates for analysis:`);
    console.log(sampleDates.join(', '));
    
    // Track statistics
    const datesToFix: string[] = [];
    let totalMissingCalculations = 0;
    const missingByModel: Record<string, number> = {};
    const reconciliationResults: ReconciliationStats[] = [];
    
    // Process dates in parallel with limited concurrency
    const limit = pLimit(CONCURRENCY_LIMIT);
    
    console.log(`\n=== Analyzing Sample Dates ===`);
    const analyzeResults = await Promise.all(
      sampleDates.map(date => limit(() => analyzeDate(date)))
    );
    
    // Process results
    for (const stats of analyzeResults) {
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
    
    // Print analysis summary
    console.log(`\n=== Sample Analysis Summary ===`);
    console.log(`Total dates examined: ${sampleDates.length}`);
    console.log(`Dates with missing calculations: ${datesToFix.length}`);
    console.log(`Total missing calculations: ${totalMissingCalculations}`);
    
    if (Object.keys(missingByModel).length > 0) {
      console.log(`\nMissing calculations by miner model:`);
      for (const [model, count] of Object.entries(missingByModel)) {
        console.log(`- ${model}: ${count}`);
      }
      
      console.log(`\nDates with missing calculations:`);
      for (const date of datesToFix) {
        const stats = reconciliationResults.find(r => r.date === date);
        const missingModels = Object.keys(stats?.missingCalculations || {});
        console.log(`- ${date}: Missing ${missingModels.join(', ')}`);
      }
    }
    
    // Fix missing data if needed
    if (datesToFix.length > 0) {
      console.log(`\n=== Fixing Missing Calculations ===`);
      console.log(`Dates to fix: ${datesToFix.length}`);
      
      let fixedCount = 0;
      
      // Fix each date sequentially
      for (const date of datesToFix) {
        const stats = reconciliationResults.find(r => r.date === date);
        if (stats) {
          const fixed = await fixMissingCalculations(date, stats);
          if (fixed) {
            fixedCount++;
            stats.fixed = true;
          }
          
          // Add a small delay between fixes
          await sleep(500);
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
      console.log('\n✓ All sampled 2023 data is properly reconciled! No fixes needed.');
    }
    
    console.log(`\n=== Sample Reconciliation Complete ===`);
    
    // Make recommendation based on findings
    if (datesToFix.length > 0) {
      const issuePercentage = (datesToFix.length / sampleDates.length * 100).toFixed(1);
      console.log(`\n! We found issues with ${issuePercentage}% of sampled dates.`);
      console.log(`  Recommendation: Run a comprehensive analysis on all 2023 dates.`);
    } else {
      console.log(`\n✓ No issues found in our sample. The 2023 data appears to be well-reconciled.`);
    }
    
  } catch (error) {
    console.error('Error during 2023 sample reconciliation:', error);
    throw error;
  }
}

// Run the reconciliation if this script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  reconcile2023Sample()
    .then(() => {
      console.log('Sample reconciliation script completed successfully');
      process.exit(0);
    })
    .catch(error => {
      console.error('Fatal error during reconciliation:', error);
      process.exit(1);
    });
}

export { reconcile2023Sample };