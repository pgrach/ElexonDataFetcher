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
import { eq, and, sql, between } from "drizzle-orm";
import { format, parseISO, eachDayOfInterval } from "date-fns";
import { processSingleDay, fetch2024Difficulties } from "../services/bitcoinService";
import pLimit from "p-limit";
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// ES Module path setup
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const START_DATE = '2024-01-01';
const END_DATE = '2024-12-31';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const CONCURRENCY_LIMIT = 2; // Lower concurrency to avoid database overload
const BATCH_SIZE = 3; // Process smaller batches to avoid timeouts
const CHECKPOINT_DIR = path.join(__dirname, '..', 'data', 'reconciliation');
const CHECKPOINT_FILE = path.join(CHECKPOINT_DIR, 'reconcile2024_checkpoint.json');

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

interface ReconciliationCheckpoint {
  lastAnalyzedDate: string | null;
  analyzedDates: string[];
  datesToFix: string[];
  fixedDates: string[];
  unfixedDates: string[];
  currentBatch: string[];
  lastUpdated: string;
  missingByModel: Record<string, number>;
  totalMissingCalculations: number;
}

// Initialize checkpoint
let checkpoint: ReconciliationCheckpoint = {
  lastAnalyzedDate: null,
  analyzedDates: [],
  datesToFix: [],
  fixedDates: [],
  unfixedDates: [],
  currentBatch: [],
  lastUpdated: new Date().toISOString(),
  missingByModel: {},
  totalMissingCalculations: 0
};

/**
 * Sleep for specified milliseconds
 */
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Save checkpoint to file
 */
function saveCheckpoint(): void {
  try {
    // Update timestamp
    checkpoint.lastUpdated = new Date().toISOString();
    
    // Ensure directory exists
    if (!fs.existsSync(CHECKPOINT_DIR)) {
      fs.mkdirSync(CHECKPOINT_DIR, { recursive: true });
    }
    
    // Write to file
    fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(checkpoint, null, 2));
    console.log(`Checkpoint saved at ${new Date().toISOString()}`);
  } catch (error) {
    console.error('Error saving checkpoint:', error);
  }
}

/**
 * Load checkpoint from file if exists
 */
function loadCheckpoint(): boolean {
  try {
    if (fs.existsSync(CHECKPOINT_FILE)) {
      const data = fs.readFileSync(CHECKPOINT_FILE, 'utf-8');
      checkpoint = JSON.parse(data);
      console.log(`Loaded checkpoint from ${checkpoint.lastUpdated}`);
      console.log(`Progress: Analyzed ${checkpoint.analyzedDates.length} dates, Fixed ${checkpoint.fixedDates.length}/${checkpoint.datesToFix.length}`);
      return true;
    }
  } catch (error) {
    console.error('Error loading checkpoint:', error);
  }
  
  return false;
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
    .where(
      between(
        curtailmentRecords.settlementDate,
        START_DATE,
        END_DATE
      )
    )
    .groupBy(curtailmentRecords.settlementDate)
    .orderBy(curtailmentRecords.settlementDate);
  
  const dates = result.map(row => format(row.date, 'yyyy-MM-dd'));
  console.log(`Found ${dates.length} dates in 2024 with curtailment records`);
  
  return dates;
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
    
    // Ensure we have difficulties loaded
    await fetch2024Difficulties();
    
    for (const minerModel of modelsWithMissing) {
      const missingInfo = stats.missingCalculations[minerModel];
      console.log(`- ${minerModel}: Missing ${missingInfo.count} periods: ${missingInfo.periods.join(', ')}`);
      
      // Process this day for the miner model
      try {
        await processSingleDay(date, minerModel);
        console.log(`✓ Processed ${date} for ${minerModel}`);
      } catch (error) {
        console.error(`Error processing ${date} for ${minerModel}:`, error);
        // Continue with next model even if this one fails
      }
      
      // Add a short delay between model processing
      await sleep(1000);
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
 * Main function to reconcile 2024 data
 */
async function reconcile2024Data() {
  try {
    console.log(`=== Starting 2024 Data Reconciliation ===`);
    console.log(`Time range: ${START_DATE} to ${END_DATE}`);
    
    // Load checkpoint if exists
    const checkpointExists = loadCheckpoint();
    
    // Get all 2024 dates with curtailment records if not in checkpoint
    const allDates = checkpointExists && checkpoint.analyzedDates.length > 0 ? 
      [] : await get2024Dates();
    
    if (!checkpointExists && allDates.length === 0) {
      console.log('No 2024 data found in curtailment_records table.');
      return;
    }
    
    // Get remaining dates to analyze (if continuing from checkpoint)
    let datesToAnalyze = allDates;
    if (checkpointExists) {
      if (checkpoint.currentBatch.length > 0) {
        // Resume current batch first
        console.log(`Resuming analysis of current batch: ${checkpoint.currentBatch.join(', ')}`);
        datesToAnalyze = checkpoint.currentBatch;
      } else if (checkpoint.lastAnalyzedDate) {
        // Continue from the last analyzed date
        const remainingDates = allDates.filter(date => date > checkpoint.lastAnalyzedDate!);
        console.log(`Continuing analysis from ${checkpoint.lastAnalyzedDate}. ${remainingDates.length} dates remaining.`);
        datesToAnalyze = remainingDates;
      }
    }
    
    // If all analysis is complete but fixes remain
    if (checkpointExists && datesToAnalyze.length === 0 && checkpoint.datesToFix.length > 0) {
      console.log('All dates analyzed. Proceeding to fix phase.');
    } else if (datesToAnalyze.length > 0) {
      // Analysis phase
      console.log(`\n=== Analyzing Dates (${datesToAnalyze.length} remaining) ===`);
      
      // Process dates in batches with limited concurrency
      const limit = pLimit(CONCURRENCY_LIMIT);
      
      // Collect new stats during this run
      const reconciliationResults: ReconciliationStats[] = [];
      
      for (let i = 0; i < datesToAnalyze.length; i += BATCH_SIZE) {
        const batch = datesToAnalyze.slice(i, i + BATCH_SIZE);
        
        // Save current batch to checkpoint
        checkpoint.currentBatch = batch;
        saveCheckpoint();
        
        try {
          const batchResults = await Promise.all(
            batch.map(date => limit(() => analyzeDate(date)))
          );
          
          // Process batch results
          for (const stats of batchResults) {
            reconciliationResults.push(stats);
            checkpoint.analyzedDates.push(stats.date);
            checkpoint.lastAnalyzedDate = stats.date;
            
            const modelsWithMissing = Object.keys(stats.missingCalculations);
            if (modelsWithMissing.length > 0) {
              checkpoint.datesToFix.push(stats.date);
              
              for (const model of modelsWithMissing) {
                const count = stats.missingCalculations[model].count;
                checkpoint.totalMissingCalculations += count;
                checkpoint.missingByModel[model] = (checkpoint.missingByModel[model] || 0) + count;
              }
            }
            
            // Save checkpoint after each date to track progress
            saveCheckpoint();
          }
          
          // Clear current batch since it's completed
          checkpoint.currentBatch = [];
          saveCheckpoint();
          
          // Print progress
          const progress = ((i + batch.length) / datesToAnalyze.length * 100).toFixed(1);
          console.log(`Analysis progress: ${progress}% (${i + batch.length}/${datesToAnalyze.length} dates)`);
          
          // Add a small delay between batches to avoid hammering the database
          if (i + BATCH_SIZE < datesToAnalyze.length) {
            await sleep(1000);
          }
        } catch (error) {
          console.error(`Error processing batch: ${batch.join(', ')}`, error);
          // Save checkpoint so we can resume from this batch
          saveCheckpoint();
          throw error;
        }
      }
    }
    
    // Print analysis summary
    console.log(`\n=== Analysis Summary ===`);
    console.log(`Total dates examined: ${checkpoint.analyzedDates.length}`);
    console.log(`Dates with missing calculations: ${checkpoint.datesToFix.length}`);
    console.log(`Total missing calculations: ${checkpoint.totalMissingCalculations}`);
    
    if (Object.keys(checkpoint.missingByModel).length > 0) {
      console.log(`\nMissing calculations by miner model:`);
      for (const [model, count] of Object.entries(checkpoint.missingByModel)) {
        console.log(`- ${model}: ${count}`);
      }
    }
    
    // Get remaining dates to fix (exclude already fixed or tried)
    const remainingToFix = checkpoint.datesToFix.filter(
      date => !checkpoint.fixedDates.includes(date) && !checkpoint.unfixedDates.includes(date)
    );
    
    // Fix missing data if needed
    if (remainingToFix.length > 0) {
      console.log(`\n=== Fixing Missing Calculations (${remainingToFix.length} remaining) ===`);
      
      // Process dates in smaller batches for fix phase
      const fixBatchSize = 1; // Fix one at a time to be safer
      
      for (let i = 0; i < remainingToFix.length; i += fixBatchSize) {
        const batch = remainingToFix.slice(i, i + fixBatchSize);
        
        // Fix each date in the batch
        for (const date of batch) {
          console.log(`\nAttempting to fix ${date} (${i + 1}/${remainingToFix.length})`);
          
          try {
            // Analyze the date to get current status
            const stats = await analyzeDate(date);
            
            // Try to fix the date
            const fixed = await fixMissingCalculations(date, stats);
            
            if (fixed) {
              checkpoint.fixedDates.push(date);
              console.log(`✓ Added ${date} to fixed dates list`);
            } else {
              checkpoint.unfixedDates.push(date);
              console.log(`× Added ${date} to unfixed dates list`);
            }
            
            // Save checkpoint after each fix attempt
            saveCheckpoint();
          } catch (error) {
            console.error(`Error fixing date ${date}:`, error);
            // Add to unfixed so we don't retry in this run
            if (!checkpoint.unfixedDates.includes(date)) {
              checkpoint.unfixedDates.push(date);
            }
            saveCheckpoint();
          }
        }
        
        // Print progress
        const progress = ((i + batch.length) / remainingToFix.length * 100).toFixed(1);
        console.log(`Fix progress: ${progress}% (${i + batch.length}/${remainingToFix.length} dates)`);
        
        // Add a delay between batches to avoid overloading the database
        if (i + fixBatchSize < remainingToFix.length) {
          await sleep(2000);
        }
      }
      
      console.log(`\n=== Fix Summary ===`);
      console.log(`Dates fixed: ${checkpoint.fixedDates.length}/${checkpoint.datesToFix.length}`);
      
      // List any dates that couldn't be fixed
      if (checkpoint.unfixedDates.length > 0) {
        console.log(`\nDates that couldn't be fixed (${checkpoint.unfixedDates.length}):`);
        checkpoint.unfixedDates.forEach(date => console.log(`- ${date}`));
      }
    } else if (checkpoint.datesToFix.length === 0) {
      console.log('\n✓ All 2024 data is properly reconciled! No fixes needed.');
    } else {
      console.log(`\n=== Fix Summary ===`);
      console.log(`All fixes attempted. Dates fixed: ${checkpoint.fixedDates.length}/${checkpoint.datesToFix.length}`);
      
      if (checkpoint.unfixedDates.length > 0) {
        console.log(`\nDates that couldn't be fixed (${checkpoint.unfixedDates.length}):`);
        checkpoint.unfixedDates.forEach(date => console.log(`- ${date}`));
      }
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