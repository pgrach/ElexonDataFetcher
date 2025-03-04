/**
 * Reconcile 2023 Data
 * 
 * This script analyzes the data reconciliation between curtailment_records and 
 * historicalBitcoinCalculations tables for all 2023 data, identifying and fixing
 * any missing Bitcoin calculations.
 * 
 * For each curtailment_record, there should be 3 corresponding historicalBitcoinCalculations
 * (one for each miner model: S19J_PRO, M20S, and S9).
 */

import { db } from "@db";
import { curtailmentRecords, historicalBitcoinCalculations } from "@db/schema";
import { eq, and, sql, like, between, desc, gt, or } from "drizzle-orm";
import { format, parseISO } from "date-fns";
import { processSingleDay } from "../services/bitcoinService";
import pLimit from "p-limit";
import fs from "fs";
import path from "path";

// Configuration
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const CONCURRENCY_LIMIT = 2; // Lower concurrency to avoid database overload
const MAX_DATES_TO_PROCESS = 10; // Limit number of dates processed in one run
const LOG_FILE = "logs/reconcile_2023_data.log";
const PROGRESS_FILE = "reconcile_2023_data_checkpoint.json";
const BATCH_DELAY_MS = 1000; // 1 second delay between batches

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
  lastProcessedDate: string | null;
  pendingDates: string[];
  completedDates: string[];
  startTime: number;
  lastUpdateTime: number;
  stats: {
    totalDates: number;
    processedDates: number;
    successfullyFixed: number;
    failedFixes: number;
  };
}

/**
 * Sleep for specified milliseconds
 */
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Ensure log directory exists
 */
function ensureLogDirectory() {
  const dir = path.dirname(LOG_FILE);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

/**
 * Log message to console and file
 */
function log(message: string, level: 'info' | 'error' | 'warning' | 'success' = 'info') {
  const timestamp = new Date().toISOString();
  const prefix = level === 'error' ? '❌ ERROR: ' : 
                level === 'warning' ? '⚠️ WARNING: ' :
                level === 'success' ? '✅ SUCCESS: ' : '';
  
  const logMessage = `[${timestamp}] ${prefix}${message}`;
  console.log(logMessage);
  
  ensureLogDirectory();
  fs.appendFileSync(LOG_FILE, logMessage + '\n');
}

/**
 * Save checkpoint to file
 */
function saveCheckpoint(checkpoint: ReconciliationCheckpoint) {
  checkpoint.lastUpdateTime = Date.now();
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(checkpoint, null, 2));
  log(`Checkpoint saved. Progress: ${checkpoint.stats.processedDates}/${checkpoint.stats.totalDates} dates processed.`);
}

/**
 * Load checkpoint from file
 */
function loadCheckpoint(): ReconciliationCheckpoint | null {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      const data = fs.readFileSync(PROGRESS_FILE, 'utf8');
      return JSON.parse(data);
    }
  } catch (error) {
    log(`Error loading checkpoint: ${error}`, 'error');
  }
  return null;
}

/**
 * Initialize a new checkpoint
 */
function initializeCheckpoint(dates: string[]): ReconciliationCheckpoint {
  return {
    lastProcessedDate: null,
    pendingDates: [...dates],
    completedDates: [],
    startTime: Date.now(),
    lastUpdateTime: Date.now(),
    stats: {
      totalDates: dates.length,
      processedDates: 0,
      successfullyFixed: 0,
      failedFixes: 0
    }
  };
}

/**
 * Analyze a specific date to check for missing Bitcoin calculations
 */
async function analyzeDate(date: string): Promise<ReconciliationStats> {
  log(`Analyzing ${date}...`);
  
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
    log(`No curtailment records found for ${date}`);
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
      log(`No missing calculations to fix for ${date}`, 'success');
      return false;
    }
    
    log(`Fixing missing calculations for ${date}:`);
    log(`- Missing miner models: ${modelsWithMissing.join(', ')}`);
    
    // Store any errors encountered
    const errors: string[] = [];
    
    for (const minerModel of modelsWithMissing) {
      try {
        const missingInfo = stats.missingCalculations[minerModel];
        log(`- ${minerModel}: Missing ${missingInfo.count} periods: ${missingInfo.periods.join(', ')}`);
        
        // Process this day for the miner model
        await processSingleDay(date, minerModel);
        log(`Processed ${date} for ${minerModel}`, 'success');
      } catch (error) {
        const errorMessage = `Error processing ${date} for ${minerModel}: ${error}`;
        log(errorMessage, 'error');
        errors.push(errorMessage);
      }
    }
    
    if (errors.length > 0) {
      log(`Encountered ${errors.length} errors while fixing ${date}`, 'warning');
      return false;
    }
    
    // Verify fix
    const verificationStats = await analyzeDate(date);
    const verificationModelsWithMissing = Object.keys(verificationStats.missingCalculations);
    
    if (verificationModelsWithMissing.length === 0) {
      log(`Successfully fixed all calculations for ${date}`, 'success');
      return true;
    } else {
      log(`Failed to fix some calculations for ${date}:`, 'warning');
      log(`- Still missing: ${verificationModelsWithMissing.join(', ')}`);
      return false;
    }
  } catch (error) {
    log(`Error fixing missing calculations for ${date}: ${error}`, 'error');
    return false;
  }
}

/**
 * Get all dates in 2023 with curtailment records
 */
async function get2023Dates(): Promise<string[]> {
  log('Getting all dates from 2023 with curtailment records...');
  
  const dateRows = await db
    .select({
      date: curtailmentRecords.settlementDate
    })
    .from(curtailmentRecords)
    .where(
      and(
        gte(curtailmentRecords.settlementDate, '2023-01-01'),
        lt(curtailmentRecords.settlementDate, '2024-01-01')
      )
    )
    .groupBy(curtailmentRecords.settlementDate)
    .orderBy(curtailmentRecords.settlementDate);
  
  const dates = dateRows.map(row => format(row.date, 'yyyy-MM-dd'));
  log(`Found ${dates.length} dates in 2023 with curtailment records`);
  
  return dates;
}

/**
 * Main function to reconcile 2023 data
 */
async function reconcile2023Data() {
  log(`=== Starting 2023 Data Reconciliation ===`);
  
  // Load checkpoint if exists
  let checkpoint = loadCheckpoint();
  let dates: string[] = [];
  
  if (checkpoint) {
    log(`Resuming from checkpoint. ${checkpoint.pendingDates.length} dates pending, ${checkpoint.completedDates.length} dates completed.`);
    dates = [...checkpoint.pendingDates, ...checkpoint.completedDates];
  } else {
    // Get all dates in 2023 with curtailment records
    dates = await get2023Dates();
    checkpoint = initializeCheckpoint(dates);
    saveCheckpoint(checkpoint);
  }
  
  if (dates.length === 0) {
    log('No 2023 dates found with curtailment records.', 'warning');
    return;
  }
  
  log(`Found ${dates.length} dates in 2023 with curtailment records`);
  
  // Process dates with potential issues
  const datesToProcess = checkpoint.pendingDates.slice(0, MAX_DATES_TO_PROCESS);
  log(`Processing batch of ${datesToProcess.length} dates: ${datesToProcess.join(', ')}`);
  
  // Track statistics
  let fixedCount = 0;
  let failedCount = 0;
  
  // Process dates with limited concurrency
  const limit = pLimit(CONCURRENCY_LIMIT);
  
  for (let i = 0; i < datesToProcess.length; i += CONCURRENCY_LIMIT) {
    const batch = datesToProcess.slice(i, i + CONCURRENCY_LIMIT);
    
    const batchPromises = batch.map(date => limit(async () => {
      try {
        checkpoint!.lastProcessedDate = date;
        
        // Analyze and fix date
        const stats = await analyzeDate(date);
        const hasIssues = Object.keys(stats.missingCalculations).length > 0;
        
        if (hasIssues) {
          log(`Found missing calculations for ${date}`, 'warning');
          const fixed = await fixMissingCalculations(date, stats);
          
          if (fixed) {
            fixedCount++;
            checkpoint!.stats.successfullyFixed++;
            log(`Fixed all missing calculations for ${date}`, 'success');
          } else {
            failedCount++;
            checkpoint!.stats.failedFixes++;
            log(`Failed to fix some calculations for ${date}`, 'warning');
          }
        } else {
          log(`No issues found for ${date}`, 'success');
        }
        
        // Mark date as completed
        checkpoint!.pendingDates = checkpoint!.pendingDates.filter(d => d !== date);
        checkpoint!.completedDates.push(date);
        checkpoint!.stats.processedDates++;
        
        return { date, fixed: hasIssues ? true : false, success: hasIssues ? true : false };
      } catch (error) {
        log(`Error processing ${date}: ${error}`, 'error');
        failedCount++;
        checkpoint!.stats.failedFixes++;
        
        // Still mark as processed but failed
        checkpoint!.pendingDates = checkpoint!.pendingDates.filter(d => d !== date);
        checkpoint!.completedDates.push(date);
        checkpoint!.stats.processedDates++;
        
        return { date, fixed: false, success: false };
      }
    }));
    
    await Promise.all(batchPromises);
    saveCheckpoint(checkpoint);
    
    // Add delay between batches to avoid overloading the database
    if (i + CONCURRENCY_LIMIT < datesToProcess.length) {
      log(`Batch completed. Pausing for ${BATCH_DELAY_MS}ms before next batch...`);
      await sleep(BATCH_DELAY_MS);
    }
  }
  
  // Print summary for this batch
  log(`\n=== Batch Reconciliation Summary ===`);
  log(`Total dates processed in this batch: ${datesToProcess.length}`);
  log(`Dates fixed: ${fixedCount}`);
  log(`Dates with failed fixes: ${failedCount}`);
  
  // Print overall progress
  log(`\n=== Overall Progress ===`);
  log(`Total dates: ${checkpoint.stats.totalDates}`);
  log(`Dates processed: ${checkpoint.stats.processedDates} (${((checkpoint.stats.processedDates / checkpoint.stats.totalDates) * 100).toFixed(1)}%)`);
  log(`Dates successfully fixed: ${checkpoint.stats.successfullyFixed}`);
  log(`Dates with failed fixes: ${checkpoint.stats.failedFixes}`);
  log(`Remaining dates: ${checkpoint.pendingDates.length}`);
  
  if (checkpoint.pendingDates.length === 0) {
    log(`\n=== 2023 Data Reconciliation Complete ===`, 'success');
    log(`All ${checkpoint.stats.totalDates} dates have been processed.`);
    log(`Total dates fixed: ${checkpoint.stats.successfullyFixed}`);
    log(`Total dates with failed fixes: ${checkpoint.stats.failedFixes}`);
    
    // Cleanup checkpoint file
    fs.unlinkSync(PROGRESS_FILE);
  } else {
    log(`\n=== 2023 Data Reconciliation Partially Complete ===`);
    log(`${checkpoint.pendingDates.length} dates remaining to process.`);
    log(`Run this script again to continue processing the next batch.`);
  }
}

// Run the reconciliation if this script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  reconcile2023Data()
    .then(() => {
      log('Reconciliation script completed successfully');
      process.exit(0);
    })
    .catch(error => {
      log(`Fatal error during reconciliation: ${error}`, 'error');
      process.exit(1);
    });
}

export { reconcile2023Data };