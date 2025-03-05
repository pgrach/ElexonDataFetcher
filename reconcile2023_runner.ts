/**
 * Reconcile 2023 - Processing Runner
 * 
 * This script runs the single date processor for multiple dates in sequence.
 * It handles a batch of dates and tracks progress, allowing for resuming interrupted jobs.
 * 
 * Usage:
 *   npx tsx reconcile2023_runner.ts [command] [options]
 * 
 * Commands:
 *   month YYYY-MM     - Process all dates in a specific month
 *   date YYYY-MM-DD   - Process a specific date
 *   range START END   - Process a range of dates (inclusive)
 *   top N             - Process the top N dates with the most missing calculations
 *   resume            - Resume from the last processed date
 */

import { db } from "./db";
import { sql } from "drizzle-orm";
import { spawn } from "child_process";
import * as fs from "fs";
import * as path from "path";
import * as readline from "readline";

// Constants
const PROGRESS_FILE = 'reconcile2023_progress.csv';
const LOG_FILE = 'reconcile2023_runner.log';
const CHECKPOINT_FILE = 'reconcile2023_runner_checkpoint.json';
const MAX_RETRIES = 3;

// Types
interface DateWithMissingCount {
  date: string;
  missingCount: number;
}

interface Checkpoint {
  lastCommand: string;
  args: string[];
  processedDates: string[];
  currentBatch: string[];
  pendingBatch: string[];
  failedDates: string[];
  startTime: number;
  lastUpdateTime: number;
}

// Global state
let checkpoint: Checkpoint = {
  lastCommand: '',
  args: [],
  processedDates: [],
  currentBatch: [],
  pendingBatch: [],
  failedDates: [],
  startTime: Date.now(),
  lastUpdateTime: Date.now()
};

/**
 * Log a message to console and log file
 */
function log(message: string): void {
  const timestamp = new Date().toISOString();
  const logMessage = `${timestamp} - ${message}`;
  
  console.log(message);
  fs.appendFileSync(LOG_FILE, logMessage + '\n');
}

/**
 * Save checkpoint to file
 */
function saveCheckpoint(): void {
  checkpoint.lastUpdateTime = Date.now();
  fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(checkpoint, null, 2));
  log("Checkpoint saved");
}

/**
 * Load checkpoint from file if exists
 */
function loadCheckpoint(): boolean {
  try {
    if (fs.existsSync(CHECKPOINT_FILE)) {
      const data = fs.readFileSync(CHECKPOINT_FILE, 'utf8');
      checkpoint = JSON.parse(data);
      log("Loaded checkpoint from file");
      return true;
    }
  } catch (error) {
    log(`Error loading checkpoint: ${error}`);
  }
  return false;
}

/**
 * Reset checkpoint
 */
function resetCheckpoint(command: string, args: string[]): void {
  checkpoint = {
    lastCommand: command,
    args,
    processedDates: [],
    currentBatch: [],
    pendingBatch: [],
    failedDates: [],
    startTime: Date.now(),
    lastUpdateTime: Date.now()
  };
  saveCheckpoint();
  log("Checkpoint reset");
}

/**
 * Sleep for specified milliseconds
 */
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Initialize the progress CSV file if it doesn't exist
 */
function initProgressFile(): void {
  if (!fs.existsSync(PROGRESS_FILE)) {
    fs.writeFileSync(PROGRESS_FILE, 'date,status,duration_seconds,timestamp\n');
    log("Created progress file");
  }
}

/**
 * Display help menu
 */
function showHelp(): void {
  console.log(`
Reconcile 2023 - Processing Runner

Usage:
  npx tsx reconcile2023_runner.ts [command] [options]

Commands:
  month YYYY-MM     - Process all dates in a specific month
  date YYYY-MM-DD   - Process a specific date
  range START END   - Process a range of dates (inclusive)
  top N             - Process the top N dates with the most missing calculations
  resume            - Resume from the last processed date
  stats             - Show reconciliation statistics
  help              - Show this help menu
`);
}

/**
 * Get all dates in a specific month with curtailment records
 */
async function getMonthDates(yearMonth: string): Promise<string[]> {
  log(`Fetching dates for ${yearMonth} with curtailment records...`);
  
  const [year, month] = yearMonth.split('-');
  const startDate = `${year}-${month}-01`;
  const endDate = month === "12" 
    ? `${parseInt(year) + 1}-01-01` 
    : `${year}-${String(parseInt(month) + 1).padStart(2, '0')}-01`;
  
  const result = await db.execute(sql`
    SELECT DISTINCT settlement_date::text
    FROM curtailment_records 
    WHERE settlement_date >= ${startDate}
    AND settlement_date < ${endDate}
    ORDER BY settlement_date
  `);
  
  const dates = result.rows.map((row: any) => row.settlement_date as string);
  log(`Found ${dates.length} dates in ${yearMonth} with curtailment records`);
  return dates;
}

/**
 * Get dates within a specific range
 */
async function getDateRange(startDate: string, endDate: string): Promise<string[]> {
  log(`Fetching dates from ${startDate} to ${endDate} with curtailment records...`);
  
  const result = await db.execute(sql`
    SELECT DISTINCT settlement_date::text
    FROM curtailment_records 
    WHERE settlement_date >= ${startDate}
    AND settlement_date <= ${endDate}
    ORDER BY settlement_date
  `);
  
  const dates = result.rows.map((row: any) => row.settlement_date as string);
  log(`Found ${dates.length} dates between ${startDate} and ${endDate} with curtailment records`);
  return dates;
}

/**
 * Get the top N dates with the most missing calculations
 */
async function getTopMissingDates(limit: number): Promise<DateWithMissingCount[]> {
  log(`Finding the top ${limit} dates with the most missing calculations...`);
  
  const result = await db.execute(sql`
    WITH curtailment_counts AS (
      SELECT 
        settlement_date, 
        COUNT(*) AS total_curtailment_records
      FROM curtailment_records
      WHERE settlement_date >= '2023-01-01' AND settlement_date <= '2023-12-31'
      GROUP BY settlement_date
    ),
    calculation_counts AS (
      SELECT 
        settlement_date, 
        miner_model,
        COUNT(*) AS total_calculations
      FROM historical_bitcoin_calculations
      WHERE settlement_date >= '2023-01-01' AND settlement_date <= '2023-12-31'
      GROUP BY settlement_date, miner_model
    ),
    model_totals AS (
      SELECT
        c.settlement_date,
        c.total_curtailment_records,
        COALESCE(SUM(CASE WHEN calc.miner_model = 'S19J_PRO' THEN calc.total_calculations ELSE 0 END), 0) AS s19j_pro_count,
        COALESCE(SUM(CASE WHEN calc.miner_model = 'S9' THEN calc.total_calculations ELSE 0 END), 0) AS s9_count,
        COALESCE(SUM(CASE WHEN calc.miner_model = 'M20S' THEN calc.total_calculations ELSE 0 END), 0) AS m20s_count
      FROM curtailment_counts c
      LEFT JOIN calculation_counts calc ON c.settlement_date = calc.settlement_date
      GROUP BY c.settlement_date, c.total_curtailment_records
    ),
    missing_counts AS (
      SELECT
        settlement_date,
        total_curtailment_records,
        (total_curtailment_records - s19j_pro_count) + 
        (total_curtailment_records - s9_count) + 
        (total_curtailment_records - m20s_count) AS total_missing
      FROM model_totals
      WHERE total_curtailment_records > s19j_pro_count OR 
            total_curtailment_records > s9_count OR 
            total_curtailment_records > m20s_count
    )
    SELECT settlement_date::text as date, total_missing as missing_count
    FROM missing_counts
    ORDER BY total_missing DESC
    LIMIT ${limit}
  `);
  
  const dates = result.rows.map((row: any) => ({
    date: row.date as string,
    missingCount: parseInt(row.missing_count, 10)
  }));
  
  log(`Found ${dates.length} dates with missing calculations`);
  dates.forEach(d => log(`  ${d.date}: ${d.missingCount} missing calculations`));
  
  return dates;
}

/**
 * Read previously processed dates from progress file
 */
function getProcessedDates(): string[] {
  if (!fs.existsSync(PROGRESS_FILE)) {
    return [];
  }
  
  const content = fs.readFileSync(PROGRESS_FILE, 'utf8');
  const lines = content.split('\n').slice(1); // Skip header
  
  return lines
    .filter(line => line.trim() !== '')
    .map(line => line.split(',')[0]) // Extract date
    .filter(date => date && date.trim() !== '');
}

/**
 * Process a single date using the single date processor
 */
function processDate(date: string): Promise<boolean> {
  return new Promise((resolve) => {
    log(`Processing date: ${date}`);
    
    const process = spawn('npx', ['tsx', 'reconcile2023_single_date.ts', date]);
    
    // Capture stdout and stderr
    process.stdout.on('data', (data) => {
      const output = data.toString();
      process.stdout.write(output);
    });
    
    process.stderr.on('data', (data) => {
      const output = data.toString();
      process.stderr.write(output);
    });
    
    // Handle process completion
    process.on('close', (code) => {
      const success = code === 0;
      log(`Date ${date} processed with ${success ? 'success' : 'failure'} (exit code: ${code})`);
      
      if (success) {
        checkpoint.processedDates.push(date);
      } else {
        checkpoint.failedDates.push(date);
      }
      
      saveCheckpoint();
      resolve(success);
    });
  });
}

/**
 * Process a batch of dates sequentially
 */
async function processBatch(dates: string[]): Promise<{
  totalProcessed: number;
  successful: number;
  failed: number;
}> {
  log(`Processing batch of ${dates.length} dates`);
  
  checkpoint.currentBatch = dates;
  checkpoint.pendingBatch = [...dates];
  saveCheckpoint();
  
  let successful = 0;
  let failed = 0;
  
  for (let i = 0; i < dates.length; i++) {
    const date = dates[i];
    log(`\nProcessing date ${i + 1}/${dates.length}: ${date}`);
    
    // Skip already processed dates
    if (checkpoint.processedDates.includes(date)) {
      log(`Date ${date} already processed successfully, skipping`);
      successful++;
      continue;
    }
    
    // Try processing with retries
    let success = false;
    let retries = 0;
    
    while (!success && retries < MAX_RETRIES) {
      if (retries > 0) {
        log(`Retry #${retries} for date ${date}`);
        await sleep(5000); // Wait before retry
      }
      
      success = await processDate(date);
      
      if (!success) {
        retries++;
      }
    }
    
    if (success) {
      successful++;
    } else {
      failed++;
      log(`Failed to process date ${date} after ${MAX_RETRIES} attempts`);
    }
    
    // Remove from pending batch
    checkpoint.pendingBatch = checkpoint.pendingBatch.filter(d => d !== date);
    saveCheckpoint();
    
    // Progress report
    log(`Progress: ${i + 1}/${dates.length} dates processed`);
    log(`Success: ${successful}, Failed: ${failed}`);
    
    // Give the system a brief break between dates
    await sleep(1000);
  }
  
  return { totalProcessed: dates.length, successful, failed };
}

/**
 * Process all dates in a specific month
 */
async function processMonth(yearMonth: string): Promise<void> {
  log(`\n===== PROCESSING MONTH: ${yearMonth} =====`);
  
  resetCheckpoint('month', [yearMonth]);
  
  // Get all dates in this month with curtailment records
  const dates = await getMonthDates(yearMonth);
  
  if (dates.length === 0) {
    log(`No dates found for month ${yearMonth}`);
    return;
  }
  
  // Process the dates
  const results = await processBatch(dates);
  
  // Log summary
  log(`\n===== MONTH ${yearMonth} RECONCILIATION COMPLETE =====`);
  log(`Total dates processed: ${results.totalProcessed}`);
  log(`Successfully processed: ${results.successful}`);
  log(`Failed to process: ${results.failed}`);
  
  if (results.failed > 0) {
    log(`Failed dates: ${checkpoint.failedDates.join(', ')}`);
  }
}

/**
 * Process a specific date
 */
async function processSingleDate(date: string): Promise<void> {
  log(`\n===== PROCESSING DATE: ${date} =====`);
  
  resetCheckpoint('date', [date]);
  
  // Process the date
  const success = await processDate(date);
  
  // Log summary
  log(`\n===== DATE ${date} RECONCILIATION COMPLETE =====`);
  log(`Status: ${success ? 'Success' : 'Failed'}`);
}

/**
 * Process a range of dates
 */
async function processDateRange(startDate: string, endDate: string): Promise<void> {
  log(`\n===== PROCESSING DATE RANGE: ${startDate} to ${endDate} =====`);
  
  resetCheckpoint('range', [startDate, endDate]);
  
  // Get all dates in the range with curtailment records
  const dates = await getDateRange(startDate, endDate);
  
  if (dates.length === 0) {
    log(`No dates found in range ${startDate} to ${endDate}`);
    return;
  }
  
  // Process the dates
  const results = await processBatch(dates);
  
  // Log summary
  log(`\n===== DATE RANGE ${startDate} to ${endDate} RECONCILIATION COMPLETE =====`);
  log(`Total dates processed: ${results.totalProcessed}`);
  log(`Successfully processed: ${results.successful}`);
  log(`Failed to process: ${results.failed}`);
  
  if (results.failed > 0) {
    log(`Failed dates: ${checkpoint.failedDates.join(', ')}`);
  }
}

/**
 * Process the top N dates with the most missing calculations
 */
async function processTopMissingDates(limit: number): Promise<void> {
  log(`\n===== PROCESSING TOP ${limit} DATES WITH MISSING CALCULATIONS =====`);
  
  resetCheckpoint('top', [limit.toString()]);
  
  // Get the top N dates with the most missing calculations
  const datesWithMissing = await getTopMissingDates(limit);
  
  if (datesWithMissing.length === 0) {
    log(`No dates found with missing calculations`);
    return;
  }
  
  const dates = datesWithMissing.map(d => d.date);
  
  // Process the dates
  const results = await processBatch(dates);
  
  // Log summary
  log(`\n===== TOP ${limit} DATES RECONCILIATION COMPLETE =====`);
  log(`Total dates processed: ${results.totalProcessed}`);
  log(`Successfully processed: ${results.successful}`);
  log(`Failed to process: ${results.failed}`);
  
  if (results.failed > 0) {
    log(`Failed dates: ${checkpoint.failedDates.join(', ')}`);
  }
}

/**
 * Resume from the last checkpoint
 */
async function resumeProcessing(): Promise<void> {
  if (!loadCheckpoint()) {
    log("No checkpoint found. Nothing to resume.");
    return;
  }
  
  log(`\n===== RESUMING PREVIOUS JOB =====`);
  log(`Last command: ${checkpoint.lastCommand} ${checkpoint.args.join(' ')}`);
  log(`Processed dates: ${checkpoint.processedDates.length}`);
  log(`Pending in batch: ${checkpoint.pendingBatch.length}`);
  log(`Failed dates: ${checkpoint.failedDates.length}`);
  
  if (checkpoint.pendingBatch.length === 0) {
    log("No pending dates to process. Job is complete.");
    return;
  }
  
  // Process the remaining dates in the batch
  const results = await processBatch(checkpoint.pendingBatch);
  
  // Log summary
  log(`\n===== RESUMED JOB COMPLETE =====`);
  log(`Total dates processed: ${results.totalProcessed}`);
  log(`Successfully processed: ${results.successful}`);
  log(`Failed to process: ${results.failed}`);
  
  if (results.failed > 0) {
    log(`Failed dates: ${checkpoint.failedDates.join(', ')}`);
  }
}

/**
 * Display reconciliation statistics
 */
async function showReconciliationStats(): Promise<void> {
  log("\n===== 2023 RECONCILIATION STATISTICS =====");
  
  // Get overall missing counts
  const result = await db.execute(sql`
    WITH curtailment_counts AS (
      SELECT 
        SUM(CASE WHEN settlement_date >= '2023-01-01' AND settlement_date <= '2023-12-31' THEN 1 ELSE 0 END) AS total_2023_records
      FROM curtailment_records
    ),
    calculation_counts AS (
      SELECT 
        miner_model,
        COUNT(*) AS model_count
      FROM historical_bitcoin_calculations
      WHERE settlement_date >= '2023-01-01' AND settlement_date <= '2023-12-31'
      GROUP BY miner_model
    ),
    date_counts AS (
      SELECT 
        COUNT(DISTINCT settlement_date) AS unique_dates
      FROM curtailment_records
      WHERE settlement_date >= '2023-01-01' AND settlement_date <= '2023-12-31'
    )
    SELECT 
      cc.total_2023_records,
      dc.unique_dates,
      COALESCE(SUM(CASE WHEN calc.miner_model = 'S19J_PRO' THEN calc.model_count ELSE 0 END), 0) AS s19j_pro_count,
      COALESCE(SUM(CASE WHEN calc.miner_model = 'S9' THEN calc.model_count ELSE 0 END), 0) AS s9_count,
      COALESCE(SUM(CASE WHEN calc.miner_model = 'M20S' THEN calc.model_count ELSE 0 END), 0) AS m20s_count
    FROM curtailment_counts cc, date_counts dc
    LEFT JOIN calculation_counts calc ON 1=1
    GROUP BY cc.total_2023_records, dc.unique_dates
  `);
  
  if (result.rows.length === 0) {
    log("No data available");
    return;
  }
  
  const data = result.rows[0];
  const totalCurtailmentRecords = parseInt(data.total_2023_records, 10);
  const uniqueDates = parseInt(data.unique_dates, 10);
  const s19jProCount = parseInt(data.s19j_pro_count, 10);
  const s9Count = parseInt(data.s9_count, 10);
  const m20sCount = parseInt(data.m20s_count, 10);
  
  const missingS19jPro = totalCurtailmentRecords - s19jProCount;
  const missingS9 = totalCurtailmentRecords - s9Count;
  const missingM20s = totalCurtailmentRecords - m20sCount;
  const totalMissing = missingS19jPro + missingS9 + missingM20s;
  
  const completionS19jPro = (s19jProCount / totalCurtailmentRecords * 100).toFixed(2);
  const completionS9 = (s9Count / totalCurtailmentRecords * 100).toFixed(2);
  const completionM20s = (m20sCount / totalCurtailmentRecords * 100).toFixed(2);
  const completionOverall = ((s19jProCount + s9Count + m20sCount) / (totalCurtailmentRecords * 3) * 100).toFixed(2);
  
  log("Overall Statistics:");
  log(`  Total curtailment records in 2023: ${totalCurtailmentRecords}`);
  log(`  Total unique dates in 2023: ${uniqueDates}`);
  log("");
  log("Bitcoin calculations by model:");
  log(`  S19J_PRO: ${s19jProCount} (${completionS19jPro}% complete)`);
  log(`  S9: ${s9Count} (${completionS9}% complete)`);
  log(`  M20S: ${m20sCount} (${completionM20s}% complete)`);
  log("");
  log("Missing calculations by model:");
  log(`  S19J_PRO: ${missingS19jPro}`);
  log(`  S9: ${missingS9}`);
  log(`  M20S: ${missingM20s}`);
  log(`  Total missing: ${totalMissing}`);
  log("");
  log(`Overall completion: ${completionOverall}%`);
  
  // Get top 5 dates with the most missing calculations
  const topMissingDates = await getTopMissingDates(5);
  
  if (topMissingDates.length > 0) {
    log("\nTop 5 dates with the most missing calculations:");
    topMissingDates.forEach((d, i) => {
      log(`  ${i + 1}. ${d.date}: ${d.missingCount} missing`);
    });
  }
  
  // Check progress file for already processed dates
  const processedDates = getProcessedDates();
  log(`\nDates processed so far: ${processedDates.length}`);
}

/**
 * Main function to handle command line arguments
 */
async function main(): Promise<void> {
  const args = process.argv.slice(2);
  
  // Initialize the progress file
  initProgressFile();
  
  if (args.length === 0) {
    showHelp();
    return;
  }
  
  const command = args[0];
  
  try {
    switch (command) {
      case 'help':
        showHelp();
        break;
      
      case 'month':
        if (args.length < 2) {
          log("Missing month argument. Use format YYYY-MM (e.g., 2023-01)");
          break;
        }
        await processMonth(args[1]);
        break;
      
      case 'date':
        if (args.length < 2) {
          log("Missing date argument. Use format YYYY-MM-DD (e.g., 2023-01-15)");
          break;
        }
        await processSingleDate(args[1]);
        break;
      
      case 'range':
        if (args.length < 3) {
          log("Missing date range. Use format: range START_DATE END_DATE");
          break;
        }
        await processDateRange(args[1], args[2]);
        break;
      
      case 'top':
        if (args.length < 2) {
          log("Missing limit argument. Use format: top N (e.g., top 10)");
          break;
        }
        const limit = parseInt(args[1], 10);
        if (isNaN(limit) || limit <= 0) {
          log("Invalid limit. Must be a positive number.");
          break;
        }
        await processTopMissingDates(limit);
        break;
      
      case 'resume':
        await resumeProcessing();
        break;
      
      case 'stats':
        await showReconciliationStats();
        break;
      
      default:
        log(`Unknown command: ${command}`);
        showHelp();
    }
  } catch (error) {
    log(`Error executing command: ${error}`);
  }
}

// Run the main function
main()
  .catch(error => {
    log(`Unhandled error: ${error}`);
    process.exit(1);
  })
  .finally(() => {
    log("Execution completed");
  });