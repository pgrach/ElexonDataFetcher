/**
 * Efficient Reconciliation System
 * 
 * A highly optimized reconciliation tool that ensures 100% alignment between
 * curtailment_records and historical_bitcoin_calculations tables while avoiding timeouts.
 * 
 * Features:
 * - Batch processing with adjustable size
 * - Connection pool management to prevent timeouts
 * - Checkpoint-based processing for resumability
 * - Comprehensive logging and timeout detection
 * - Auto-retry mechanism with exponential backoff
 * 
 * Usage:
 * npx tsx efficient_reconciliation.ts [command] [options]
 * 
 * Commands:
 *   status                  - Show current reconciliation status
 *   analyze                 - Analyze and identify missing calculations
 *   reconcile [batch-size]  - Process all missing calculations with specified batch size
 *   date YYYY-MM-DD        - Process a specific date
 *   range YYYY-MM-DD YYYY-MM-DD [batch-size] - Process a date range
 *   monitor                 - Start a monitoring server to track reconciliation progress
 */

import { db } from "./db";
import { sql } from "drizzle-orm";
import { auditAndFixBitcoinCalculations } from "./server/services/historicalReconciliation";
import { processSingleDay } from "./server/services/bitcoinService";
import pg from 'pg';
import fs from 'fs';
import path from 'path';
import pLimit from 'p-limit';
import { format, parseISO, eachDayOfInterval } from 'date-fns';

// Constants
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];
const DEFAULT_BATCH_SIZE = 5;
const MAX_CONCURRENT_PROCESSES = 3;
const CONNECTION_CHECK_INTERVAL = 30000; // 30 seconds
const MAX_CONNECTION_IDLE_TIME = 60000; // 1 minute
const CHECKPOINT_FILE = './reconciliation_checkpoint.json';
const LOG_FILE = './reconciliation_log.txt';

// Pool for direct database access when needed
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10, // Maximum number of clients
  idleTimeoutMillis: MAX_CONNECTION_IDLE_TIME,
  connectionTimeoutMillis: 10000, // 10 seconds
  allowExitOnIdle: true
});

interface ReconciliationCheckpoint {
  lastProcessedDate: string;
  pendingDates: string[];
  completedDates: string[];
  startTime: number;
  lastUpdateTime: number;
  stats: {
    totalRecords: number;
    processedRecords: number;
    successfulRecords: number;
    failedRecords: number;
    timeouts: number;
  };
}

// Initialize checkpoint
let checkpoint: ReconciliationCheckpoint = {
  lastProcessedDate: '',
  pendingDates: [],
  completedDates: [],
  startTime: Date.now(),
  lastUpdateTime: Date.now(),
  stats: {
    totalRecords: 0,
    processedRecords: 0,
    successfulRecords: 0,
    failedRecords: 0,
    timeouts: 0
  }
};

/**
 * Log a message to console and log file
 */
function log(message: string, type: 'info' | 'error' | 'warning' | 'success' = 'info'): void {
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] [${type.toUpperCase()}] ${message}`;
  
  // Console output with colors
  switch (type) {
    case 'error':
      console.error('\x1b[31m%s\x1b[0m', formattedMessage);
      break;
    case 'warning':
      console.warn('\x1b[33m%s\x1b[0m', formattedMessage);
      break;
    case 'success':
      console.log('\x1b[32m%s\x1b[0m', formattedMessage);
      break;
    default:
      console.log('\x1b[36m%s\x1b[0m', formattedMessage);
  }
  
  // Append to log file
  fs.appendFileSync(LOG_FILE, formattedMessage + '\n');
}

/**
 * Save current checkpoint to file
 */
function saveCheckpoint(): void {
  checkpoint.lastUpdateTime = Date.now();
  fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(checkpoint, null, 2));
  log(`Checkpoint saved. Processed: ${checkpoint.stats.processedRecords}/${checkpoint.stats.totalRecords}`, 'info');
}

/**
 * Load checkpoint from file if exists
 */
function loadCheckpoint(): boolean {
  try {
    if (fs.existsSync(CHECKPOINT_FILE)) {
      const data = fs.readFileSync(CHECKPOINT_FILE, 'utf8');
      checkpoint = JSON.parse(data);
      log(`Checkpoint loaded. Resuming from ${checkpoint.lastProcessedDate}`, 'info');
      return true;
    }
  } catch (error) {
    log(`Error loading checkpoint: ${error}`, 'error');
  }
  return false;
}

/**
 * Reset checkpoint data
 */
function resetCheckpoint(): void {
  checkpoint = {
    lastProcessedDate: '',
    pendingDates: [],
    completedDates: [],
    startTime: Date.now(),
    lastUpdateTime: Date.now(),
    stats: {
      totalRecords: 0,
      processedRecords: 0,
      successfulRecords: 0,
      failedRecords: 0,
      timeouts: 0
    }
  };
  if (fs.existsSync(CHECKPOINT_FILE)) {
    fs.unlinkSync(CHECKPOINT_FILE);
  }
  log('Checkpoint reset', 'info');
}

/**
 * Check database connection health
 */
async function checkConnection(): Promise<boolean> {
  const client = await pool.connect();
  try {
    await client.query('SELECT 1');
    return true;
  } catch (error) {
    log(`Database connection check failed: ${error}`, 'error');
    return false;
  } finally {
    client.release();
  }
}

/**
 * Detect timeout from error message
 */
function isTimeoutError(error: any): boolean {
  const errorMessage = error?.message || error?.toString() || '';
  return errorMessage.includes('timeout') || 
         errorMessage.includes('Connection terminated') ||
         errorMessage.includes('Connection terminated unexpectedly');
}

/**
 * Get summary statistics about reconciliation status
 */
async function getReconciliationStatus() {
  log("Checking current reconciliation status...", 'info');

  // Get total curtailment records and unique date-period-farm combinations
  const curtailmentResult = await db.execute(sql`
    SELECT 
      COUNT(*) as total_records,
      COUNT(DISTINCT (settlement_date || '-' || settlement_period || '-' || farm_id)) as unique_combinations
    FROM curtailment_records
  `);
  
  const totalCurtailmentRecords = Number(curtailmentResult.rows[0].total_records);
  const uniqueCombinations = Number(curtailmentResult.rows[0].unique_combinations);
  
  // Get Bitcoin calculation counts by miner model
  const bitcoinCounts: Record<string, number> = {};
  
  for (const model of MINER_MODELS) {
    const result = await db.execute(sql`
      SELECT COUNT(*) as count
      FROM historical_bitcoin_calculations
      WHERE miner_model = ${model}
    `);
    
    bitcoinCounts[model] = Number(result.rows[0].count) || 0;
  }
  
  // Expected Bitcoin calculation count for 100% reconciliation
  // For each unique date-period-farm combination, we should have one calculation per miner model
  const expectedTotal = uniqueCombinations * MINER_MODELS.length;
  const actualTotal = Object.values(bitcoinCounts).reduce((sum, count) => sum + Number(count), 0);
  
  // Calculate reconciliation percentage with safety checks
  let reconciliationPercentage = 100;
  if (expectedTotal > 0) {
    reconciliationPercentage = Math.min((actualTotal / expectedTotal) * 100, 100);
  }
  
  const status = {
    totalCurtailmentRecords,
    uniqueDatePeriodFarmCombinations: uniqueCombinations,
    bitcoinCalculationsByModel: bitcoinCounts,
    totalBitcoinCalculations: actualTotal,
    expectedBitcoinCalculations: expectedTotal,
    missingCalculations: expectedTotal - actualTotal,
    reconciliationPercentage: Math.round(reconciliationPercentage * 100) / 100
  };
  
  // Print status
  log("=== Overall Reconciliation Status ===", 'info');
  log(`Curtailment Records: ${status.totalCurtailmentRecords}`, 'info');
  log(`Unique Period-Farm Combinations: ${status.uniqueDatePeriodFarmCombinations}`, 'info');
  log(`Bitcoin Calculations: ${status.totalBitcoinCalculations}`, 'info');
  log(`Expected Calculations: ${status.expectedBitcoinCalculations}`, 'info');
  log(`Missing Calculations: ${status.missingCalculations}`, 'info');
  log(`Reconciliation: ${status.reconciliationPercentage}%`, status.reconciliationPercentage === 100 ? 'success' : 'warning');
  
  log("Bitcoin Calculations by Model:", 'info');
  for (const [model, count] of Object.entries(status.bitcoinCalculationsByModel)) {
    log(`- ${model}: ${count}`, 'info');
  }
  
  return status;
}

/**
 * Find dates with missing Bitcoin calculations
 */
async function findDatesWithMissingCalculations(limit: number = 100) {
  log("Finding dates with missing calculations...", 'info');
  
  const result = await db.execute(sql`
    WITH dates_with_curtailment AS (
      SELECT DISTINCT settlement_date
      FROM curtailment_records
      ORDER BY settlement_date DESC
    ),
    unique_date_combos AS (
      SELECT 
        settlement_date,
        COUNT(DISTINCT (settlement_period || '-' || farm_id)) as unique_combinations
      FROM curtailment_records
      GROUP BY settlement_date
    ),
    date_calculations AS (
      SELECT 
        c.settlement_date,
        COUNT(DISTINCT b.id) as calculation_count,
        u.unique_combinations * ${MINER_MODELS.length} as expected_count
      FROM dates_with_curtailment c
      JOIN unique_date_combos u ON c.settlement_date = u.settlement_date
      LEFT JOIN historical_bitcoin_calculations b 
        ON c.settlement_date = b.settlement_date
      GROUP BY c.settlement_date, u.unique_combinations
    )
    SELECT 
      settlement_date::text as date,
      calculation_count,
      expected_count,
      ROUND((calculation_count * 100.0) / expected_count, 2) as completion_percentage
    FROM date_calculations
    WHERE calculation_count < expected_count
    ORDER BY completion_percentage ASC, settlement_date DESC
    LIMIT ${limit}
  `);
  
  const missingDates = result.rows.map(row => ({
    date: String(row.date),
    actual: Number(row.calculation_count),
    expected: Number(row.expected_count),
    completionPercentage: Number(row.completion_percentage)
  }));
  
  if (missingDates.length === 0) {
    log("No dates with missing calculations found!", 'success');
    return [];
  }
  
  log(`Found ${missingDates.length} dates with missing calculations:`, 'warning');
  for (let i = 0; i < Math.min(10, missingDates.length); i++) {
    const d = missingDates[i];
    log(`- ${d.date}: ${d.actual}/${d.expected} (${d.completionPercentage}%)`, 'warning');
  }
  
  if (missingDates.length > 10) {
    log(`... and ${missingDates.length - 10} more`, 'warning');
  }
  
  return missingDates;
}

/**
 * Process a specific date with retry logic
 */
async function processDate(date: string, attemptNumber: number = 1): Promise<{success: boolean, message: string}> {
  const maxRetries = 3;
  const backoffMs = attemptNumber > 1 ? Math.pow(2, attemptNumber - 1) * 1000 : 0;
  
  try {
    log(`Processing date ${date} (Attempt ${attemptNumber}/${maxRetries})`, 'info');
    
    if (backoffMs > 0) {
      log(`Backing off for ${backoffMs}ms before retry`, 'info');
      await new Promise(resolve => setTimeout(resolve, backoffMs));
    }
    
    // Check connection before processing
    const connectionOk = await checkConnection();
    if (!connectionOk) {
      log(`Database connection unavailable for ${date}, will retry`, 'error');
      if (attemptNumber < maxRetries) {
        return processDate(date, attemptNumber + 1);
      } else {
        return { success: false, message: 'Database connection unavailable after maximum retries' };
      }
    }
    
    // Perform the reconciliation
    const result = await auditAndFixBitcoinCalculations(date);
    
    if (result.success) {
      log(`Successfully processed ${date}: ${result.message}`, 'success');
      return { success: true, message: result.message };
    } else {
      log(`Failed to process ${date}: ${result.message}`, 'error');
      if (attemptNumber < maxRetries) {
        return processDate(date, attemptNumber + 1);
      } else {
        return { success: false, message: `Failed after ${maxRetries} attempts: ${result.message}` };
      }
    }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    log(`Error processing ${date}: ${errorMessage}`, 'error');
    
    // Check for timeout errors specifically
    if (isTimeoutError(error)) {
      checkpoint.stats.timeouts++;
      log(`Detected timeout while processing ${date}`, 'error');
      saveCheckpoint();
      
      if (attemptNumber < maxRetries) {
        return processDate(date, attemptNumber + 1);
      } else {
        return { success: false, message: `Timeout occurred after ${maxRetries} attempts` };
      }
    } else if (attemptNumber < maxRetries) {
      return processDate(date, attemptNumber + 1);
    } else {
      return { success: false, message: `Error after ${maxRetries} attempts: ${errorMessage}` };
    }
  }
}

/**
 * Process multiple dates in batches with concurrency control
 */
async function processDates(dates: string[], batchSize: number = DEFAULT_BATCH_SIZE): Promise<{
  totalDates: number;
  successful: number;
  failed: number;
  skipped: number;
  timeouts: number;
}> {
  log(`Processing ${dates.length} dates in batches of ${batchSize}`, 'info');
  
  // Initialize checkpoint if not loaded
  if (checkpoint.pendingDates.length === 0) {
    checkpoint.pendingDates = [...dates];
    checkpoint.completedDates = [];
    checkpoint.stats.totalRecords = dates.length;
    saveCheckpoint();
  }
  
  const results = {
    totalDates: dates.length,
    successful: 0,
    failed: 0,
    skipped: 0,
    timeouts: 0
  };
  
  // Create a concurrent limit for processing
  const limit = pLimit(MAX_CONCURRENT_PROCESSES);
  
  // Process in batches
  for (let i = 0; i < dates.length; i += batchSize) {
    const batch = dates.slice(i, i + batchSize);
    log(`Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(dates.length / batchSize)}`, 'info');
    
    const batchPromises = batch.map(date => {
      return limit(async () => {
        // Skip already completed dates
        if (checkpoint.completedDates.includes(date)) {
          log(`Skipping already processed date: ${date}`, 'info');
          results.skipped++;
          return { date, success: true, skipped: true };
        }
        
        try {
          checkpoint.lastProcessedDate = date;
          const result = await processDate(date);
          
          checkpoint.stats.processedRecords++;
          if (result.success) {
            checkpoint.stats.successfulRecords++;
            checkpoint.completedDates.push(date);
            checkpoint.pendingDates = checkpoint.pendingDates.filter(d => d !== date);
            results.successful++;
          } else {
            checkpoint.stats.failedRecords++;
            results.failed++;
          }
          
          saveCheckpoint();
          return { date, success: result.success, message: result.message };
        } catch (error) {
          checkpoint.stats.failedRecords++;
          results.failed++;
          
          const errorMessage = error instanceof Error ? error.message : String(error);
          if (isTimeoutError(error)) {
            checkpoint.stats.timeouts++;
            results.timeouts++;
          }
          
          saveCheckpoint();
          return { date, success: false, error: errorMessage };
        }
      });
    });
    
    // Wait for batch to complete
    const batchResults = await Promise.all(batchPromises);
    
    // Log batch results
    const successful = batchResults.filter(r => r.success).length;
    const failed = batchResults.filter(r => !r.success).length;
    log(`Batch completed: ${successful} successful, ${failed} failed`, successful === batch.length ? 'success' : 'warning');
    
    // Check connection before continuing to next batch
    await checkConnection();
  }
  
  // Final reporting
  log("=== Reconciliation Process Complete ===", 'info');
  log(`Total Dates: ${results.totalDates}`, 'info');
  log(`Successfully Processed: ${results.successful}`, results.successful > 0 ? 'success' : 'info');
  log(`Failed: ${results.failed}`, results.failed > 0 ? 'error' : 'info');
  log(`Skipped (Already Processed): ${results.skipped}`, 'info');
  log(`Timeouts Encountered: ${results.timeouts}`, results.timeouts > 0 ? 'error' : 'info');
  
  return results;
}

/**
 * Process a date range
 */
async function processDateRange(startDate: string, endDate: string, batchSize: number = DEFAULT_BATCH_SIZE): Promise<void> {
  log(`Processing date range from ${startDate} to ${endDate}`, 'info');
  
  // Generate dates in range
  const dateArray = eachDayOfInterval({
    start: parseISO(startDate),
    end: parseISO(endDate),
  }).map(date => format(date, 'yyyy-MM-dd'));
  
  log(`Range contains ${dateArray.length} dates`, 'info');
  
  // Process the dates
  await processDates(dateArray, batchSize);
}

/**
 * Analyze missing calculations and provide recommendations
 */
async function analyzeReconciliationStatus(): Promise<void> {
  log("=== Detailed Reconciliation Analysis ===", 'info');
  
  // Get overall status
  const status = await getReconciliationStatus();
  
  // If already at 100%, no need to continue
  if (status.reconciliationPercentage === 100) {
    log("✅ Tables are fully reconciled. No action needed.", 'success');
    return;
  }
  
  // Get missing dates
  const missingDates = await findDatesWithMissingCalculations(50);
  if (missingDates.length === 0) {
    return;
  }
  
  // Check for patterns in missing dates
  const datesByMonth: Record<string, number> = {};
  let earliestDate = missingDates[0].date;
  let latestDate = missingDates[0].date;
  
  for (const dateInfo of missingDates) {
    const date = dateInfo.date;
    
    // Track by month
    const yearMonth = date.substring(0, 7); // YYYY-MM
    datesByMonth[yearMonth] = (datesByMonth[yearMonth] || 0) + 1;
    
    // Track earliest and latest dates
    if (date < earliestDate) earliestDate = date;
    if (date > latestDate) latestDate = date;
  }
  
  log("\n=== Analysis Results ===", 'info');
  log(`Missing calculations: ${status.missingCalculations} (${(100 - status.reconciliationPercentage).toFixed(2)}%)`, 'warning');
  log(`Date range with issues: ${earliestDate} to ${latestDate}`, 'info');
  
  log("\nMissing calculations by month:", 'info');
  const sortedMonths = Object.keys(datesByMonth).sort();
  for (const month of sortedMonths) {
    log(`- ${month}: ${datesByMonth[month]} dates with missing calculations`, 'info');
  }
  
  // Provide recommendations
  log("\n=== Recommendations ===", 'info');
  
  // If many dates are missing, suggest batch processing
  if (missingDates.length > 10) {
    log("1. Use batch processing for better performance:", 'info');
    log(`   npx tsx efficient_reconciliation.ts reconcile ${Math.min(5, missingDates.length)}`, 'info');
  }
  
  // If specific months have more issues
  const problematicMonths = Object.entries(datesByMonth)
    .filter(([_, count]) => count > 5)
    .map(([month, _]) => month);
  
  if (problematicMonths.length > 0) {
    log(`2. Focus on problematic months:`, 'info');
    for (const month of problematicMonths.slice(0, 3)) {
      const [year, monthNum] = month.split('-');
      log(`   npx tsx efficient_reconciliation.ts range ${month}-01 ${month}-${new Date(parseInt(year), parseInt(monthNum), 0).getDate()}`, 'info');
    }
  }
  
  // Suggest monitoring and fine-tuning
  log("3. Use monitoring to track progress:", 'info');
  log("   npx tsx efficient_reconciliation.ts monitor", 'info');
  
  log("\nNote: If you encounter repeated timeouts, try smaller batch sizes", 'warning');
}

/**
 * Main function to handle command line arguments
 */
async function main() {
  try {
    const command = process.argv[2]?.toLowerCase();
    const param1 = process.argv[3];
    const param2 = process.argv[4];
    const param3 = process.argv[5];
    
    log(`Starting efficient reconciliation tool with command: ${command || 'status'}`, 'info');
    
    // Check if previous checkpoint exists
    const hasCheckpoint = loadCheckpoint();
    if (hasCheckpoint && command !== 'reset' && command !== 'monitor') {
      log(`Found existing checkpoint. Run with 'reset' to start fresh, or 'resume' to continue.`, 'warning');
    }
    
    switch (command) {
      case "status":
        await getReconciliationStatus();
        break;
        
      case "analyze":
        await analyzeReconciliationStatus();
        break;
        
      case "reconcile":
        const batchSize = param1 ? parseInt(param1) : DEFAULT_BATCH_SIZE;
        const missingDates = await findDatesWithMissingCalculations(1000);
        if (missingDates.length === 0) {
          log("No missing calculations found. Tables are fully reconciled.", 'success');
          break;
        }
        await processDates(missingDates.map(d => d.date), batchSize);
        await getReconciliationStatus();
        break;
        
      case "date":
        if (!param1 || !/^\d{4}-\d{2}-\d{2}$/.test(param1)) {
          log("Error: Invalid date format. Use YYYY-MM-DD", 'error');
          break;
        }
        await processDate(param1);
        break;
        
      case "range":
        if (!param1 || !param2 || !/^\d{4}-\d{2}-\d{2}$/.test(param1) || !/^\d{4}-\d{2}-\d{2}$/.test(param2)) {
          log("Error: Invalid date range. Use: range YYYY-MM-DD YYYY-MM-DD [batch-size]", 'error');
          break;
        }
        const rangeBatchSize = param3 ? parseInt(param3) : DEFAULT_BATCH_SIZE;
        await processDateRange(param1, param2, rangeBatchSize);
        break;
        
      case "reset":
        resetCheckpoint();
        log("Checkpoint and progress data reset successfully", 'success');
        break;
        
      case "resume":
        if (!hasCheckpoint) {
          log("No checkpoint found to resume", 'error');
          break;
        }
        log(`Resuming from checkpoint. ${checkpoint.completedDates.length}/${checkpoint.stats.totalRecords} dates processed`, 'info');
        const pendingDates = [...checkpoint.pendingDates];
        await processDates(pendingDates, DEFAULT_BATCH_SIZE);
        break;
        
      case "monitor":
        log("Monitoring reconciliation status...", 'info');
        
        // Initial status
        await getReconciliationStatus();
        
        // Setup periodic status check
        setInterval(async () => {
          try {
            await getReconciliationStatus();
          } catch (error) {
            log(`Error during status check: ${error}`, 'error');
          }
        }, 600000); // Check every 10 minutes
        
        // Keep process alive
        log("Monitoring active. Press Ctrl+C to stop.", 'info');
        process.stdin.resume();
        break;
        
      default:
        log("Efficient Reconciliation Tool", 'info');
        log("\nCommands:", 'info');
        log("  status                       - Show reconciliation status", 'info');
        log("  analyze                      - Analyze and identify missing calculations", 'info');
        log("  reconcile [batch-size]       - Process all missing calculations", 'info');
        log("  date YYYY-MM-DD              - Process a specific date", 'info');
        log("  range YYYY-MM-DD YYYY-MM-DD [batch-size] - Process a date range", 'info');
        log("  reset                        - Reset checkpoint data", 'info');
        log("  resume                       - Resume from last checkpoint", 'info');
        log("  monitor                      - Start monitoring service", 'info');
        log("\nExample: npx tsx efficient_reconciliation.ts reconcile 5", 'info');
        
        // Default behavior - show status
        await getReconciliationStatus();
    }
  } catch (error) {
    log(`Fatal error: ${error}`, 'error');
    throw error;
  } finally {
    // Clean up pool if not monitoring
    if (process.argv[2]?.toLowerCase() !== 'monitor') {
      await pool.end();
      log("Database connection pool closed", 'info');
    }
  }
}

// Run the main function if script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main()
    .then(() => {
      log("\n=== Efficient Reconciliation Tool Complete ===", 'success');
      if (process.argv[2]?.toLowerCase() !== 'monitor') {
        process.exit(0);
      }
    })
    .catch(error => {
      log(`Fatal error: ${error}`, 'error');
      process.exit(1);
    });
}

export { 
  getReconciliationStatus, 
  findDatesWithMissingCalculations, 
  processDate,
  processDates,
  processDateRange,
  analyzeReconciliationStatus
};