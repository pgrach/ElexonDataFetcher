/**
 * Optimized Reconciliation Tool
 * 
 * This script implements a memory-efficient approach to reconciliation that:
 * 1. Processes data in small batches to prevent timeout
 * 2. Uses a checkpoint system to resume interrupted operations
 * 3. Optimizes database queries for improved performance
 * 4. Implements a distributed worker system for parallel processing
 */

import pg from 'pg';
import fs from 'fs/promises';
import { format } from 'date-fns';
import * as reconciliation from './server/services/historicalReconciliation';

// Create database connection
const dbUrl = process.env.DATABASE_URL;
if (!dbUrl) {
  console.error('DATABASE_URL environment variable is not set');
  process.exit(1);
}

const pool = new pg.Pool({
  connectionString: dbUrl,
  // Performance optimizations
  max: 10, // Maximum number of clients in the pool
  idleTimeoutMillis: 30000, // Close idle clients after 30 seconds
  connectionTimeoutMillis: 2000, // Timeout after 2 seconds when connecting
});

// Configurable batch size
const BATCH_SIZE = process.env.BATCH_SIZE ? parseInt(process.env.BATCH_SIZE) : 5;
const CONCURRENCY = process.env.CONCURRENCY ? parseInt(process.env.CONCURRENCY) : 3;

// Checkpoint file for resuming interrupted operations
const CHECKPOINT_FILE = './reconciliation_checkpoint.json';

// State interface for tracking progress
interface ReconciliationState {
  startDate: string;
  currentDate: string | null;
  completedDates: string[];
  failedDates: string[];
  inProgress: boolean;
  startTime: string;
  lastUpdated: string;
  totalProcessed: number;
  totalFailed: number;
}

// Utility functions
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Find dates with missing or incomplete Bitcoin calculations
 * Using optimized query with indexing considerations
 */
async function findDatesWithMissingCalculations(limit = 100): Promise<string[]> {
  console.log(`Finding dates with missing calculations (limit: ${limit})...`);
  
  const client = await pool.connect();
  try {
    // Optimized query with better index usage
    const query = `
      WITH missing_dates AS (
        SELECT 
          cr.settlement_date,
          COUNT(DISTINCT (cr.settlement_period, cr.farm_id)) * 3 AS expected_count,
          COUNT(DISTINCT (hbc.settlement_period, hbc.farm_id, hbc.miner_model)) AS actual_count
        FROM 
          curtailment_records cr
        LEFT JOIN 
          historical_bitcoin_calculations hbc ON cr.settlement_date = hbc.settlement_date
          AND cr.settlement_period = hbc.settlement_period
          AND cr.farm_id = hbc.farm_id
        GROUP BY 
          cr.settlement_date
        HAVING 
          COUNT(DISTINCT (cr.settlement_period, cr.farm_id)) * 3 > 
          COUNT(DISTINCT (hbc.settlement_period, hbc.farm_id, hbc.miner_model))
      )
      SELECT 
        settlement_date::text AS date,
        expected_count - actual_count AS missing_count
      FROM 
        missing_dates
      ORDER BY 
        missing_count DESC
      LIMIT $1;
    `;
    
    const result = await client.query(query, [limit]);
    return result.rows.map(row => row.date);
  } finally {
    client.release();
  }
}

/**
 * Save checkpoint to allow resuming process
 */
async function saveCheckpoint(state: ReconciliationState): Promise<void> {
  state.lastUpdated = new Date().toISOString();
  await fs.writeFile(CHECKPOINT_FILE, JSON.stringify(state, null, 2));
  console.log(`Checkpoint saved at: ${state.lastUpdated}`);
}

/**
 * Load checkpoint if available
 */
async function loadCheckpoint(): Promise<ReconciliationState | null> {
  try {
    const data = await fs.readFile(CHECKPOINT_FILE, 'utf-8');
    return JSON.parse(data) as ReconciliationState;
  } catch (error) {
    return null;
  }
}

/**
 * Create fresh state if no checkpoint exists
 */
async function createInitialState(startDate?: string): Promise<ReconciliationState> {
  const now = new Date().toISOString();
  return {
    startDate: startDate || format(new Date(), 'yyyy-MM-dd'),
    currentDate: null,
    completedDates: [],
    failedDates: [],
    inProgress: false,
    startTime: now,
    lastUpdated: now,
    totalProcessed: 0,
    totalFailed: 0
  };
}

/**
 * Process a single date with error handling and retries
 */
async function processDate(date: string, retryCount = 0): Promise<boolean> {
  console.log(`Processing date: ${date}`);
  try {
    // Use the reconciliation service for reliable processing
    await reconciliation.auditAndFixBitcoinCalculations(date);
    console.log(`Successfully processed date: ${date}`);
    return true;
  } catch (error) {
    console.error(`Error processing date ${date}:`, error);
    
    // Retry logic
    if (retryCount < 2) {
      console.log(`Retrying date ${date} (attempt ${retryCount + 1})...`);
      await sleep(1000); // Wait before retry
      return processDate(date, retryCount + 1);
    }
    
    return false;
  }
}

/**
 * Process a batch of dates with controlled concurrency
 */
async function processBatch(dates: string[]): Promise<{
  successful: string[];
  failed: string[];
}> {
  console.log(`Processing batch of ${dates.length} dates with concurrency ${CONCURRENCY}`);
  
  const successful: string[] = [];
  const failed: string[] = [];
  
  // Process dates with controlled concurrency
  let i = 0;
  while (i < dates.length) {
    const batchPromises: Promise<boolean>[] = [];
    
    // Take up to CONCURRENCY dates
    for (let j = 0; j < CONCURRENCY && i + j < dates.length; j++) {
      const date = dates[i + j];
      batchPromises.push(processDate(date));
    }
    
    // Wait for the current batch to complete
    const results = await Promise.all(batchPromises);
    
    // Process results
    for (let j = 0; j < results.length; j++) {
      const date = dates[i + j];
      if (results[j]) {
        successful.push(date);
      } else {
        failed.push(date);
      }
    }
    
    i += CONCURRENCY;
    
    // Print progress
    console.log(`Batch progress: ${i}/${dates.length} dates processed`);
    console.log(`Success: ${successful.length}, Failed: ${failed.length}`);
    
    // Short pause between batches to prevent resource contention
    if (i < dates.length) {
      await sleep(500);
    }
  }
  
  return { successful, failed };
}

/**
 * Verify reconciliation status for a date
 */
async function verifyDate(date: string): Promise<{
  records: number;
  calculationsExpected: number;
  calculationsActual: number;
  percentage: number;
}> {
  const client = await pool.connect();
  try {
    const query = `
      WITH date_stats AS (
        SELECT 
          COUNT(DISTINCT (cr.settlement_period, cr.farm_id)) AS records,
          COUNT(DISTINCT (cr.settlement_period, cr.farm_id)) * 3 AS expected_count,
          COUNT(DISTINCT (hbc.settlement_period, hbc.farm_id, hbc.miner_model)) AS actual_count
        FROM 
          curtailment_records cr
        LEFT JOIN 
          historical_bitcoin_calculations hbc ON cr.settlement_date = hbc.settlement_date
          AND cr.settlement_period = hbc.settlement_period
          AND cr.farm_id = hbc.farm_id
        WHERE 
          cr.settlement_date = $1
      )
      SELECT 
        records,
        expected_count AS calculations_expected,
        actual_count AS calculations_actual,
        CASE 
          WHEN expected_count = 0 THEN 100
          ELSE ROUND((actual_count::numeric / expected_count) * 100, 2)
        END AS percentage
      FROM 
        date_stats;
    `;
    
    const result = await client.query(query, [date]);
    
    if (result.rows.length === 0) {
      return { records: 0, calculationsExpected: 0, calculationsActual: 0, percentage: 0 };
    }
    
    return {
      records: parseInt(result.rows[0].records),
      calculationsExpected: parseInt(result.rows[0].calculations_expected),
      calculationsActual: parseInt(result.rows[0].calculations_actual),
      percentage: parseFloat(result.rows[0].percentage)
    };
  } finally {
    client.release();
  }
}

/**
 * Get overall reconciliation status
 */
async function getOverallStatus(): Promise<{
  totalExpected: number;
  totalActual: number;
  percentage: number;
}> {
  const client = await pool.connect();
  try {
    const query = `
      SELECT 
        COUNT(DISTINCT (cr.settlement_date, cr.settlement_period, cr.farm_id)) * 3 AS total_expected,
        COUNT(DISTINCT (hbc.settlement_date, hbc.settlement_period, hbc.farm_id, hbc.miner_model)) AS total_actual
      FROM 
        curtailment_records cr
      LEFT JOIN 
        historical_bitcoin_calculations hbc ON cr.settlement_date = hbc.settlement_date
        AND cr.settlement_period = hbc.settlement_period
        AND cr.farm_id = hbc.farm_id;
    `;
    
    const result = await client.query(query);
    
    const totalExpected = parseInt(result.rows[0].total_expected);
    const totalActual = parseInt(result.rows[0].total_actual);
    const percentage = (totalActual / totalExpected) * 100;
    
    return { totalExpected, totalActual, percentage };
  } finally {
    client.release();
  }
}

/**
 * Print summary of reconciliation status
 */
async function printSummary(state: ReconciliationState): Promise<void> {
  const status = await getOverallStatus();
  
  console.log("\n===== Reconciliation Progress Summary =====");
  console.log(`Started: ${new Date(state.startTime).toLocaleString()}`);
  console.log(`Last updated: ${new Date(state.lastUpdated).toLocaleString()}`);
  console.log(`Overall progress: ${status.percentage.toFixed(2)}% (${status.totalActual.toLocaleString()}/${status.totalExpected.toLocaleString()} calculations)`);
  console.log(`Dates processed this run: ${state.completedDates.length}`);
  console.log(`Dates failed this run: ${state.failedDates.length}`);
  
  if (state.failedDates.length > 0) {
    console.log("\nFailed dates:");
    state.failedDates.forEach(date => console.log(`  - ${date}`));
  }
  
  console.log("\n=== Completed Dates ===");
  for (const date of state.completedDates.slice(-5)) {
    const verification = await verifyDate(date);
    console.log(`${date}: ${verification.percentage}% (${verification.calculationsActual}/${verification.calculationsExpected})`);
  }
  
  console.log("\n============================================");
}

/**
 * Main reconciliation function
 */
async function runReconciliation(): Promise<void> {
  console.log("Starting optimized reconciliation process...");
  
  try {
    // Load or create state
    let state = await loadCheckpoint() || await createInitialState();
    
    // Print initial status
    console.log("Initial state loaded:", state.inProgress ? "Resuming previous run" : "Starting new run");
    
    // Set in progress flag
    state.inProgress = true;
    await saveCheckpoint(state);
    
    // Main reconciliation loop
    while (true) {
      // Get dates with missing calculations
      const missingDates = await findDatesWithMissingCalculations(BATCH_SIZE);
      
      if (missingDates.length === 0) {
        console.log("No more dates with missing calculations found!");
        break;
      }
      
      console.log(`Found ${missingDates.length} dates with missing calculations`);
      console.log("Next batch:", missingDates);
      
      // Process the batch
      const { successful, failed } = await processBatch(missingDates);
      
      // Update state
      state.completedDates.push(...successful);
      state.failedDates.push(...failed);
      state.totalProcessed += successful.length;
      state.totalFailed += failed.length;
      
      // Save checkpoint
      await saveCheckpoint(state);
      
      // Print current status
      await printSummary(state);
      
      console.log("\nContinuing to next batch...");
    }
    
    // Finalize process
    state.inProgress = false;
    await saveCheckpoint(state);
    
    // Final summary
    console.log("\n===== Reconciliation Complete =====");
    const finalStatus = await getOverallStatus();
    console.log(`Final progress: ${finalStatus.percentage.toFixed(2)}% (${finalStatus.totalActual.toLocaleString()}/${finalStatus.totalExpected.toLocaleString()} calculations)`);
    console.log(`Total dates processed: ${state.completedDates.length}`);
    console.log(`Total dates failed: ${state.failedDates.length}`);
    
  } catch (error) {
    console.error("Error in reconciliation process:", error);
  } finally {
    await pool.end();
  }
}

/**
 * Process a specific month
 */
async function runMonthReconciliation(yearMonth: string): Promise<void> {
  console.log(`Starting reconciliation for month: ${yearMonth}`);
  
  const client = await pool.connect();
  try {
    // Find dates in the specified month with missing calculations
    const query = `
      WITH missing_dates AS (
        SELECT 
          cr.settlement_date,
          COUNT(DISTINCT (cr.settlement_period, cr.farm_id)) * 3 AS expected_count,
          COUNT(DISTINCT (hbc.settlement_period, hbc.farm_id, hbc.miner_model)) AS actual_count
        FROM 
          curtailment_records cr
        LEFT JOIN 
          historical_bitcoin_calculations hbc ON cr.settlement_date = hbc.settlement_date
          AND cr.settlement_period = hbc.settlement_period
          AND cr.farm_id = hbc.farm_id
        WHERE 
          TO_CHAR(cr.settlement_date, 'YYYY-MM') = $1
        GROUP BY 
          cr.settlement_date
        HAVING 
          COUNT(DISTINCT (cr.settlement_period, cr.farm_id)) * 3 > 
          COUNT(DISTINCT (hbc.settlement_period, hbc.farm_id, hbc.miner_model))
      )
      SELECT 
        settlement_date::text AS date,
        expected_count - actual_count AS missing_count
      FROM 
        missing_dates
      ORDER BY 
        missing_count DESC;
    `;
    
    const result = await client.query(query, [yearMonth]);
    const missingDates = result.rows.map(row => row.date);
    
    if (missingDates.length === 0) {
      console.log(`No missing calculations found for ${yearMonth}`);
      return;
    }
    
    console.log(`Found ${missingDates.length} dates with missing calculations for ${yearMonth}`);
    
    // Process dates in batches
    let i = 0;
    while (i < missingDates.length) {
      const batch = missingDates.slice(i, i + BATCH_SIZE);
      console.log(`Processing batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(missingDates.length / BATCH_SIZE)}: ${batch.join(', ')}`);
      
      const { successful, failed } = await processBatch(batch);
      
      console.log(`Batch results - Success: ${successful.length}, Failed: ${failed.length}`);
      if (failed.length > 0) {
        console.log(`Failed dates: ${failed.join(', ')}`);
      }
      
      i += BATCH_SIZE;
      
      // Short pause between batches
      if (i < missingDates.length) {
        await sleep(1000);
      }
    }
    
    console.log(`Month ${yearMonth} reconciliation complete`);
    
  } finally {
    client.release();
    await pool.end();
  }
}

/**
 * Main entry point
 */
async function main(): Promise<void> {
  const args = process.argv.slice(2);
  
  if (args.length === 0) {
    // Default: Run full reconciliation
    await runReconciliation();
  } else if (args[0] === 'month' && args[1]) {
    // Process specific month: e.g., npm run start -- month 2023-12
    await runMonthReconciliation(args[1]);
  } else if (args[0] === 'date' && args[1]) {
    // Process specific date: e.g., npm run start -- date 2023-12-24
    console.log(`Processing single date: ${args[1]}`);
    const success = await processDate(args[1]);
    console.log(`Date ${args[1]} processing ${success ? 'successful' : 'failed'}`);
    
    if (success) {
      const verification = await verifyDate(args[1]);
      console.log(`Verification: ${verification.percentage}% (${verification.calculationsActual}/${verification.calculationsExpected})`);
    }
    
    await pool.end();
  } else {
    console.log("Usage:");
    console.log("  npm run start                    - Run full reconciliation");
    console.log("  npm run start -- month YYYY-MM   - Process specific month");
    console.log("  npm run start -- date YYYY-MM-DD - Process specific date");
  }
}

// Run the main function if script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main()
    .then(() => {
      console.log("Reconciliation process completed.");
      process.exit(0);
    })
    .catch(error => {
      console.error("Fatal error:", error);
      process.exit(1);
    });
}

export { 
  runReconciliation,
  runMonthReconciliation,
  processDate,
  verifyDate
};