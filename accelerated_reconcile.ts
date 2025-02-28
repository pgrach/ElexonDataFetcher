/**
 * Accelerated Reconciliation System
 * 
 * This high-performance tool reconciles curtailment_records with historical_bitcoin_calculations
 * in hours instead of days using massively parallel processing and optimized database operations.
 */

import { db } from "./db";
import { sql } from "drizzle-orm";
import { reconcileDay } from "./server/services/historicalReconciliation";
import os from "os";
import cluster from "cluster";
import { format, parse } from "date-fns";
import { createHash } from "crypto";

// PERFORMANCE SETTINGS
const MAX_WORKER_COUNT = Math.max(2, Math.min(16, os.cpus().length * 2)); // 2x CPU cores up to 16 max
const CHUNK_SIZE = 5; // Dates processed per worker assignment
const DB_MAX_CONNECTIONS = 20; // Adjust based on DB capabilities
const DB_STATEMENT_TIMEOUT = 60000; // 60 seconds max per SQL query
const USE_BULK_OPERATIONS = true;
const ENABLE_MEMORY_MONITOR = true;
const MEMORY_THRESHOLD = 0.85; // 85% memory usage triggers throttling

interface WorkerMessage {
  type: 'done' | 'failed' | 'status' | 'memory-check';
  dates?: string[];
  failedDates?: string[];
  metrics?: {
    processedCount: number;
    successCount: number;
    duration: number;
  };
  memoryUsage?: number;
}

// Main state for master process
interface ReconciliationState {
  pendingDates: string[];
  inProgressDates: Map<string, string[]>; // workerId -> dates
  completedDates: string[];
  failedDates: {date: string, reason: string}[];
  workers: Map<string, any>;
  startTime: Date;
  endTime?: Date;
  metrics: {
    totalDates: number;
    processedCount: number;
    datesPer10Seconds: number[];
    memoryUsage: number[];
  }
}

// Initialize state for tracking
const state: ReconciliationState = {
  pendingDates: [],
  inProgressDates: new Map(),
  completedDates: [],
  failedDates: [],
  workers: new Map(),
  startTime: new Date(),
  metrics: {
    totalDates: 0,
    processedCount: 0,
    datesPer10Seconds: [],
    memoryUsage: []
  }
};

// Helper function to measure time
function formatDuration(ms: number): string {
  const seconds = Math.floor((ms / 1000) % 60);
  const minutes = Math.floor((ms / (1000 * 60)) % 60);
  const hours = Math.floor(ms / (1000 * 60 * 60));
  return `${hours}h ${minutes}m ${seconds}s`;
}

/**
 * Setup optimized database for reconciliation
 */
async function optimizeDatabaseForReconciliation(): Promise<void> {
  console.log("Optimizing database for accelerated reconciliation...");
  
  try {
    // Add temporary indices for faster joins
    await db.execute(sql`
      CREATE INDEX IF NOT EXISTS temp_curtailment_date_idx 
      ON curtailment_records(settlement_date);
      
      CREATE INDEX IF NOT EXISTS temp_curtailment_combo_idx 
      ON curtailment_records(settlement_date, settlement_period, farm_id);
      
      CREATE INDEX IF NOT EXISTS temp_historical_combo_idx 
      ON historical_bitcoin_calculations(settlement_date, settlement_period, farm_id, miner_model);
    `);
    
    // Update database statistics for optimal query planning
    await db.execute(sql`ANALYZE curtailment_records`);
    await db.execute(sql`ANALYZE historical_bitcoin_calculations`);
    
    // Set statement timeout to prevent long-running queries
    await db.execute(sql`SET statement_timeout = ${DB_STATEMENT_TIMEOUT}`);
    
    console.log("Database optimization complete.");
  } catch (error) {
    console.error("Failed to optimize database:", error);
    throw error;
  }
}

/**
 * Remove temporary database optimizations when complete
 */
async function cleanupDatabaseOptimizations(): Promise<void> {
  console.log("Cleaning up temporary database optimizations...");
  
  try {
    // Drop temporary indices
    await db.execute(sql`
      DROP INDEX IF EXISTS temp_curtailment_date_idx;
      DROP INDEX IF EXISTS temp_curtailment_combo_idx;
      DROP INDEX IF EXISTS temp_historical_combo_idx;
    `);
    
    // Reset statement timeout
    await db.execute(sql`RESET statement_timeout`);
    
    console.log("Database cleanup complete.");
  } catch (error) {
    console.error("Failed to cleanup database optimizations:", error);
  }
}

/**
 * Get all dates from curtailment_records that need reconciliation
 */
async function getAllDatesToProcess(): Promise<string[]> {
  console.log("Finding all dates needing reconciliation...");
  
  try {
    // Efficient query to find dates with missing calculations
    const result = await db.execute(sql`
      WITH date_combinations AS (
        SELECT 
          settlement_date,
          COUNT(DISTINCT (settlement_period, farm_id)) AS combinations
        FROM curtailment_records
        GROUP BY settlement_date
      ),
      date_calculations AS (
        SELECT 
          settlement_date,
          COUNT(*) / 3 AS calculations
        FROM historical_bitcoin_calculations
        GROUP BY settlement_date
      )
      SELECT 
        dc.settlement_date
      FROM date_combinations dc
      LEFT JOIN date_calculations calcs ON dc.settlement_date = calcs.settlement_date
      WHERE COALESCE(calcs.calculations, 0) < dc.combinations
      ORDER BY dc.settlement_date DESC
    `);
    
    const dates = result.rows.map((row: any) => String(row.settlement_date));
    console.log(`Found ${dates.length} dates needing reconciliation.`);
    
    return dates;
  } catch (error) {
    console.error("Failed to get dates for processing:", error);
    return [];
  }
}

/**
 * Master process: Display progress dashboard
 */
function displayProgressDashboard(): void {
  const now = new Date();
  const elapsedMs = now.getTime() - state.startTime.getTime();
  const percentComplete = Math.round((state.completedDates.length / state.metrics.totalDates) * 100);
  
  const rate = state.completedDates.length / (elapsedMs / 1000);
  const estimatedTotalTime = state.metrics.totalDates / rate * 1000;
  const estimatedTimeRemaining = Math.max(0, estimatedTotalTime - elapsedMs);
  
  // Get current memory usage percentage
  const memUsage = process.memoryUsage();
  const memoryUsagePercent = Math.round((memUsage.heapUsed / memUsage.heapTotal) * 100);
  
  // Calculate current processing rate (last 10 seconds)
  const recentCount = state.metrics.datesPer10Seconds[state.metrics.datesPer10Seconds.length - 1] || 0;
  const recentRate = recentCount / 10; // dates per second
  
  console.clear();
  console.log("=".repeat(80));
  console.log(`ACCELERATED RECONCILIATION - PROGRESS DASHBOARD`);
  console.log("=".repeat(80));
  console.log(`Time Elapsed: ${formatDuration(elapsedMs)}`);
  console.log(`Est. Time Remaining: ${formatDuration(estimatedTimeRemaining)}`);
  console.log();
  console.log(`Progress: ${state.completedDates.length} / ${state.metrics.totalDates} dates (${percentComplete}%)`);
  console.log(`Processing Rate: ${rate.toFixed(2)} dates/sec (Current: ${recentRate.toFixed(2)} dates/sec)`);
  console.log(`Active Workers: ${state.workers.size} / ${MAX_WORKER_COUNT}`);
  console.log(`Memory Usage: ${memoryUsagePercent}%`);
  console.log();
  console.log(`Dates Pending: ${state.pendingDates.length}`);
  console.log(`Dates In Progress: ${Array.from(state.inProgressDates.values()).flat().length}`);
  console.log(`Dates Completed: ${state.completedDates.length}`);
  console.log(`Dates Failed: ${state.failedDates.length}`);
  console.log("=".repeat(80));
  
  if (state.failedDates.length > 0) {
    console.log("FAILED DATES (most recent first):");
    state.failedDates.slice(-5).reverse().forEach(({date, reason}) => {
      console.log(`- ${date}: ${reason.substring(0, 60)}${reason.length > 60 ? '...' : ''}`);
    });
    console.log("=".repeat(80));
  }
  
  // Update metrics
  state.metrics.memoryUsage.push(memoryUsagePercent);
}

/**
 * Measure memory usage and throttle if needed
 */
function checkAndThrottleBasedOnMemory(): boolean {
  if (!ENABLE_MEMORY_MONITOR) return false;
  
  const memUsage = process.memoryUsage();
  const memoryUsagePercent = memUsage.heapUsed / memUsage.heapTotal;
  
  if (memoryUsagePercent > MEMORY_THRESHOLD) {
    console.log(`⚠️ High memory usage (${Math.round(memoryUsagePercent * 100)}%) detected, throttling...`);
    return true;
  }
  
  return false;
}

/**
 * Create a worker to process dates
 */
function createWorker(workerId: string): void {
  if (cluster.isPrimary) {
    const worker = cluster.fork({ WORKER_ID: workerId });
    state.workers.set(workerId, worker);
    
    worker.on('message', (msg: WorkerMessage) => {
      if (msg.type === 'done') {
        // Mark dates as complete
        const dates = msg.dates || [];
        state.completedDates.push(...dates);
        state.inProgressDates.delete(workerId);
        state.metrics.processedCount += dates.length;
        
        // Reassign worker to more work if available
        assignWorkToWorker(workerId);
      } else if (msg.type === 'failed') {
        // Mark dates as failed and record failures
        const dates = msg.dates || [];
        const failedDates = msg.failedDates || [];
        
        // Add all unprocessed dates back to pending
        const unprocessedDates = dates.filter(d => !failedDates.includes(d));
        state.pendingDates.unshift(...unprocessedDates);
        
        // Record failures
        failedDates.forEach(date => {
          state.failedDates.push({date, reason: "Worker failed to process date"});
        });
        
        state.inProgressDates.delete(workerId);
        
        // Reassign worker to more work if available
        assignWorkToWorker(workerId);
      } else if (msg.type === 'memory-check') {
        // If worker reports high memory usage, we may need to throttle
        if (msg.memoryUsage && msg.memoryUsage > MEMORY_THRESHOLD) {
          console.log(`⚠️ Worker ${workerId} reports high memory usage (${Math.round(msg.memoryUsage * 100)}%)`);
        }
      }
    });
    
    worker.on('exit', () => {
      // Handle worker exit
      console.log(`Worker ${workerId} exited. Starting replacement...`);
      state.workers.delete(workerId);
      
      // Return any in-progress work to the queue
      const inProgressDates = state.inProgressDates.get(workerId) || [];
      if (inProgressDates.length > 0) {
        console.log(`Returning ${inProgressDates.length} dates to queue from failed worker ${workerId}`);
        state.pendingDates.unshift(...inProgressDates);
        state.inProgressDates.delete(workerId);
      }
      
      // Create a replacement worker
      const newWorkerId = createWorkerId();
      setTimeout(() => createWorker(newWorkerId), 1000);
    });
  }
}

/**
 * Generate a unique worker ID
 */
function createWorkerId(): string {
  return createHash('md5').update(`${Date.now()}-${Math.random()}`).digest('hex').substring(0, 8);
}

/**
 * Assign work to an available worker
 */
function assignWorkToWorker(workerId: string): void {
  if (state.pendingDates.length === 0) {
    // No more work left
    console.log(`No more dates to process for worker ${workerId}`);
    if (state.inProgressDates.size === 0) {
      // All work is done
      console.log("All dates processed. Shutting down workers...");
      state.endTime = new Date();
      
      // Terminate all workers
      for (const worker of state.workers.values()) {
        worker.kill();
      }
      
      // Display final summary
      displayFinalSummary();
    }
    return;
  }
  
  // Get a chunk of dates to process
  const datesToProcess = state.pendingDates.splice(0, CHUNK_SIZE);
  state.inProgressDates.set(workerId, datesToProcess);
  
  // Send the dates to the worker
  const worker = state.workers.get(workerId);
  if (worker) {
    worker.send({ cmd: 'process', dates: datesToProcess });
    console.log(`Assigned ${datesToProcess.length} dates to worker ${workerId}: ${datesToProcess.join(', ')}`);
  }
}

/**
 * Display final reconciliation summary
 */
function displayFinalSummary(): void {
  if (!state.endTime) {
    state.endTime = new Date();
  }
  
  const elapsedMs = state.endTime.getTime() - state.startTime.getTime();
  
  console.log("\n" + "=".repeat(80));
  console.log("ACCELERATED RECONCILIATION - FINAL SUMMARY");
  console.log("=".repeat(80));
  console.log(`Started: ${state.startTime.toISOString()}`);
  console.log(`Completed: ${state.endTime.toISOString()}`);
  console.log(`Total Duration: ${formatDuration(elapsedMs)}`);
  console.log();
  console.log(`Total Dates Processed: ${state.completedDates.length} of ${state.metrics.totalDates}`);
  console.log(`Average Processing Rate: ${(state.completedDates.length / (elapsedMs / 1000)).toFixed(2)} dates/sec`);
  console.log(`Failed Dates: ${state.failedDates.length}`);
  console.log("=".repeat(80));
  
  // Cleanup and exit
  setTimeout(() => {
    process.exit(0);
  }, 1000);
}

/**
 * Run the master process
 */
async function runMasterProcess(): Promise<void> {
  console.log(`Starting accelerated reconciliation with ${MAX_WORKER_COUNT} workers...`);
  
  try {
    // Optimize database for better performance
    await optimizeDatabaseForReconciliation();
    
    // Get all dates that need processing
    state.pendingDates = await getAllDatesToProcess();
    state.metrics.totalDates = state.pendingDates.length;
    
    if (state.pendingDates.length === 0) {
      console.log("No dates found requiring reconciliation. All data is already 100% reconciled.");
      return;
    }
    
    console.log(`Starting reconciliation of ${state.metrics.totalDates} dates...`);
    
    // Setup metrics tracking
    setInterval(() => {
      // Track processing rate per 10 seconds
      const last10Seconds = state.metrics.processedCount;
      setTimeout(() => {
        const current = state.metrics.processedCount;
        const processed = current - last10Seconds;
        state.metrics.datesPer10Seconds.push(processed);
        
        // Keep only the last 6 measurements (last minute)
        if (state.metrics.datesPer10Seconds.length > 6) {
          state.metrics.datesPer10Seconds.shift();
        }
      }, 10000);
    }, 10000);
    
    // Update progress dashboard periodically
    setInterval(() => {
      displayProgressDashboard();
    }, 1000);
    
    // Start workers
    for (let i = 0; i < MAX_WORKER_COUNT; i++) {
      const workerId = createWorkerId();
      createWorker(workerId);
    }
    
    // Wait for all workers to initialize
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    // Assign initial work to all workers
    for (const workerId of state.workers.keys()) {
      assignWorkToWorker(workerId);
    }
  } catch (error) {
    console.error("Critical error in master process:", error);
    
    // Cleanup and exit
    await cleanupDatabaseOptimizations();
    process.exit(1);
  }
}

/**
 * Worker process: Process a batch of dates
 */
async function processDates(dates: string[]): Promise<{ 
  processedDates: string[], 
  failedDates: string[],
  metrics: { processedCount: number, successCount: number, duration: number }
}> {
  const startTime = Date.now();
  const failedDates: string[] = [];
  const processedDates: string[] = [];
  
  // Process each date
  for (const date of dates) {
    try {
      await reconcileDay(date);
      processedDates.push(date);
    } catch (error) {
      console.error(`Worker failed to process date ${date}:`, error);
      failedDates.push(date);
    }
    
    // Check memory usage and report back to master
    if (ENABLE_MEMORY_MONITOR) {
      const memUsage = process.memoryUsage();
      const memoryUsagePercent = memUsage.heapUsed / memUsage.heapTotal;
      if (memoryUsagePercent > MEMORY_THRESHOLD * 0.8) { // 80% of threshold as early warning
        if (process.send) {
          process.send({ 
            type: 'memory-check', 
            memoryUsage: memoryUsagePercent 
          } as WorkerMessage);
        }
      }
    }
  }
  
  const endTime = Date.now();
  
  return {
    processedDates,
    failedDates,
    metrics: {
      processedCount: dates.length,
      successCount: processedDates.length,
      duration: endTime - startTime
    }
  };
}

/**
 * Run the worker process
 */
async function runWorkerProcess(): Promise<void> {
  const workerId = process.env.WORKER_ID || createWorkerId();
  console.log(`Worker ${workerId} started.`);
  
  // Listen for commands from master
  process.on('message', async (msg: any) => {
    if (msg.cmd === 'process') {
      const dates = msg.dates || [];
      console.log(`Worker ${workerId} processing ${dates.length} dates: ${dates.join(', ')}`);
      
      try {
        const result = await processDates(dates);
        
        if (process.send) {
          if (result.failedDates.length > 0) {
            // Some dates failed
            process.send({ 
              type: 'failed', 
              dates,
              failedDates: result.failedDates,
              metrics: result.metrics
            } as WorkerMessage);
          } else {
            // All dates successful
            process.send({ 
              type: 'done', 
              dates,
              metrics: result.metrics
            } as WorkerMessage);
          }
        }
      } catch (error) {
        console.error(`Worker ${workerId} encountered an error:`, error);
        
        if (process.send) {
          process.send({ 
            type: 'failed', 
            dates,
            failedDates: dates,
            metrics: {
              processedCount: dates.length,
              successCount: 0,
              duration: 0
            }
          } as WorkerMessage);
        }
      }
    }
  });
  
  // Send ready signal
  if (process.send) {
    process.send({ type: 'status', status: 'ready' });
  }
}

/**
 * Main entry point
 */
async function main(): Promise<void> {
  if (cluster.isPrimary) {
    // Master process
    await runMasterProcess();
  } else {
    // Worker process
    await runWorkerProcess();
  }
}

// Start the application
if (require.main === module) {
  main().catch(console.error);
}