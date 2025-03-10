/**
 * Enhanced Daily Reconciliation Check
 * 
 * This script automatically checks the reconciliation status for recent dates
 * and processes any missing calculations using the centralized reconciliation system.
 * It includes robust error handling, connection resilience, and comprehensive logging.
 * 
 * Usage:
 *   npx tsx daily_reconciliation_check.ts [days=2] [forceProcess=false]
 * 
 * Options:
 *   days - Number of recent days to check (default: 2)
 *   forceProcess - 'true' to force processing even if no issues found (default: false)
 */

import { format, subDays, parseISO } from "date-fns";
import pg from "pg";
import fs from "fs";
import path from "path";
import { 
  getReconciliationStatus,
  findDatesWithMissingCalculations,
  processDate,
  reconcileDay, 
  auditAndFixBitcoinCalculations 
} from "./server/services/historicalReconciliation";

// Configuration
const RECENT_DAYS_TO_CHECK = parseInt(process.argv[2] || "2", 10);
const FORCE_PROCESS = process.argv[3] === "true";
const MAX_RETRY_ATTEMPTS = 3;
const CHECKPOINT_FILE = "./daily_reconciliation_checkpoint.json";
const LOG_DIR = "./logs";
const LOG_FILE = `${LOG_DIR}/daily_reconciliation_${format(new Date(), "yyyy-MM-dd")}.log`;

// Ensure log directory exists
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

// Initiate logging
function log(message: string, level: "info" | "error" | "warning" | "success" = "info"): void {
  const timestamp = new Date().toISOString();
  const levelStr = level.toUpperCase();
  const formatted = `[${timestamp}] [${levelStr}] ${message}`;
  
  // Console output with colors
  let consoleMessage = formatted;
  if (level === "error") {
    consoleMessage = `\x1b[31m${formatted}\x1b[0m`; // Red
  } else if (level === "warning") {
    consoleMessage = `\x1b[33m${formatted}\x1b[0m`; // Yellow
  } else if (level === "success") {
    consoleMessage = `\x1b[32m${formatted}\x1b[0m`; // Green
  }
  
  try {
    console.log(consoleMessage);
  } catch (err) {
    // Handle stdout errors (like EPIPE)
  }
  
  // Log to file
  try {
    fs.appendFileSync(LOG_FILE, formatted + "\n");
  } catch (err) {
    // Try to report file write errors to console
    try {
      console.error(`Error writing to log file: ${err}`);
    } catch (_) {
      // Last resort silence
    }
  }
}

// Enhanced database connection with automatic retry logic
const dbUrl = process.env.DATABASE_URL;
if (!dbUrl) {
  log("DATABASE_URL environment variable is not set", "error");
  process.exit(1);
}

// Create database pool with reasonable limits
const pool = new pg.Pool({
  connectionString: dbUrl,
  max: 5,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
  query_timeout: 30000
});

// Add error handler to pool
pool.on("error", (err) => {
  log(`Database pool error: ${err.message}`, "error");
});

// Sleep utility
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Retry wrapper for database operations
async function withRetry<T>(
  operation: () => Promise<T>,
  description: string,
  maxAttempts: number = MAX_RETRY_ATTEMPTS,
  operationTimeout: number = 30000
): Promise<T> {
  let lastError: Error | null = null;
  
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      if (attempt > 1) {
        log(`Retry attempt ${attempt}/${maxAttempts} for: ${description}`, "info");
        
        // Clean up connections before retries
        if (description.includes("reconcile") || description.includes("fix")) {
          log("Cleaning up database connections before retry...", "info");
          await cleanupConnections().catch(e => log(`Warning: Connection cleanup failed: ${e}`, "warning"));
          await sleep(1000); // Give connections time to reset
        }
      }
      
      // Run with timeout protection
      return await Promise.race([
        operation(),
        new Promise<never>((_, reject) => {
          setTimeout(() => {
            reject(new Error(`Operation timed out after ${operationTimeout}ms: ${description}`));
          }, operationTimeout);
        })
      ]);
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      const errorMsg = lastError.message;
      
      // Log with different levels based on error type
      if (errorMsg.includes("timeout") || errorMsg.includes("ECONNRESET") || errorMsg.includes("deadlock")) {
        log(`Connection issue on attempt ${attempt}/${maxAttempts} for ${description}: ${errorMsg}`, "warning");
      } else {
        log(`Error on attempt ${attempt}/${maxAttempts} for ${description}: ${errorMsg}`, "error");
      }
      
      // Only retry if not the last attempt
      if (attempt < maxAttempts) {
        // Exponential backoff with jitter
        const baseDelay = 1000 * Math.pow(2, attempt - 1);
        const jitter = Math.floor(Math.random() * 1000);
        const delay = baseDelay + jitter;
        
        log(`Waiting ${Math.round(delay/1000)}s before retry...`, "info");
        await sleep(delay);
      }
    }
  }
  
  throw lastError || new Error(`Failed after ${maxAttempts} attempts: ${description}`);
}

// Checkpoint management
interface Checkpoint {
  lastRun: string;
  dates: string[];
  processedDates: string[];
  lastProcessedDate: string | null;
  status: "running" | "completed" | "failed";
  startTime: string;
  endTime: string | null;
}

function saveCheckpoint(checkpoint: Checkpoint): void {
  try {
    fs.writeFileSync(
      CHECKPOINT_FILE,
      JSON.stringify(checkpoint, null, 2)
    );
  } catch (error) {
    log(`Error saving checkpoint: ${error}`, "error");
  }
}

function loadCheckpoint(): Checkpoint | null {
  try {
    if (fs.existsSync(CHECKPOINT_FILE)) {
      const data = fs.readFileSync(CHECKPOINT_FILE, "utf8");
      return JSON.parse(data) as Checkpoint;
    }
  } catch (error) {
    log(`Error loading checkpoint: ${error}`, "warning");
  }
  return null;
}

// Clean up database connections
async function cleanupConnections(): Promise<void> {
  let client: pg.PoolClient | null = null;
  try {
    // Try to acquire a new connection (outside the existing pool ideally)
    try {
      client = await pool.connect();
    } catch (connectionError) {
      log(`Connection issue during cleanup: ${connectionError}. Will try alternate cleanup.`, "warning");
      
      // If we can't even get a connection, try to reset the pool
      try {
        log(`Attempting to end and recreate pool...`, "info");
        await pool.end().catch(() => null); // Ignore errors on end
        
        // Small pause to ensure connections close
        await sleep(1000);
        
        // Recreate pool with minimal connections
        Object.assign(pool, new pg.Pool({
          connectionString: process.env.DATABASE_URL,
          max: 2,
          idleTimeoutMillis: 5000,
          connectionTimeoutMillis: 5000
        }));
        
        // Try to get a connection from the new pool
        client = await pool.connect();
        log(`Pool reset successful`, "success");
      } catch (resetError) {
        log(`Pool reset failed: ${resetError}`, "error");
        return; // Cannot proceed without a connection
      }
    }
    
    // If we have a client now, try terminating stale connections
    try {
      log(`Finding and terminating stale connections...`, "info");
      
      // First, terminate any connections stuck in a transaction for over 1 minute
      const stuckResult = await client.query(`
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid <> pg_backend_pid()
          AND state = 'active'
          AND xact_start < now() - interval '1 minute'
      `);
      
      // Next, terminate any idle-in-transaction connections
      const idleResult = await client.query(`
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid <> pg_backend_pid()
          AND state = 'idle in transaction'
      `);
      
      // Finally, terminate any specifically from our reconciliation processes
      const reconcileResult = await client.query(`
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid <> pg_backend_pid()
          AND (
            application_name LIKE '%reconcil%'
            OR query LIKE '%curtailment_records%'
            OR query LIKE '%historical_bitcoin_calculations%'
          )
          AND state = 'active'
          AND query_start < now() - interval '30 seconds'
      `);
      
      const stuckCount = stuckResult?.rowCount || 0;
      const idleCount = idleResult?.rowCount || 0;
      const reconcileCount = reconcileResult?.rowCount || 0;
      log(`Connection cleanup completed: ${stuckCount + idleCount + reconcileCount} connections terminated`, "info");
      
      // Report active connections for debugging
      try {
        const activeResult = await client.query(`
          SELECT count(*) as count
          FROM pg_stat_activity
          WHERE datname = current_database()
        `);
        
        const connectionCount = activeResult?.rows?.[0]?.count || 'unknown';
        log(`Current active database connections: ${connectionCount}`, "info");
      } catch (countError) {
        log(`Error counting active connections: ${countError}`, "warning");
      }
    } catch (queryError) {
      log(`Error during connection cleanup queries: ${queryError}`, "warning");
    }
  } catch (overallError) {
    log(`Overall error in connection cleanup: ${overallError}`, "error");
  } finally {
    if (client) {
      try {
        client.release();
      } catch (releaseError) {
        log(`Error releasing cleanup client: ${releaseError}`, "warning");
      }
    }
  }
}

// Safe database pool close
async function safePoolEnd(): Promise<void> {
  try {
    await pool.end();
  } catch (error) {
    log(`Error closing database pool: ${error}`, "warning");
  }
}

// Check reconciliation status for a specific date
interface ReconciliationStatus {
  date: string;
  expected: number;
  actual: number;
  missing: number;
  completionPercentage: number;
}

async function checkDateReconciliationStatus(date: string): Promise<ReconciliationStatus> {
  return withRetry(async () => {
    let client: pg.PoolClient | null = null;
    
    try {
      client = await pool.connect();
      
      // Use a more resilient query structure with NULL handling
      const query = `
        WITH required_combinations AS (
          SELECT 
            count(DISTINCT (settlement_period || '-' || bmu_id)) * 3 AS expected_count
          FROM 
            curtailment_records
          WHERE 
            settlement_date = $1
        ),
        actual_calculations AS (
          SELECT 
            COUNT(*) AS actual_count
          FROM 
            historical_bitcoin_calculations
          WHERE 
            settlement_date = $1
        )
        SELECT 
          $1::text AS date,
          COALESCE((SELECT expected_count FROM required_combinations), 0) AS expected_count,
          COALESCE((SELECT actual_count FROM actual_calculations), 0) AS actual_count,
          COALESCE((SELECT expected_count FROM required_combinations), 0) -
            COALESCE((SELECT actual_count FROM actual_calculations), 0) AS missing_count,
          CASE 
            WHEN COALESCE((SELECT expected_count FROM required_combinations), 0) = 0 THEN 100.0
            ELSE ROUND(
              (COALESCE((SELECT actual_count FROM actual_calculations), 0)::numeric / 
               NULLIF(COALESCE((SELECT expected_count FROM required_combinations), 0), 0)) * 100.0, 2)
          END AS completion_percentage
      `;
      
      // Execute with explicit parameters
      const result = await client.query(query, [date]);
      
      // Safely parse with fallbacks to prevent NaN
      const expected = parseInt(result.rows[0]?.expected_count || '0') || 0;
      const actual = parseInt(result.rows[0]?.actual_count || '0') || 0;
      const missing = parseInt(result.rows[0]?.missing_count || '0') || 0;
      
      // Handle percentage specially to prevent NaN
      let completionPercentage: number;
      if (result.rows[0] && result.rows[0].completion_percentage != null) {
        completionPercentage = parseFloat(result.rows[0].completion_percentage) || 0;
      } else {
        completionPercentage = expected === 0 ? 100 : Math.round((actual / expected) * 100 * 100) / 100;
      }
      
      return {
        date,
        expected,
        actual,
        missing,
        completionPercentage
      };
    } catch (error) {
      // On serious failure, try a simpler query approach
      if (client) {
        try {
          log(`Attempting fallback query for ${date}...`, "warning");
          
          const fallbackQuery = `
            SELECT 
              COUNT(DISTINCT (settlement_period || '-' || bmu_id)) * 3 AS expected_count 
            FROM 
              curtailment_records 
            WHERE 
              settlement_date = $1
          `;
          
          const expectedResult = await client.query(fallbackQuery, [date]);
          const expected = parseInt(expectedResult.rows[0]?.expected_count || '0') || 0;
          
          const actualQuery = `
            SELECT 
              COUNT(*) AS actual_count 
            FROM 
              historical_bitcoin_calculations 
            WHERE 
              settlement_date = $1
          `;
          
          const actualResult = await client.query(actualQuery, [date]);
          const actual = parseInt(actualResult.rows[0]?.actual_count || '0') || 0;
          
          const missing = expected - actual;
          const completionPercentage = expected === 0 ? 100 : Math.round((actual / expected) * 100 * 100) / 100;
          
          return {
            date,
            expected,
            actual,
            missing,
            completionPercentage
          };
        } catch (fallbackError) {
          // If even the fallback fails, just log and throw the original error
          log(`Fallback query failed for ${date}: ${fallbackError}`, "error");
          throw error;
        }
      } else {
        throw error;
      }
    } finally {
      if (client) {
        try {
          client.release();
        } catch (releaseError) {
          log(`Error releasing client for ${date} status check: ${releaseError}`, "warning");
        }
      }
    }
  }, `Check reconciliation status for ${date}`, 3, 15000);
}

// Interface for unified reconciliation status
interface UnifiedReconciliationStatus {
  overview: {
    totalRecords: number;
    totalCalculations: number;
    missingCalculations: number;
    completionPercentage: number;
  };
  dateStats: Array<{
    date: string;
    expected: number;
    actual: number;
    missing: number;
    completionPercentage: number;
  }>;
}

// Fix a specific date with comprehensive error handling using unified reconciliation
async function fixDateComprehensive(date: string): Promise<{
  success: boolean;
  message: string;
  processed: number;
  previouslyMissing: number;
  stillMissing: number;
}> {
  try {
    // First get the initial status using the unified system
    const initialStatusResult = await withRetry<UnifiedReconciliationStatus>(
      async () => await getReconciliationStatus(),
      `Get initial reconciliation status`,
      2
    );
    
    // Find the specific date in the status result
    const dateStats = initialStatusResult.dateStats.find((stat) => stat.date === date);
    
    // If no stats found for this date, create default values
    const initialStatus = dateStats ? {
      date,
      expected: dateStats.expected,
      actual: dateStats.actual,
      missing: dateStats.missing,
      completionPercentage: dateStats.completionPercentage
    } : await checkDateReconciliationStatus(date);
    
    log(`Initial status for ${date}: ${initialStatus.actual}/${initialStatus.expected} (${initialStatus.completionPercentage}%)`, "info");
    
    if (initialStatus.completionPercentage === 100) {
      return {
        success: true,
        message: "Already fully reconciled",
        processed: 0,
        previouslyMissing: 0,
        stillMissing: 0
      };
    }
    
    const previouslyMissing = initialStatus.missing;
    
    // Use the centralized reconciliation system's processDate function
    log(`Attempting to fix ${date} using centralized reconciliation system...`, "info");
    const processResult = await withRetry<{success: boolean; message: string}>(
      async () => await processDate(date),
      `Centralized reconciliation for ${date}`,
      3
    );
    
    // If the centralized system fails, fall back to the standard reconciliation
    if (!processResult.success) {
      log(`Centralized reconciliation failed: ${processResult.message}. Falling back to standard reconciliation...`, "warning");
      await withRetry(
        async () => await reconcileDay(date),
        `Standard reconciliation for ${date}`,
        2
      );
    }
    
    // Check status after reconciliation attempt
    let statusAfterReconciliation = await checkDateReconciliationStatus(date);
    log(`Status after reconciliation: ${statusAfterReconciliation.actual}/${statusAfterReconciliation.expected} (${statusAfterReconciliation.completionPercentage}%)`, "info");
    
    // Only try audit and fix if reconciliation didn't fully resolve
    if (statusAfterReconciliation.completionPercentage < 100) {
      log(`Attempting to fix remaining records with targeted reconciliation...`, "info");
      
      const auditResult = await withRetry(
        async () => await auditAndFixBitcoinCalculations(date),
        `Audit and fix for ${date}`,
        2
      );
      
      // Get current status after audit fixes
      const currentStatus = await checkDateReconciliationStatus(date);
      
      const statusMessage = auditResult.fixed ? 
        `Fixed some calculations - ${auditResult.message}` : 
        `No fixes needed - ${auditResult.message}`;
      
      log(`Audit and fix result: ${statusMessage}`, 
        currentStatus.missing === 0 ? "success" : "warning");
    }
    
    // Get final status
    const finalStatus = await checkDateReconciliationStatus(date);
    log(`Final status for ${date}: ${finalStatus.actual}/${finalStatus.expected} (${finalStatus.completionPercentage}%)`, "info");
    
    const processed = finalStatus.actual - initialStatus.actual;
    const stillMissing = finalStatus.missing;
    
    const success = finalStatus.completionPercentage === 100;
    const message = success
      ? `Successfully reconciled all ${processed} missing calculations`
      : `Fixed ${processed} calculations, but still missing ${stillMissing}`;
    
    log(message, success ? "success" : "warning");
    
    return {
      success,
      message,
      processed,
      previouslyMissing,
      stillMissing
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    log(`Error fixing ${date}: ${errorMessage}`, "error");
    
    return {
      success: false,
      message: `Error: ${errorMessage}`,
      processed: 0,
      previouslyMissing: 0,
      stillMissing: 0
    };
  }
}

// Main function
async function runDailyCheck(): Promise<{
  dates: string[];
  missingDates: string[];
  processedDates: string[];
  fixedDates: string[];
  status: "fully_reconciled" | "all_fixed" | "partial_fix" | "failed";
}> {
  log(`=== Starting Enhanced Daily Reconciliation Check ===`, "info");
  log(`Date: ${new Date().toISOString()}`, "info");
  log(`Checking the last ${RECENT_DAYS_TO_CHECK} days for reconciliation issues...`, "info");
  
  // Set up checkpoint
  let checkpoint = loadCheckpoint();
  let isResume = false;
  
  // If there's a valid checkpoint that's still running, try to resume
  if (checkpoint && checkpoint.status === "running" &&
      checkpoint.dates.length > 0 && 
      checkpoint.lastRun === format(new Date(), "yyyy-MM-dd")) {
    log(`Resuming from previous checkpoint. Last processed date: ${checkpoint.lastProcessedDate || "none"}`, "info");
    isResume = true;
  } else {
    // Start fresh
    const today = new Date();
    const dates: string[] = [];
    
    for (let i = 0; i < RECENT_DAYS_TO_CHECK; i++) {
      const date = subDays(today, i);
      dates.push(format(date, "yyyy-MM-dd"));
    }
    
    log(`Dates to check: ${dates.join(", ")}`, "info");
    
    checkpoint = {
      lastRun: format(new Date(), "yyyy-MM-dd"),
      dates,
      processedDates: [],
      lastProcessedDate: null,
      status: "running",
      startTime: new Date().toISOString(),
      endTime: null
    };
    
    saveCheckpoint(checkpoint);
  }
  
  try {
    // Check reconciliation status for each date
    const dateStatuses = await Promise.all(
      checkpoint.dates.map(date => checkDateReconciliationStatus(date))
    );
    
    // Filter for dates with missing calculations or if force process is enabled
    const datesToProcess = FORCE_PROCESS
      ? dateStatuses // Process all if force mode
      : dateStatuses.filter(status => 
          status.completionPercentage < 100 && status.expected > 0 &&
          !checkpoint.processedDates.includes(status.date)
        );
    
    if (datesToProcess.length === 0) {
      log(`\n✅ No dates need processing. All checked dates are fully reconciled.`, "success");
      
      // Update and save checkpoint
      checkpoint.status = "completed";
      checkpoint.endTime = new Date().toISOString();
      saveCheckpoint(checkpoint);
      
      await safePoolEnd();
      return {
        dates: checkpoint.dates,
        missingDates: [],
        processedDates: checkpoint.processedDates,
        fixedDates: [],
        status: "fully_reconciled"
      };
    }
    
    // Log dates with missing calculations
    if (!FORCE_PROCESS) {
      log(`\nFound ${datesToProcess.length} dates with missing calculations:`, "info");
      datesToProcess.forEach(d => {
        log(`- ${d.date}: ${d.actual}/${d.expected} calculations (${d.completionPercentage}%)`, "info");
      });
    } else {
      log(`\nForce processing ${datesToProcess.length} dates:`, "info");
      datesToProcess.forEach(d => {
        log(`- ${d.date}: ${d.actual}/${d.expected} calculations (${d.completionPercentage}%)`, "info");
      });
    }
    
    // Fix each date with missing calculations
    log(`\nAttempting to process calculations...`, "info");
    
    const results = [];
    const newlyProcessedDates = [];
    const fixedDates = [];
    
    for (const { date } of datesToProcess) {
      log(`\nProcessing ${date}...`, "info");
      
      try {
        // Update checkpoint before processing
        checkpoint.lastProcessedDate = date;
        saveCheckpoint(checkpoint);
        
        // Process the date
        const result = await fixDateComprehensive(date);
        results.push({ date, ...result });
        newlyProcessedDates.push(date);
        
        // Update processed dates in checkpoint
        checkpoint.processedDates.push(date);
        saveCheckpoint(checkpoint);
        
        if (result.success) {
          fixedDates.push(date);
          log(`✅ Successfully processed ${date}`, "success");
        } else {
          log(`⚠️ Could not completely fix ${date}: ${result.message}`, "warning");
        }
        
        // Small pause between dates to avoid overwhelming the database
        await sleep(2000);
      } catch (error) {
        log(`Error processing ${date}: ${error}`, "error");
      }
    }
    
    // Verify the final status of each processed date
    const finalStatuses = await Promise.all(
      datesToProcess.map(({ date }) => checkDateReconciliationStatus(date))
    );
    
    const successfullyReconciled = finalStatuses.filter(
      status => status.completionPercentage === 100
    );
    
    // Summary
    log(`\n=== Daily Reconciliation Check Summary ===`, "info");
    log(`Dates Checked: ${checkpoint.dates.join(", ")}`, "info");
    log(`Dates with Issues: ${datesToProcess.length}`, "info");
    log(`Dates Processed: ${newlyProcessedDates.length}`, "info");
    log(`Dates Fixed: ${successfullyReconciled.length}`, "info");
    
    // Update checkpoint status
    checkpoint.status = "completed";
    checkpoint.endTime = new Date().toISOString();
    saveCheckpoint(checkpoint);
    
    // Clean up
    await cleanupConnections();
    await safePoolEnd();
    
    // Determine overall status
    let status: "fully_reconciled" | "all_fixed" | "partial_fix" | "failed";
    
    if (datesToProcess.length === 0) {
      status = "fully_reconciled";
      log(`\n✅ All dates were already fully reconciled.`, "success");
    } else if (successfullyReconciled.length === datesToProcess.length) {
      status = "all_fixed";
      log(`\n✅ All issues successfully fixed!`, "success");
    } else if (successfullyReconciled.length > 0) {
      status = "partial_fix";
      log(`\n⚠️ Some dates could not be fully fixed. Manual intervention may be required.`, "warning");
    } else {
      status = "failed";
      log(`\n❌ Failed to fix any dates.`, "error");
    }
    
    return {
      dates: checkpoint.dates,
      missingDates: datesToProcess.map(d => d.date),
      processedDates: [...checkpoint.processedDates],
      fixedDates: successfullyReconciled.map(d => d.date),
      status
    };
  } catch (error) {
    // Update checkpoint to indicate failure
    checkpoint.status = "failed";
    checkpoint.endTime = new Date().toISOString();
    saveCheckpoint(checkpoint);
    
    log(`Fatal error during daily reconciliation check: ${error}`, "error");
    
    // Clean up
    await cleanupConnections();
    await safePoolEnd();
    
    throw error;
  }
}

// Set up global error handlers
process.on("uncaughtException", async (error) => {
  log(`Uncaught exception: ${error.stack || error.message}`, "error");
  await safePoolEnd();
  process.exit(1);
});

process.on("unhandledRejection", async (reason) => {
  log(`Unhandled rejection: ${reason}`, "error");
  await safePoolEnd();
  process.exit(1);
});

// Run the reconciliation check if this script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  runDailyCheck()
    .then(() => {
      log("\n=== Daily Reconciliation Check Complete ===", "success");
      process.exit(0);
    })
    .catch(error => {
      log(`Fatal error during daily reconciliation check: ${error}`, "error");
      process.exit(1);
    });
}

export { runDailyCheck, checkDateReconciliationStatus, ReconciliationStatus };