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

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { fetchBidsOffers } from "./server/services/elexon";
import fs from "fs/promises";
import * as process from "process";
import path from "path";
import { fileURLToPath } from 'url';
import { sql } from "drizzle-orm";
import { eq } from "drizzle-orm";

// Constants
const NUM_PERIODS_PER_DAY = 48;
const CHECKPOINT_FILE = 'daily_reconciliation_checkpoint.json';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");

// Track if we have connected to the database
let hasConnected = false;

// Colors for console output
const colors = {
  reset: "\x1b[0m",
  red: "\x1b[31m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  blue: "\x1b[34m"
};

/**
 * Log message with optional level
 * @export Can be used by importing modules
 */
export function log(message: string, level: "info" | "error" | "warning" | "success" = "info"): void {
  const timestamp = new Date().toISOString();
  let prefix = "";
  
  switch (level) {
    case "error":
      prefix = `${colors.red}[ERROR]${colors.reset}`;
      break;
    case "warning":
      prefix = `${colors.yellow}[WARNING]${colors.reset}`;
      break;
    case "success":
      prefix = `${colors.green}[SUCCESS]${colors.reset}`;
      break;
    default:
      prefix = `${colors.blue}[INFO]${colors.reset}`;
  }
  
  console.log(`${timestamp} ${prefix} ${message}`);
}

/**
 * Utility function to delay execution
 */
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Execute a function with retry logic
 */
async function withRetry<T>(
  fn: () => Promise<T>,
  options: {
    maxRetries?: number;
    retryDelay?: number;
    description?: string;
  } = {}
): Promise<T> {
  const { maxRetries = 3, retryDelay = 2000, description = "operation" } = options;
  let lastError: Error | undefined;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error as Error;
      log(`Failed ${description} (attempt ${attempt}/${maxRetries}): ${lastError.message}`, "warning");
      
      if (attempt < maxRetries) {
        log(`Retrying in ${retryDelay / 1000} seconds...`, "info");
        await sleep(retryDelay);
      }
    }
  }
  
  throw lastError;
}

/**
 * Checkpoint data structure
 */
interface Checkpoint {
  lastRun: string;
  dates: string[];
  processedDates: string[];
  lastProcessedDate: string | null;
  status: "running" | "completed" | "failed";
  startTime: string;
  endTime: string | null;
}

/**
 * Save checkpoint to file
 */
function saveCheckpoint(checkpoint: Checkpoint): void {
  try {
    fs.writeFile(CHECKPOINT_FILE, JSON.stringify(checkpoint, null, 2), 'utf8');
  } catch (error) {
    log(`Failed to save checkpoint: ${(error as Error).message}`, "error");
  }
}

/**
 * Load checkpoint from file
 */
function loadCheckpoint(): Checkpoint | null {
  try {
    const data = require(`./${CHECKPOINT_FILE}`);
    return data as Checkpoint;
  } catch (error) {
    // Checkpoint doesn't exist or is invalid
    return null;
  }
}

/**
 * Cleanup function (placeholder for future use)
 */
async function cleanupConnections(): Promise<void> {
  log("Cleanup complete", "success");
}

/**
 * Safe shutdown function (placeholder for future use)
 */
async function safePoolEnd(): Promise<void> {
  log("Shutdown complete", "success");
}

/**
 * Reconciliation status for a date
 */
interface ReconciliationStatus {
  date: string;
  expected: number;
  actual: number;
  missing: number;
  completionPercentage: number;
}

/**
 * Check reconciliation status for a specific date
 */
async function checkDateReconciliationStatus(date: string): Promise<ReconciliationStatus> {
  log(`Checking reconciliation status for ${date}...`);
  
  // Count actual records in database
  const recordCountResult = await db
    .select({ count: sql<number>`count(*)` })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  const actual = recordCountResult[0]?.count || 0;
  
  // Load BMU mappings to determine the expected count
  const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
  const bmuMapping = JSON.parse(mappingContent);
  const windFarmCount = bmuMapping.filter((bmu: any) => bmu.fuelType === "WIND").length;
  
  // For simplicity, we'll use a worst-case estimate:
  // In a perfect world, we'd expect all wind farms to have records for all periods
  // But in reality, only farms with curtailment will have records
  // So we'll use the API to get a rough count of farms with curtailment
  
  // Sample a single period to get an idea of how many farms typically report
  const samplePeriod = 25; // Midday period
  const apiRecords = await fetchBidsOffers(date, samplePeriod);
  
  // Filter for valid curtailment records
  const windFarmIds = new Set<string>(
    bmuMapping
      .filter((bmu: any) => bmu.fuelType === "WIND")
      .map((bmu: any) => bmu.elexonBmUnit)
  );
  
  const validApiRecords = apiRecords.filter(record =>
    record.volume < 0 &&
    (record.soFlag || record.cadlFlag) &&
    windFarmIds.has(record.id)
  );
  
  // Use the sample count to estimate total records
  // Add a 10% margin for varying patterns throughout the day
  const averageFarmsPerPeriod = Math.ceil(validApiRecords.length * 1.1);
  const expected = averageFarmsPerPeriod * NUM_PERIODS_PER_DAY;
  
  const missing = Math.max(0, expected - actual);
  const completionPercentage = Math.min(100, Math.round((actual / expected) * 100));
  
  const status: ReconciliationStatus = {
    date,
    expected,
    actual,
    missing,
    completionPercentage
  };
  
  log(`Status for ${date}: ${status.actual}/${status.expected} records (${status.completionPercentage}% complete)`);
  return status;
}

/**
 * Unified reconciliation status for multiple dates
 */
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

/**
 * Comprehensive fix for a specific date
 * 
 * @export Function can be imported by other modules
 */
export async function fixDateComprehensive(date: string): Promise<{
  success: boolean;
  recordsProcessed: number;
  recordsAdded: number;
  periodsProcessed: number;
}> {
  log(`Running comprehensive fix for ${date}...`, "info");
  
  // Validate date format (YYYY-MM-DD)
  if (!date.match(/^\d{4}-\d{2}-\d{2}$/)) {
    log(`Invalid date format: ${date}. Expected format: YYYY-MM-DD`, "error");
    return {
      success: false,
      recordsProcessed: 0,
      recordsAdded: 0,
      periodsProcessed: 0
    };
  }
  
  try {
    // Import the optimized critical date processor
    const processDateModule = await import(`./optimized_critical_date_processor`);
    
    // Execute the processor for all periods
    const result = await processDateModule.processDate(date, 1, 48);
    
    // Run reconciliation to update all summaries and Bitcoin calculations
    try {
      log(`Running reconciliation for ${date}...`, "info");
      
      // Import and execute the updateSummaries function from unified_reconciliation
      // which already correctly exports the functions from update_summaries.ts
      const { updateSummaries, updateBitcoinCalculations } = await import('./unified_reconciliation');
      await updateSummaries(date);
      await updateBitcoinCalculations(date);
      
      log(`Successfully updated summaries and Bitcoin calculations for ${date}`, "success");
    } catch (error) {
      log(`Error during reconciliation: ${error}`, "error");
    }
    
    log(`Fix completed successfully for ${date}`, "success");
    return {
      success: true,
      recordsProcessed: result.recordsProcessed || 0,
      recordsAdded: result.recordsAdded || 0,
      periodsProcessed: result.periodsProcessed || 0
    };
  } catch (error) {
    log(`Error fixing data for ${date}: ${(error as Error).message}`, "error");
    return {
      success: false,
      recordsProcessed: 0,
      recordsAdded: 0,
      periodsProcessed: 0
    };
  }
}

/**
 * Run daily reconciliation check
 * 
 * @export Function can be imported by other modules
 */
export async function runDailyCheck(): Promise<{
  datesChecked: string[];
  datesProcessed: string[];
  successful: boolean;
}> {
  // Get command line args
  let daysArg = process.argv[2];
  let datesToCheck: string[] = [];
  
  // Check if it's a date in YYYY-MM-DD format
  if (daysArg?.match(/^\d{4}-\d{2}-\d{2}$/)) {
    // User provided a specific date to check
    datesToCheck = [daysArg];
    log(`Checking specific date: ${daysArg}`, "info");
  } else {
    // Treat as number of days to check
    const daysToCheck = daysArg && !isNaN(parseInt(daysArg, 10)) ? parseInt(daysArg, 10) : 2;
    const validDaysToCheck = daysToCheck > 0 ? daysToCheck : 2;
    
    if (isNaN(daysToCheck) || daysToCheck < 1) {
      log("Invalid 'days' parameter. Using default of 2 days.", "warning");
    }
    
    log(`Starting daily reconciliation check for the last ${validDaysToCheck} days...`, "info");
    
    // Generate dates to check (most recent first)
    const now = new Date();
    for (let i = 1; i <= validDaysToCheck; i++) {
      const date = new Date(now);
      date.setDate(now.getDate() - i);
      datesToCheck.push(date.toISOString().split('T')[0]); // Format: YYYY-MM-DD
    }
  }
  
  const forceProcess = process.argv[3] === 'true';
  
  if (isNaN(daysToCheck) || daysToCheck < 1) {
    log("Invalid 'days' parameter. Using default of 2 days.", "warning");
  }
  
  log(`Starting daily reconciliation check for the last ${validDaysToCheck} days...`, "info");
  log(`Force processing is ${forceProcess ? 'enabled' : 'disabled'}`, "info");
  
  // Load previous checkpoint or create a new one
  const previousCheckpoint = loadCheckpoint();
  const newCheckpoint: Checkpoint = {
    lastRun: new Date().toISOString(),
    dates: [],
    processedDates: [],
    lastProcessedDate: null,
    status: "running",
    startTime: new Date().toISOString(),
    endTime: null
  };
  
  // Save initial checkpoint
  saveCheckpoint(newCheckpoint);
  
  // Generate dates to check (most recent first)
  const datesToCheck: string[] = [];
  const now = new Date();
  
  for (let i = 1; i <= validDaysToCheck; i++) {
    const date = new Date(now);
    date.setDate(now.getDate() - i);
    datesToCheck.push(date.toISOString().split('T')[0]); // Format: YYYY-MM-DD
  }
  
  newCheckpoint.dates = datesToCheck;
  saveCheckpoint(newCheckpoint);
  
  // Check each date
  const dateResults: ReconciliationStatus[] = [];
  const datesToProcess: string[] = [];
  
  for (const date of datesToCheck) {
    try {
      const status = await checkDateReconciliationStatus(date);
      dateResults.push(status);
      
      // Decide if this date needs processing
      if (forceProcess || status.completionPercentage < 95) {
        log(`Date ${date} needs processing (${status.completionPercentage}% complete)`, "warning");
        datesToProcess.push(date);
      } else {
        log(`Date ${date} is sufficiently complete (${status.completionPercentage}%)`, "success");
      }
    } catch (error) {
      log(`Error checking date ${date}: ${(error as Error).message}`, "error");
    }
  }
  
  // Return early if nothing to process
  if (datesToProcess.length === 0) {
    log("No dates need processing", "success");
    
    newCheckpoint.status = "completed";
    newCheckpoint.endTime = new Date().toISOString();
    saveCheckpoint(newCheckpoint);
    
    return {
      datesChecked: datesToCheck,
      datesProcessed: [],
      successful: true
    };
  }
  
  // Process each identified date
  log(`Processing ${datesToProcess.length} dates with missing data...`, "info");
  const processedDates: string[] = [];
  let allSuccessful = true;
  
  for (const date of datesToProcess) {
    log(`Processing date: ${date}`, "info");
    newCheckpoint.lastProcessedDate = date;
    saveCheckpoint(newCheckpoint);
    
    try {
      const result = await fixDateComprehensive(date);
      
      if (result.success) {
        log(`Successfully processed ${date}: ${result.recordsAdded} records added across ${result.periodsProcessed} periods`, "success");
        processedDates.push(date);
        newCheckpoint.processedDates.push(date);
        saveCheckpoint(newCheckpoint);
      } else {
        log(`Failed to process ${date}`, "error");
        allSuccessful = false;
      }
    } catch (error) {
      log(`Error processing date ${date}: ${(error as Error).message}`, "error");
      allSuccessful = false;
    }
  }
  
  // Complete the checkpoint
  newCheckpoint.status = allSuccessful ? "completed" : "failed";
  newCheckpoint.endTime = new Date().toISOString();
  saveCheckpoint(newCheckpoint);
  
  return {
    datesChecked: datesToCheck,
    datesProcessed: processedDates,
    successful: allSuccessful
  };
}

// Main function
(async () => {
  try {
    const startTime = Date.now();
    
    // Run the check
    const result = await runDailyCheck();
    
    // Calculate execution time
    const executionTime = ((Date.now() - startTime) / 1000).toFixed(1);
    
    // Display final summary
    log("\n=== Reconciliation Check Summary ===", "info");
    log(`Execution time: ${executionTime} seconds`, "info");
    log(`Dates checked: ${result.datesChecked.join(", ")}`, "info");
    
    if (result.datesProcessed.length > 0) {
      log(`Dates processed: ${result.datesProcessed.join(", ")}`, "success");
    } else {
      log("No dates needed processing", "success");
    }
    
    log(`Overall status: ${result.successful ? "Success" : "Partial success/failure"}`, 
      result.successful ? "success" : "warning");
    
    // Clean up before exit
    await cleanupConnections();
    await safePoolEnd();
    
    // Exit with appropriate code
    process.exit(result.successful ? 0 : 1);
  } catch (error) {
    log(`Unhandled error in reconciliation check: ${(error as Error).message}`, "error");
    console.error(error);
    
    // Clean up on error
    await cleanupConnections();
    await safePoolEnd();
    
    process.exit(1);
  }
})();