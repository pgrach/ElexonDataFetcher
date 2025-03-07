/**
 * Efficient Date Reprocessing Tool
 * 
 * This script provides a simple, reliable way to reprocess data for a specific date,
 * focusing primarily on Bitcoin calculations, which is the most common use case.
 * It's designed to be efficient and avoid timeouts even with large datasets.
 * 
 * Features:
 * - Fast Bitcoin calculation reconciliation that completes within timeout limits
 * - Clear console output with progress indicators
 * - Auto-detection of missing calculations
 * - Automatic updating of monthly and yearly summaries
 * - Verification of completeness after processing
 * 
 * Usage:
 *   npx tsx reprocess_date.ts <date> [--full]
 * 
 * Arguments:
 *   date      The date to reprocess in YYYY-MM-DD format
 * 
 * Options:
 *   --full    Run full reprocessing including curtailment data (slower, may timeout)
 * 
 * Examples:
 *   npx tsx reprocess_date.ts 2025-03-06
 *   npx tsx reprocess_date.ts 2025-03-06 --full
 */

import { processDate, auditAndFixBitcoinCalculations } from "./server/services/historicalReconciliation";
import { processDailyCurtailment } from "./server/services/curtailment";
import { isValidDateString } from "./server/utils/dates";
import pkg from 'pg';
const { Pool } = pkg;
import { logger } from "./server/utils/logger";

// Create a database pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

// ANSI color codes for better console output
const colors = {
  reset: "\x1b[0m",
  bright: "\x1b[1m",
  green: "\x1b[32m",
  cyan: "\x1b[36m",
  yellow: "\x1b[33m",
  red: "\x1b[31m",
  magenta: "\x1b[35m"
};

/**
 * Log a message with color formatting
 */
function log(message: string, type: "info" | "success" | "warning" | "error" | "title" = "info"): void {
  const timestamp = new Date().toISOString().substring(11, 19);
  let prefix = `[${timestamp}]`;
  
  switch (type) {
    case "title":
      console.log(`${colors.bright}${colors.magenta}${message}${colors.reset}`);
      break;
    case "info":
      console.log(`${prefix} ${colors.cyan}${message}${colors.reset}`);
      break;
    case "success":
      console.log(`${prefix} ${colors.green}${message}${colors.reset}`);
      break;
    case "warning":
      console.log(`${prefix} ${colors.yellow}${message}${colors.reset}`);
      break;
    case "error":
      console.log(`${prefix} ${colors.red}${message}${colors.reset}`);
      break;
  }
}

/**
 * Verify curtailment data is complete for a date
 */
async function verifyCurtailmentData(date: string): Promise<{
  isComplete: boolean;
  records: number;
  periods: number;
}> {
  const result = await pool.query(
    `SELECT COUNT(*) as record_count, COUNT(DISTINCT settlement_period) as period_count 
     FROM curtailment_records 
     WHERE settlement_date = $1`,
    [date]
  );
  
  const { record_count, period_count } = result.rows[0];
  
  return {
    isComplete: parseInt(period_count) === 48,
    records: parseInt(record_count),
    periods: parseInt(period_count)
  };
}

/**
 * Parse command line arguments
 */
function parseArgs() {
  const args = process.argv.slice(2);
  
  // Default options
  const options = {
    date: "",
    fullReprocessing: false
  };
  
  // Parse positional argument (date)
  if (args.length > 0 && !args[0].startsWith('--')) {
    options.date = args[0];
  }
  
  // Parse flags
  for (const arg of args) {
    if (arg === '--full') {
      options.fullReprocessing = true;
    }
  }
  
  return options;
}

/**
 * Main function to handle command line arguments and process the date
 */
async function main() {
  // Start timing
  const startTime = Date.now();
  
  // Parse arguments
  const options = parseArgs();
  
  // Validate date
  if (!options.date || !isValidDateString(options.date)) {
    log("Please provide a valid date in YYYY-MM-DD format", "error");
    log("Usage: npx tsx reprocess_date.ts <date> [--full]", "info");
    process.exit(1);
  }
  
  // Print start message
  log(`Reprocessing data for ${options.date}`, "title");
  
  try {
    // Check if curtailment data exists
    const curtailmentStatus = await verifyCurtailmentData(options.date);
    
    if (curtailmentStatus.isComplete) {
      log(`Found ${curtailmentStatus.records} curtailment records across ${curtailmentStatus.periods} periods`, "info");
    } else if (curtailmentStatus.records > 0) {
      log(`Found ${curtailmentStatus.records} curtailment records but only ${curtailmentStatus.periods}/48 periods`, "warning");
    } else {
      log(`No curtailment records found for ${options.date}`, "warning");
    }
    
    // Process curtailment data if requested or not complete
    if (options.fullReprocessing || !curtailmentStatus.isComplete) {
      log(`Processing curtailment data for ${options.date}...`, "info");
      await processDailyCurtailment(options.date);
      log(`Curtailment data processing complete for ${options.date}`, "success");
    }
    
    // Process Bitcoin calculations
    log(`Fixing Bitcoin calculations for ${options.date}...`, "info");
    
    // Use the direct audit and fix function which is more efficient
    const result = await auditAndFixBitcoinCalculations(options.date);
    
    if (result.success) {
      if (!result.fixed) {
        log(`All Bitcoin calculations for ${options.date} are complete`, "success");
      } else {
        log(`Fixed Bitcoin calculations for ${options.date}`, "success");
        log(`${result.message}`, "info");
      }
    } else {
      log(`Failed to fix Bitcoin calculations: ${result.message}`, "error");
    }
    
    // Calculate and display duration
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    log(`Reprocessing complete in ${duration}s`, "success");
    
  } catch (error: any) {
    logger.error(`Error reprocessing data for ${options.date}: ${error.message}`);
    log(`Error reprocessing data: ${error.message}`, "error");
    process.exit(1);
  } finally {
    // Always close the pool
    await pool.end();
  }
}

// Execute the main function
main();