/**
 * Simple Data Reprocessing Tool
 * 
 * A streamlined script for reprocessing data for a specific date.
 * Unlike the other more complex reingestion scripts, this focuses on simplicity
 * and reliability for the common use case of reprocessing a single date.
 * 
 * Usage:
 *   npx tsx reprocess_date_simple.ts <date>
 * 
 * Example:
 *   npx tsx reprocess_date_simple.ts 2025-03-06
 */

import { processDate } from "./server/services/historicalReconciliation";
import { isValidDateString } from "./server/utils/dates";

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
 * Main function to process a specific date
 */
async function main() {
  const startTime = Date.now();
  
  // Get date from command line
  const date = process.argv[2];
  
  // Validate date
  if (!date || !isValidDateString(date)) {
    log("Please provide a valid date in YYYY-MM-DD format", "error");
    log("Usage: npx tsx reprocess_date_simple.ts YYYY-MM-DD", "info");
    process.exit(1);
  }
  
  // Print start message
  log(`Reprocessing Bitcoin calculations for ${date}`, "title");
  
  try {
    // Process the date using the historicalReconciliation service
    log(`Processing date ${date}...`, "info");
    const result = await processDate(date);
    
    if (result.success) {
      log(`Success: ${result.message}`, "success");
    } else {
      log(`Failed: ${result.message}`, "error");
      process.exit(1);
    }
    
    // Calculate duration
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    log(`Completed in ${duration}s`, "success");
    
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    log(`Error processing date: ${errorMessage}`, "error");
    process.exit(1);
  }
}

// Run the main function
main();