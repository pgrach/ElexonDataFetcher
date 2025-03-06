/**
 * Simple Bitcoin Calculator for 2025-03-04
 * 
 * This script uses a simplified approach to calculate Bitcoin for all periods
 * by directly calling the service function that handles the calculation process.
 */

import { processSingleDay } from "./server/services/bitcoinService";

// Configuration
const DATE = "2025-03-04"; // Target date for reingestion
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// ANSI color codes for console output
const colors = {
  reset: "\x1b[0m",
  bright: "\x1b[1m",
  dim: "\x1b[2m",
  red: "\x1b[31m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  blue: "\x1b[36m",
  magenta: "\x1b[35m"
};

function log(message: string, type: "info" | "success" | "warning" | "error" | "title" = "info"): void {
  const timestamp = new Date().toISOString().split('T')[1].replace('Z', '');
  
  switch (type) {
    case "title":
      console.log(`${colors.bright}${colors.magenta}${message}${colors.reset}`);
      break;
    case "info":
      console.log(`[${timestamp}] ${colors.blue}${message}${colors.reset}`);
      break;
    case "success":
      console.log(`[${timestamp}] ${colors.green}${message}${colors.reset}`);
      break;
    case "warning":
      console.log(`[${timestamp}] ${colors.yellow}${message}${colors.reset}`);
      break;
    case "error":
      console.log(`[${timestamp}] ${colors.red}${message}${colors.reset}`);
      break;
  }
}

async function processAllModels(): Promise<void> {
  log(`Processing Bitcoin calculations for ${DATE}`, "title");
  
  try {
    // Process Bitcoin calculations for each miner model
    for (const minerModel of MINER_MODELS) {
      log(`Processing calculations for ${minerModel}...`, "info");
      
      try {
        await processSingleDay(DATE, minerModel);
        log(`Successfully processed Bitcoin calculations for ${minerModel}`, "success");
      } catch (error) {
        log(`Error processing ${minerModel}: ${error}`, "error");
      }
      
      // Small pause between models
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    log(`Bitcoin calculations completed for ${DATE}`, "success");
  } catch (error) {
    log(`Error: ${error}`, "error");
    process.exit(1);
  }
}

// Run the main function
processAllModels();