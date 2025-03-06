/**
 * Simple script to process a specific date
 * This script uses the direct functions from the services rather than the CLI tool
 */

import { processDate } from "../server/services/historicalReconciliation";

// Change this to the date you want to process
const DATE_TO_PROCESS = "2025-03-04";

async function process() {
  console.log(`Processing date: ${DATE_TO_PROCESS}`);
  
  try {
    const result = await processDate(DATE_TO_PROCESS);
    
    if (result.success) {
      console.log(`Success: ${result.message}`);
    } else {
      console.error(`Failed: ${result.message}`);
    }
  } catch (error) {
    console.error(`Error: ${error}`);
  }
}

process();