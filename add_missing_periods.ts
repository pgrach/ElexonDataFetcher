/**
 * Add Missing Periods Script
 * 
 * This script focuses specifically on adding the missing periods
 * for March 4, 2025 for periods 16, 39-48.
 */

import { fetchBidsOffers } from "./server/services/elexon";
import { processDailyCurtailment } from "./server/services/curtailment";

const TARGET_DATE = '2025-03-04';
const MISSING_PERIODS = [16, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48];

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function processPeriod(date: string, period: number): Promise<void> {
  try {
    console.log(`\nProcessing period ${period} for ${date}...`);
    const records = await fetchBidsOffers(date, period);
    console.log(`Retrieved ${records.length} records for period ${period}`);
    
    // Add a small delay to prevent overwhelming the API
    await delay(1000);
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
  }
}

async function main() {
  try {
    console.log(`\n=== Adding Missing Periods for ${TARGET_DATE} ===\n`);
    
    // Process each missing period one at a time
    for (const period of MISSING_PERIODS) {
      await processPeriod(TARGET_DATE, period);
    }
    
    // After fetching all the missing periods, reprocess the entire day
    console.log("\nReprocessing the entire day to ensure all data is properly recorded...");
    await processDailyCurtailment(TARGET_DATE);
    
    console.log("\n=== Processing Complete ===\n");
    console.log("Please check the database to verify all periods are now present.");
  } catch (error) {
    console.error('Error during processing:', error);
  }
}

// Run the script
main();