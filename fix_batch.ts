/**
 * Fix Missing Periods in Batches
 * 
 * This script processes a small batch of missing periods at a time
 * to avoid timeouts.
 */

import { fetchBidsOffers } from "./server/services/elexon";
import { sql } from "drizzle-orm";
import { db } from "./db";

const TARGET_DATE = '2025-03-04';
// Specify a small batch of periods to process (modify these numbers as needed)
const BATCH_TO_PROCESS = [37, 38, 39]; 

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Fetch a single period from Elexon
async function fetchPeriod(period: number): Promise<void> {
  try {
    console.log(`\nFetching data for period ${period}...`);
    const records = await fetchBidsOffers(TARGET_DATE, period);
    console.log(`Period ${period}: Retrieved ${records.length} records`);
    await delay(1000); // Brief delay between requests
  } catch (error) {
    console.error(`Error fetching period ${period}:`, error);
  }
}

// Main function
async function main() {
  try {
    console.log(`\n=== Processing Batch for ${TARGET_DATE} ===\n`);
    console.log(`Batch: ${BATCH_TO_PROCESS.join(', ')}`);
    
    // Fetch each period in the batch
    for (const period of BATCH_TO_PROCESS) {
      await fetchPeriod(period);
    }
    
    console.log("\n=== Batch Processing Complete ===\n");
    console.log("Next step: Run processDailyCurtailment('2025-03-04') to process the fetched data");
  } catch (error) {
    console.error('Error during processing:', error);
  }
}

// Run the script
main();